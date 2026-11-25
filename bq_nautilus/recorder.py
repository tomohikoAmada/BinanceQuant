"""Verified Python 3.13 Recorder raw/mcp-export reader and Nautilus data converters."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import struct
from typing import Any, Iterable, Iterator
from uuid import UUID

import cbor2
import google_crc32c
import zstandard

from bq_feed.events import (
    AggTradeEvent,
    BookTickerEvent,
    FeedEvent,
    Market,
    ReferenceObservationEvent,
    Stream,
    StreamHealthEvent,
)
from bq_feed.replay import (
    _MARKET_ALIASES,
    _MARKET_TO_RECORDER_CANDIDATES,
    _normalize_manifest_stream,
    _parse_market,
    parse_raw_record,
)

RAW_MAGIC = b"BMRCHNK\x1a"
FORMAT_MAJOR = 1
FORMAT_MINOR = 0
BYTE_ORDER_MARKER = 0xFEFF
MAX_HEADER_BODY_BYTES = 64 * 1024
FIXED_HEADER = struct.Struct(">8sBBHIII")
FRAME_PREFIX_WITHOUT_CRC = struct.Struct(">IHH")
FRAME_PREFIX = struct.Struct(">IHHI")


class RecorderArtifactError(RuntimeError):
    """Raised when a sealed Raw chunk or manifest fails verification."""


@dataclass(frozen=True, slots=True)
class VerifiedChunkRef:
    chunk_id: str
    artifact_path: Path
    market: str
    symbol: str
    stream: str
    stored_bytes: int
    stored_sha256: str
    uncompressed_sha256: str
    created_at_utc_ns: int
    receive_min_ns: int | None
    receive_max_ns: int | None
    complete: bool
    gap: bool


@dataclass(frozen=True, slots=True)
class RawEnvelopeRecord:
    chunk_id: str
    ordinal: int
    market: str
    symbol: str
    stream: str
    receive_time_utc_ns: int
    receive_monotonic_ns: int
    capture_flags: tuple[str, ...]
    raw_payload: bytes


@dataclass(frozen=True, slots=True)
class NautilusConversionSummary:
    data: list[Any]
    quote_provenance: list[dict[str, Any]]
    proxy_ts_event_count: int
    health_events_count: int


def _read_up_to(source: Any, length: int) -> bytes:
    parts: list[bytes] = []
    remaining = length
    while remaining > 0:
        block = source.read(remaining)
        if not block:
            break
        parts.append(block)
        remaining -= len(block)
    return b"".join(parts)


def _decode_one_cbor(body: bytes) -> dict[str, Any]:
    src = io.BytesIO(body)
    val = cbor2.CBORDecoder(src).decode()
    if src.read(1):
        raise RecorderArtifactError("CBOR payload contains trailing bytes")
    if not isinstance(val, dict):
        raise RecorderArtifactError("CBOR root is not a map")
    return val


def verify_sealed_zstd_artifact(chunk: VerifiedChunkRef) -> None:
    """Verify exact compressed size, compressed SHA-256, and uncompressed SHA-256."""
    if not chunk.artifact_path.is_file():
        raise RecorderArtifactError(f"Missing sealed artifact: {chunk.artifact_path}")
    actual_size = chunk.artifact_path.stat().st_size
    if actual_size != chunk.stored_bytes:
        raise RecorderArtifactError(
            f"Size mismatch for {chunk.chunk_id}: expected {chunk.stored_bytes}, got {actual_size}"
        )
    comp_hash = hashlib.sha256()
    uncomp_hash = hashlib.sha256()
    dctx = zstandard.ZstdDecompressor()
    decomp = dctx.decompressobj()
    with chunk.artifact_path.open("rb") as fh:
        while block := fh.read(1024 * 1024):
            comp_hash.update(block)
            uncomp_hash.update(decomp.decompress(block))
        uncomp_hash.update(decomp.flush())
    if comp_hash.hexdigest() != chunk.stored_sha256:
        raise RecorderArtifactError(f"stored_sha256 mismatch for {chunk.chunk_id}")
    if uncomp_hash.hexdigest() != chunk.uncompressed_sha256:
        raise RecorderArtifactError(f"uncompressed_sha256 mismatch for {chunk.chunk_id}")


def iter_raw_chunk_records(
    chunk: VerifiedChunkRef,
    *,
    verify_hashes: bool = True,
) -> Iterator[RawEnvelopeRecord]:
    """Stream and verify one Raw chunk v1 zstd artifact frame by frame."""
    if verify_hashes:
        verify_sealed_zstd_artifact(chunk)

    dctx = zstandard.ZstdDecompressor()
    with chunk.artifact_path.open("rb") as compressed, dctx.stream_reader(compressed) as reader:
        fixed = _read_up_to(reader, FIXED_HEADER.size)
        if len(fixed) != FIXED_HEADER.size:
            raise RecorderArtifactError(f"Truncated fixed header in {chunk.chunk_id}")
        magic, major, minor, marker, flags, body_length, expected_crc = FIXED_HEADER.unpack(fixed)
        if magic != RAW_MAGIC or (major, minor) != (FORMAT_MAJOR, FORMAT_MINOR) or marker != BYTE_ORDER_MARKER:
            raise RecorderArtifactError(f"Invalid Raw chunk header in {chunk.chunk_id}")
        if flags != 0 or body_length > MAX_HEADER_BODY_BYTES:
            raise RecorderArtifactError(f"Unsupported Raw header flags/size in {chunk.chunk_id}")
        hdr_body = _read_up_to(reader, body_length)
        if len(hdr_body) != body_length:
            raise RecorderArtifactError(f"Truncated CBOR header in {chunk.chunk_id}")
        if google_crc32c.value(fixed[:-4] + hdr_body) != expected_crc:
            raise RecorderArtifactError(f"Header CRC32C mismatch in {chunk.chunk_id}")
        hdr = _decode_one_cbor(hdr_body)
        hdr_uuid = str(UUID(bytes=hdr["chunk_id"]))
        if hdr_uuid != chunk.chunk_id:
            raise RecorderArtifactError(f"Header chunk_id {hdr_uuid} != {chunk.chunk_id}")
        max_frame_bytes = int(hdr["max_frame_bytes"])

        ordinal = 0
        while True:
            prefix = _read_up_to(reader, FRAME_PREFIX.size)
            if not prefix:
                break
            if len(prefix) != FRAME_PREFIX.size:
                raise RecorderArtifactError(f"Truncated frame prefix in {chunk.chunk_id}")
            frame_len, f_flags, f_res, f_crc = FRAME_PREFIX.unpack(prefix)
            if frame_len > max_frame_bytes or f_flags != 0 or f_res != 0:
                raise RecorderArtifactError(f"Invalid frame prefix in {chunk.chunk_id}")
            body = _read_up_to(reader, frame_len)
            if len(body) != frame_len:
                raise RecorderArtifactError(f"Truncated frame body in {chunk.chunk_id}")
            if google_crc32c.value(prefix[: FRAME_PREFIX_WITHOUT_CRC.size] + body) != f_crc:
                raise RecorderArtifactError(f"Frame CRC32C mismatch in {chunk.chunk_id} at ordinal {ordinal}")
            env = _decode_one_cbor(body)
            raw_payload = env["raw_payload"]
            if not isinstance(raw_payload, bytes):
                raw_payload = bytes(raw_payload)
            yield RawEnvelopeRecord(
                chunk_id=chunk.chunk_id,
                ordinal=ordinal,
                market=str(env["market"]),
                symbol=str(env["symbol"]),
                stream=str(env["stream"]),
                receive_time_utc_ns=int(env["receive_time_utc_ns"]),
                receive_monotonic_ns=int(env["receive_monotonic_ns"]),
                capture_flags=tuple(str(f) for f in env.get("capture_flags", ())),
                raw_payload=raw_payload,
            )
            ordinal += 1


def _resolve_catalog_external_path(
    partition_root: Path,
    chunk_id: str,
    external_roots: dict[str, Path],
) -> Path | None:
    cat_path = partition_root / "catalog" / "catalog.sqlite"
    if not cat_path.is_file():
        return None
    con = sqlite3.connect(f"file:{cat_path}?mode=ro", uri=True)
    try:
        tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "archive_copies" in tables:
            rows = con.execute(
                "SELECT storage_id, target_relative_path FROM archive_copies WHERE chunk_id = ?",
                (chunk_id,),
            ).fetchall()
            for storage_id, rel in rows:
                root = external_roots.get(str(storage_id))
                if root is not None:
                    cand = (root / str(rel)).resolve()
                    if cand.is_file():
                        return cand
    finally:
        con.close()
    return None


def _matches_chunk_filters(
    *,
    c_market: str,
    c_symbol: str,
    c_stream: str,
    c_min: int | None,
    c_max: int | None,
    market_filter: set[Market] | None,
    symbol_filter: set[str] | None,
    norm_stream_filter: set[str] | None,
    min_utc_ns: int | None,
    max_utc_ns: int | None,
) -> bool:
    if market_filter and c_market not in {"__MIXED__", "__GLOBAL__"}:
        try:
            if _parse_market(c_market) not in market_filter:
                return False
        except ValueError:
            return False
    if symbol_filter and c_symbol not in {"__MIXED__", "__GLOBAL__"} and c_symbol not in symbol_filter:
        return False
    if norm_stream_filter and c_stream != "__MIXED__":
        if _normalize_manifest_stream(c_stream) not in norm_stream_filter:
            return False
    if min_utc_ns is not None and c_max is not None and int(c_max) < min_utc_ns:
        return False
    if max_utc_ns is not None and c_min is not None and int(c_min) >= max_utc_ns:
        return False
    return True


def load_partition_chunks(
    partition_root: Path,
    *,
    external_roots: dict[str, Path] | None = None,
    market_filter: set[Market] | None = None,
    symbol_filter: set[str] | None = None,
    stream_filter: set[str | Stream] | None = None,
    min_utc_ns: int | None = None,
    max_utc_ns: int | None = None,
) -> list[VerifiedChunkRef]:
    """Load and prune verified chunk refs from a Recorder partition directory."""
    manifests_dir = partition_root / "manifests"
    if not manifests_dir.is_dir():
        raise RecorderArtifactError(f"Missing manifests directory under {partition_root}")
    norm_stream_filter = (
        {_normalize_manifest_stream(s.value if isinstance(s, Stream) else str(s)) for s in stream_filter}
        if stream_filter is not None
        else None
    )
    ext = external_roots or {}
    refs: list[VerifiedChunkRef] = []
    for mpath in sorted(manifests_dir.glob("*.manifest.json")):
        doc = json.loads(mpath.read_text(encoding="utf-8"))
        if doc.get("manifest_schema_version") != "raw-chunk-manifest.v1":
            raise RecorderArtifactError(f"Unsupported manifest schema in {mpath.name}")
        c_market = str(doc["market"])
        c_symbol = str(doc["symbol"])
        c_stream = str(doc["stream"])
        tr = doc.get("receive_time_utc_range_ns") or {}
        c_min = int(tr["min"]) if tr.get("min") is not None else None
        c_max = int(tr["max"]) if tr.get("max") is not None else None
        if not _matches_chunk_filters(
            c_market=c_market,
            c_symbol=c_symbol,
            c_stream=c_stream,
            c_min=c_min,
            c_max=c_max,
            market_filter=market_filter,
            symbol_filter=symbol_filter,
            norm_stream_filter=norm_stream_filter,
            min_utc_ns=min_utc_ns,
            max_utc_ns=max_utc_ns,
        ):
            continue
        artifact = (partition_root / str(doc["relative_path"])).resolve()
        if not artifact.is_file():
            resolved = _resolve_catalog_external_path(partition_root, str(doc["chunk_id"]), ext)
            if resolved is None:
                raise RecorderArtifactError(f"Artifact unavailable for chunk {doc['chunk_id']}")
            artifact = resolved
        refs.append(
            VerifiedChunkRef(
                chunk_id=str(doc["chunk_id"]),
                artifact_path=artifact,
                market=c_market,
                symbol=c_symbol,
                stream=c_stream,
                stored_bytes=int(doc["stored_bytes"]),
                stored_sha256=str(doc["stored_sha256"]),
                uncompressed_sha256=str(doc["uncompressed_sha256"]),
                created_at_utc_ns=int(doc["created_at_utc_ns"]),
                receive_min_ns=c_min,
                receive_max_ns=c_max,
                complete=bool(doc.get("complete", False)),
                gap=bool(doc.get("gap", False)),
            )
        )
    refs.sort(key=lambda r: (r.created_at_utc_ns, r.chunk_id))
    return refs


def load_mcp_export_chunks(
    export_root: Path,
    *,
    revision_manifest_path: Path | None = None,
    market_filter: set[Market] | None = None,
    symbol_filter: set[str] | None = None,
    stream_filter: set[str | Stream] | None = None,
    min_utc_ns: int | None = None,
    max_utc_ns: int | None = None,
) -> list[VerifiedChunkRef]:
    """Load and prune verified chunks using an MCP export revision manifest on the same host."""
    if revision_manifest_path is None:
        latest = json.loads((export_root / "latest.json").read_text(encoding="utf-8"))
        revision_manifest_path = export_root / str(latest["manifest_rel_path"])
    manifest = json.loads(revision_manifest_path.read_text(encoding="utf-8"))
    archive_targets = {
        tid: Path(str(info["local_path"]))
        for tid, info in manifest.get("archive_targets", {}).items()
        if info.get("local_path")
    }
    norm_stream_filter = (
        {_normalize_manifest_stream(s.value if isinstance(s, Stream) else str(s)) for s in stream_filter}
        if stream_filter is not None
        else None
    )
    rec_markets = (
        {_MARKET_TO_RECORDER_CANDIDATES[m][0] for m in market_filter if m in _MARKET_TO_RECORDER_CANDIDATES}
        if market_filter
        else None
    )
    refs: list[VerifiedChunkRef] = []
    seen_chunks: set[str] = set()
    for part in manifest.get("partitions", []):
        if rec_markets and part.get("market") not in rec_markets:
            continue
        chunks_url = str(part["files"]["chunks_jsonl"]["url_path"])
        rel = chunks_url.removeprefix("export/")
        chunks_jsonl = export_root / rel
        if not chunks_jsonl.is_file():
            continue
        part_market = str(part["market"])
        active_roots = [
            Path(p) / "partitions" / str(part["partition_id"])
            for p in manifest.get("markets", {}).get(part_market, {}).get("active_roots", [])
        ]
        with chunks_jsonl.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                row = json.loads(line)
                cid = str(row["chunk_id"])
                if cid in seen_chunks or not row.get("ready", False):
                    continue
                tr = row.get("receive_time_utc_range_ns") or {}
                c_min = int(tr["min"]) if tr.get("min") is not None else None
                c_max = int(tr["max"]) if tr.get("max") is not None else None
                if not _matches_chunk_filters(
                    c_market=str(row["market"]),
                    c_symbol=str(row["symbol"]),
                    c_stream=str(row["stream"]),
                    c_min=c_min,
                    c_max=c_max,
                    market_filter=market_filter,
                    symbol_filter=symbol_filter,
                    norm_stream_filter=norm_stream_filter,
                    min_utc_ns=min_utc_ns,
                    max_utc_ns=max_utc_ns,
                ):
                    continue
                artifact: Path | None = None
                sid = row.get("storage_id")
                t_rel = row.get("target_relative_path")
                if sid and t_rel and str(sid) in archive_targets:
                    cand = archive_targets[str(sid)] / str(t_rel)
                    if cand.is_file():
                        artifact = cand
                if artifact is None:
                    for ar in active_roots:
                        mfile = ar / "manifests" / f"{cid}.manifest.json"
                        if mfile.is_file():
                            mdoc = json.loads(mfile.read_text(encoding="utf-8"))
                            cand = ar / str(mdoc["relative_path"])
                            if cand.is_file():
                                artifact = cand
                                break
                if artifact is None:
                    raise RecorderArtifactError(
                        f"Sealed artifact for ready chunk {cid} not found on archive or active roots"
                    )
                seen_chunks.add(cid)
                refs.append(
                    VerifiedChunkRef(
                        chunk_id=cid,
                        artifact_path=artifact,
                        market=str(row["market"]),
                        symbol=str(row["symbol"]),
                        stream=str(row["stream"]),
                        stored_bytes=int(row["stored_bytes"]),
                        stored_sha256=str(row["stored_sha256"]),
                        uncompressed_sha256=str(row["uncompressed_sha256"]),
                        created_at_utc_ns=int(row["created_at_utc_ns"]),
                        receive_min_ns=c_min,
                        receive_max_ns=c_max,
                        complete=not bool(row.get("gap", False)),
                        gap=bool(row.get("gap", False)),
                    )
                )
    refs.sort(key=lambda r: (r.created_at_utc_ns, r.chunk_id))
    return refs


def read_recorder_events(
    chunks: Iterable[VerifiedChunkRef],
    *,
    market_filter: set[Market] | None = None,
    symbol_filter: set[str] | None = None,
    stream_filter: set[str | Stream] | None = None,
    min_utc_ns: int | None = None,
    max_utc_ns: int | None = None,
    verify_hashes: bool = True,
) -> list[FeedEvent]:
    """Decode, filter [min_utc_ns, max_utc_ns), and causally sort BMD FeedEvents."""
    norm_stream_filter = (
        {_normalize_manifest_stream(s.value if isinstance(s, Stream) else str(s)) for s in stream_filter}
        if stream_filter is not None
        else None
    )
    raw_items: list[tuple[int, int, FeedEvent]] = []
    ordinal = 0
    for chunk in chunks:
        for env in iter_raw_chunk_records(chunk, verify_hashes=verify_hashes):
            if min_utc_ns is not None and env.receive_time_utc_ns < min_utc_ns:
                continue
            if max_utc_ns is not None and env.receive_time_utc_ns >= max_utc_ns:
                continue
            mkt = _parse_market(env.market)
            if market_filter and mkt not in market_filter:
                continue
            if symbol_filter and env.symbol not in {"__GLOBAL__", "__MIXED__"} and env.symbol not in symbol_filter:
                continue
            if norm_stream_filter and _normalize_manifest_stream(env.stream) not in norm_stream_filter:
                continue
            res = parse_raw_record(
                market=mkt,
                symbol=env.symbol,
                stream=env.stream,
                raw_payload=env.raw_payload,
                receive_time_utc_ns=env.receive_time_utc_ns,
                receive_monotonic_ns=env.receive_monotonic_ns,
                quality_flags=env.capture_flags,
                source="recorder_raw",
            )
            if res is None:
                continue
            items = res if isinstance(res, list) else [res]
            for item in items:
                if symbol_filter and getattr(item, "symbol", None) not in symbol_filter:
                    continue
                if norm_stream_filter and item.stream.value not in norm_stream_filter:
                    continue
                raw_items.append((env.receive_time_utc_ns, ordinal, item))
                ordinal += 1

    raw_items.sort(key=lambda tup: (tup[0], tup[1]))

    out: list[FeedEvent] = []
    seq = 0
    from dataclasses import replace

    for ts_ns, _, ev in raw_items:
        if isinstance(ev, BookTickerEvent) and "sequence_gap" in ev.quality_flags:
            seq += 1
            out.append(
                StreamHealthEvent(
                    venue=ev.venue,
                    market=ev.market,
                    symbol=ev.symbol,
                    stream=Stream.BOOK_TICKER,
                    status="GAP",
                    reason="recorder_sequence_gap",
                    observed_time_utc_ns=ts_ns,
                    delivery_seq=seq,
                    source=ev.source,
                )
            )
        seq += 1
        out.append(replace(ev, delivery_seq=seq))
    return out


def bmd_book_ticker_to_quote_tick(
    ev: BookTickerEvent,
    instrument: Any,
    *,
    delay_ns: int = 0,
) -> tuple[Any, dict[str, Any]]:
    """Convert BMD BookTickerEvent -> Nautilus QuoteTick + explicit clock/id provenance."""
    from nautilus_trader.model import Price, Quantity, QuoteTick

    if ev.receive_time_utc_ns is None or ev.receive_time_utc_ns <= 0:
        raise ValueError("BookTickerEvent missing receive_time_utc_ns")
    if ev.exchange_event_time_ms is not None:
        ts_event = ev.exchange_event_time_ms * 1_000_000
        ts_event_source = "exchange_event_time_ms"
    else:
        ts_event = ev.receive_time_utc_ns
        ts_event_source = "capture_receive_proxy"
    ts_init = ev.receive_time_utc_ns + delay_ns
    p_dp = instrument.price_precision
    s_dp = instrument.size_precision
    tick = QuoteTick(
        instrument_id=instrument.id,
        bid_price=Price.from_decimal_dp(Decimal(ev.best_bid_price), p_dp),
        ask_price=Price.from_decimal_dp(Decimal(ev.best_ask_price), p_dp),
        bid_size=Quantity.from_decimal_dp(Decimal(ev.best_bid_quantity), s_dp),
        ask_size=Quantity.from_decimal_dp(Decimal(ev.best_ask_quantity), s_dp),
        ts_event=ts_event,
        ts_init=ts_init,
    )
    prov = {
        "update_id": ev.update_id,
        "delivery_seq": ev.delivery_seq,
        "ts_event": ts_event,
        "ts_init": ts_init,
        "ts_event_source": ts_event_source,
        "quality_flags": list(ev.quality_flags),
        "bid_price": str(tick.bid_price),
        "ask_price": str(tick.ask_price),
        "bid_size": str(tick.bid_size),
        "ask_size": str(tick.ask_size),
    }
    return tick, prov


def bmd_agg_trade_to_trade_tick(
    ev: AggTradeEvent,
    instrument: Any,
    *,
    delay_ns: int = 0,
) -> Any:
    """Convert BMD AggTradeEvent -> Nautilus TradeTick."""
    from nautilus_trader.model import AggressorSide, Price, Quantity, TradeId, TradeTick

    if ev.receive_time_utc_ns is None or ev.receive_time_utc_ns <= 0:
        raise ValueError("AggTradeEvent missing receive_time_utc_ns")
    ts_event = ev.trade_time_ns if ev.trade_time_ns is not None else ev.trade_time_ms * 1_000_000
    ts_init = ev.receive_time_utc_ns + delay_ns
    aggressor = AggressorSide.SELL if ev.buyer_is_maker else AggressorSide.BUY
    return TradeTick(
        instrument_id=instrument.id,
        price=Price.from_decimal_dp(Decimal(ev.price), instrument.price_precision),
        size=Quantity.from_decimal_dp(Decimal(ev.quantity), instrument.size_precision),
        aggressor_side=aggressor,
        trade_id=TradeId(str(ev.aggregate_trade_id)),
        ts_event=ts_event,
        ts_init=ts_init,
    )


def bmd_health_to_instrument_status(
    ev: StreamHealthEvent,
    instrument: Any,
    *,
    delay_ns: int = 0,
) -> Any:
    """Convert BMD StreamHealthEvent -> Nautilus InstrumentStatus."""
    from nautilus_trader.model import InstrumentStatus, MarketStatusAction

    healthy = ev.status == "LIVE"
    action = MarketStatusAction.TRADING if healthy else MarketStatusAction.HALT
    ts_ev = ev.observed_time_utc_ns
    return InstrumentStatus(
        instrument_id=instrument.id,
        action=action,
        ts_event=ts_ev,
        ts_init=ts_ev + delay_ns,
        reason=f"{ev.status}:{ev.reason}",
        is_trading=healthy,
        is_quoting=healthy,
    )


def events_to_nautilus_data(
    events: Iterable[FeedEvent],
    instruments: dict[tuple[Market, str], Any],
    *,
    delay_ns: int = 0,
) -> NautilusConversionSummary:
    """Convert typed BMD FeedEvents to Nautilus data objects preserving delivery order."""
    data: list[Any] = []
    quote_prov: list[dict[str, Any]] = []
    proxy_count = 0
    health_count = 0
    for ev in events:
        key = (ev.market, ev.symbol)
        inst = instruments.get(key)
        if inst is None:
            continue
        if isinstance(ev, BookTickerEvent):
            tick, prov = bmd_book_ticker_to_quote_tick(ev, inst, delay_ns=delay_ns)
            if prov["ts_event_source"] == "capture_receive_proxy":
                proxy_count += 1
            data.append(tick)
            quote_prov.append(prov)
        elif isinstance(ev, AggTradeEvent):
            data.append(bmd_agg_trade_to_trade_tick(ev, inst, delay_ns=delay_ns))
        elif isinstance(ev, StreamHealthEvent):
            health_count += 1
            data.append(bmd_health_to_instrument_status(ev, inst, delay_ns=delay_ns))
    return NautilusConversionSummary(
        data=data,
        quote_provenance=quote_prov,
        proxy_ts_event_count=proxy_count,
        health_events_count=health_count,
    )


__all__ = [
    "NautilusConversionSummary",
    "RawEnvelopeRecord",
    "RecorderArtifactError",
    "VerifiedChunkRef",
    "bmd_agg_trade_to_trade_tick",
    "bmd_book_ticker_to_quote_tick",
    "bmd_health_to_instrument_status",
    "events_to_nautilus_data",
    "iter_raw_chunk_records",
    "load_mcp_export_chunks",
    "load_partition_chunks",
    "read_recorder_events",
    "verify_sealed_zstd_artifact",
]
