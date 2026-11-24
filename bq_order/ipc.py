"""Local Unix Domain Socket JSON-line IPC boundary keeping keys isolated inside BQOrder."""

from __future__ import annotations

from decimal import Decimal
import json
import os
from pathlib import Path
import socket
import socketserver
import threading
from typing import Any

from bq_backtest.strategy import OrderIntent
from bq_feed.events import Market

from .guard import OrderExecutionGuard


def serialize_intent(
    intent: OrderIntent,
    *,
    reference_price: Decimal = Decimal("0"),
    limit_price: Decimal | None = None,
) -> dict[str, Any]:
    return {
        "intent_id": intent.intent_id,
        "market": intent.market.value,
        "symbol": intent.symbol,
        "side": intent.side,
        "quantity": str(intent.quantity),
        "order_type": intent.order_type,
        "limit_price": str(limit_price) if limit_price is not None else None,
        "reference_price": str(reference_price),
        "decision_seq": intent.decision_seq,
        "decision_ts_ns": intent.decision_ts_ns,
        "decision_infer_ns": intent.decision_infer_ns,
        "reason": intent.reason,
    }


def deserialize_intent(payload: dict[str, Any]) -> tuple[OrderIntent, Decimal | None, Decimal]:
    intent = OrderIntent(
        intent_id=str(payload["intent_id"]),
        market=Market(str(payload["market"])),
        symbol=str(payload["symbol"]),
        side="BUY" if payload["side"] == "BUY" else "SELL",
        quantity=Decimal(str(payload["quantity"])),
        order_type=str(payload.get("order_type", "MARKET_TAKER")),
        decision_seq=int(payload.get("decision_seq", 0)),
        decision_ts_ns=int(payload.get("decision_ts_ns", 0)),
        decision_infer_ns=int(payload.get("decision_infer_ns", 0)),
        reason=str(payload.get("reason", "ipc_intent")),
    )
    limit_px = Decimal(str(payload["limit_price"])) if payload.get("limit_price") is not None else None
    ref_px = Decimal(str(payload.get("reference_price") or "0"))
    return intent, limit_px, ref_px


class _OrderUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class OrderIpcServer:
    """Single-process UDS receiver forwarding OrderIntents to OrderExecutionGuard."""

    def __init__(self, socket_path: Path, guard: OrderExecutionGuard, *, default_ref_price: Decimal = Decimal("50000")) -> None:
        self._socket_path = Path(socket_path)
        self._guard = guard
        self._default_ref_price = default_ref_price
        self._lock = threading.Lock()
        self._server: _OrderUnixServer | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self._socket_path.exists():
            self._socket_path.unlink()

        parent = self

        class _Handler(socketserver.StreamRequestHandler):
            def handle(self) -> None:
                for raw in self.rfile:
                    line = raw.decode("utf-8").strip()
                    if not line:
                        continue
                    try:
                        req = json.loads(line)
                        cmd = req.get("cmd", "submit")
                        with parent._lock:
                            if cmd == "submit":
                                intent, limit_px, ref_px = deserialize_intent(req["intent"])
                                eff_ref = ref_px if ref_px > 0 else parent._default_ref_price
                                res = parent._guard.submit_intent(
                                    intent, reference_price=eff_ref, limit_price=limit_px
                                )
                                resp = {
                                    "ok": res.accepted,
                                    "intent_id": res.intent_id,
                                    "state": res.state.value,
                                    "reason": res.reason,
                                }
                            elif cmd == "kill_switch":
                                ks, summary = parent._guard.trigger_kill_switch(
                                    reason=str(req.get("reason", "ipc_kill_switch"))
                                )
                                resp = {
                                    "ok": True,
                                    "active": ks.active,
                                    "still_pending": summary.still_pending_count,
                                }
                            elif cmd == "status":
                                ks = parent._guard.ledger.get_kill_switch()
                                active = parent._guard.ledger.list_active_or_unknown()
                                resp = {
                                    "ok": True,
                                    "kill_switch_active": ks.active,
                                    "active_orders": len(active),
                                }
                            else:
                                resp = {"ok": False, "error": f"unknown_cmd:{cmd}"}
                    except Exception as exc:
                        resp = {"ok": False, "error": f"{type(exc).__name__}:{exc}"}
                    self.wfile.write((json.dumps(resp) + "\n").encode("utf-8"))
                    self.wfile.flush()

        self._server = _OrderUnixServer(str(self._socket_path), _Handler)
        os.chmod(self._socket_path, 0o600)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None
        if self._socket_path.exists():
            self._socket_path.unlink()


class OrderIpcClient:
    """Credential-free IPC sink callable from SignalRunner(mode='live', intent_sink=client)."""

    def __init__(self, socket_path: Path, *, timeout_s: float = 3.0, default_ref_price: Decimal = Decimal("0")) -> None:
        self._socket_path = Path(socket_path)
        self._timeout_s = timeout_s
        self._default_ref_price = default_ref_price
        self.last_responses: list[dict[str, Any]] = []

    def send_command(self, payload: dict[str, Any]) -> dict[str, Any]:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.settimeout(self._timeout_s)
            sock.connect(str(self._socket_path))
            sock.sendall((json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8"))
            buf = b""
            while not buf.endswith(b"\n"):
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buf += chunk
        return json.loads(buf.decode("utf-8").strip())

    def __call__(self, intent: OrderIntent) -> None:
        ref = intent.reference_price if intent.reference_price is not None and intent.reference_price > 0 else self._default_ref_price
        resp = self.send_command({"cmd": "submit", "intent": serialize_intent(intent, reference_price=ref)})
        self.last_responses.append(resp)
