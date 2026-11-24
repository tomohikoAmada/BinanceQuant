"""Exchange client abstraction, deterministic fault-injectable FakeSpotExchange, and Binance Spot Testnet REST client."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hashlib
import hmac
import json
import os
import time
from typing import Literal, Protocol
import urllib.error
import urllib.parse
import urllib.request

from .models import ExchangeFill, ExchangeOrderSnapshot


class ExchangeTransportError(RuntimeError):
    """Raised when the wire response is lost/timed out and the order fate on exchange is unknown."""


class ExchangeRejectedError(RuntimeError):
    """Raised when the exchange explicitly rejected an order/cancel cleanly without side effects."""

    def __init__(self, code: int, msg: str) -> None:
        super().__init__(f"Binance API error {code}: {msg}")
        self.code = code
        self.msg = msg


class ExchangeClient(Protocol):
    def submit_order(
        self,
        *,
        symbol: str,
        side: Literal["BUY", "SELL"],
        order_type: Literal["MARKET", "LIMIT"],
        quantity: Decimal,
        client_order_id: str,
        limit_price: Decimal | None = None,
    ) -> ExchangeOrderSnapshot: ...

    def query_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot | None: ...

    def query_trades(self, *, symbol: str, client_order_id: str) -> tuple[ExchangeFill, ...]: ...

    def cancel_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot: ...


class FakeSpotExchange:
    """Deterministic Spot fake exchange simulating real Binance Spot edge cases & fault injection.

    Key realistic Spot semantics verified by Team C:
    - Re-submitting the SAME newClientOrderId AFTER the prior order is FILLED is accepted by Spot
      and creates a SECOND fill unless BQOrder blocks resends locally!
    - Supports fault injection hooks (`drop_ack_after_fill`, `timeout_before_exchange`,
      `partial_fill_ratio`, `fail_next_cancel_unreachable`, `network_unreachable`).
    """

    def __init__(
        self,
        *,
        initial_quote_cash: Decimal = Decimal("100000"),
        initial_base_qty: Decimal = Decimal("0"),
        fill_price: Decimal = Decimal("50000"),
        fee_bps: Decimal = Decimal("10"),
    ) -> None:
        self.quote_cash = initial_quote_cash
        self.base_qty = initial_base_qty
        self.fill_price = fill_price
        self.fee_bps = fee_bps

        self.submit_wire_calls = 0
        self.query_wire_calls = 0
        self.cancel_wire_calls = 0

        self._seq = 0
        self._trade_seq = 0
        self._orders: dict[str, ExchangeOrderSnapshot] = {}
        self._order_history: list[ExchangeOrderSnapshot] = []
        self._trades_by_cid: dict[str, list[ExchangeFill]] = {}

        # Fault-injection flags (one-shot except persistent network_unreachable)
        self.drop_next_ack_after_fill = False
        self.timeout_next_before_exchange = False
        self.next_partial_fill_ratio: Decimal | None = None
        self.next_cancel_race_fills_rest = False
        self.fail_next_cancel_count = 0
        self.network_unreachable = False

    def _next_order_id(self) -> str:
        self._seq += 1
        return f"fake-ex-{self._seq}"

    def _next_fill_id(self) -> str:
        self._trade_seq += 1
        return f"fake-fill-{self._trade_seq}"

    def submit_order(
        self,
        *,
        symbol: str,
        side: Literal["BUY", "SELL"],
        order_type: Literal["MARKET", "LIMIT"],
        quantity: Decimal,
        client_order_id: str,
        limit_price: Decimal | None = None,
    ) -> ExchangeOrderSnapshot:
        self.submit_wire_calls += 1
        if self.network_unreachable:
            raise ExchangeTransportError("fake_network_unreachable")
        if self.timeout_next_before_exchange:
            self.timeout_next_before_exchange = False
            raise ExchangeTransportError("timeout_before_exchange_receipt")

        # Realistic Binance Spot trap: rejects active duplicate cid, but RE-ACCEPTS cid after FILLED!
        prior = self._orders.get(client_order_id)
        if prior is not None and prior.status in ("NEW", "PARTIALLY_FILLED"):
            raise ExchangeRejectedError(-2010, f"Duplicate order sent for open {client_order_id}")

        px = limit_price if (limit_price is not None and order_type == "LIMIT") else self.fill_price
        ex_id = self._next_order_id()

        if self.next_partial_fill_ratio is not None:
            ratio = self.next_partial_fill_ratio
            self.next_partial_fill_ratio = None
            exec_qty = (quantity * ratio).normalize()
            status: Literal["NEW", "PARTIALLY_FILLED", "FILLED"] = (
                "NEW" if exec_qty <= 0 else ("FILLED" if exec_qty >= quantity else "PARTIALLY_FILLED")
            )
        elif order_type == "LIMIT" and limit_price is not None and (
            (side == "BUY" and limit_price < self.fill_price) or (side == "SELL" and limit_price > self.fill_price)
        ):
            exec_qty = Decimal(0)
            status = "NEW"
        else:
            exec_qty = quantity
            status = "FILLED"

        fills: list[ExchangeFill] = []
        quote_qty = Decimal(0)
        fee_quote = Decimal(0)
        if exec_qty > 0:
            quote_qty = exec_qty * px
            fee_quote = (quote_qty * self.fee_bps) / Decimal("10000")
            if side == "BUY":
                self.quote_cash -= quote_qty + fee_quote
                self.base_qty += exec_qty
            else:
                self.base_qty -= exec_qty
                self.quote_cash += quote_qty - fee_quote
            fl = ExchangeFill(
                fill_id=self._next_fill_id(),
                client_order_id=client_order_id,
                symbol=symbol,
                side=side,
                qty=exec_qty,
                price=px,
                fee=fee_quote,
                fee_asset="USDT",
                trade_time_ms=1795490000000 + self._trade_seq,
            )
            fills.append(fl)
            self._trades_by_cid.setdefault(client_order_id, []).append(fl)

        snap = ExchangeOrderSnapshot(
            client_order_id=client_order_id,
            exchange_order_id=ex_id,
            symbol=symbol,
            side=side,
            status=status,
            orig_qty=quantity,
            executed_qty=exec_qty,
            cummulative_quote_qty=quote_qty,
            avg_price=px if exec_qty > 0 else None,
            fee_quote=fee_quote,
            fee_asset="USDT",
            fills=tuple(fills),
        )
        self._orders[client_order_id] = snap
        self._order_history.append(snap)

        if self.drop_next_ack_after_fill:
            self.drop_next_ack_after_fill = False
            raise ExchangeTransportError("ack_dropped_after_exchange_execution")

        return snap

    def query_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot | None:
        self.query_wire_calls += 1
        if self.network_unreachable:
            raise ExchangeTransportError("fake_network_unreachable")
        return self._orders.get(client_order_id)

    def query_trades(self, *, symbol: str, client_order_id: str) -> tuple[ExchangeFill, ...]:
        if self.network_unreachable:
            raise ExchangeTransportError("fake_network_unreachable")
        return tuple(self._trades_by_cid.get(client_order_id, ()))

    def cancel_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot:
        self.cancel_wire_calls += 1
        if self.network_unreachable:
            raise ExchangeTransportError("fake_network_unreachable")
        if self.fail_next_cancel_count > 0:
            self.fail_next_cancel_count -= 1
            raise ExchangeTransportError("transient_cancel_timeout")
        cur = self._orders.get(client_order_id)
        if cur is None:
            raise ExchangeRejectedError(-2011, "Unknown order sent.")
        if self.next_cancel_race_fills_rest and cur.status in ("NEW", "PARTIALLY_FILLED"):
            self.next_cancel_race_fills_rest = False
            rem = cur.orig_qty - cur.executed_qty
            if rem > 0:
                px = self.fill_price
                q_delta = rem * px
                f_delta = (q_delta * self.fee_bps) / Decimal("10000")
                if cur.side == "BUY":
                    self.quote_cash -= q_delta + f_delta
                    self.base_qty += rem
                else:
                    self.base_qty -= rem
                    self.quote_cash += q_delta - f_delta
                fl = ExchangeFill(
                    fill_id=self._next_fill_id(),
                    client_order_id=client_order_id,
                    symbol=symbol,
                    side=cur.side,
                    qty=rem,
                    price=px,
                    fee=f_delta,
                    fee_asset="USDT",
                    trade_time_ms=1795490000000 + self._trade_seq,
                )
                self._trades_by_cid.setdefault(client_order_id, []).append(fl)
                all_fills = tuple(self._trades_by_cid[client_order_id])
                tot_q = sum((f.qty * f.price for f in all_fills), Decimal(0))
                tot_f = sum((f.fee for f in all_fills), Decimal(0))
                filled_snap = ExchangeOrderSnapshot(
                    client_order_id=client_order_id,
                    exchange_order_id=cur.exchange_order_id,
                    symbol=symbol,
                    side=cur.side,
                    status="FILLED",
                    orig_qty=cur.orig_qty,
                    executed_qty=cur.orig_qty,
                    cummulative_quote_qty=tot_q,
                    avg_price=tot_q / cur.orig_qty,
                    fee_quote=tot_f,
                    fee_asset="USDT",
                    fills=all_fills,
                )
                self._orders[client_order_id] = filled_snap
            raise ExchangeRejectedError(-2011, "Unknown order sent (already filled during cancel).")

        if cur.status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
            return cur
        canceled = ExchangeOrderSnapshot(
            client_order_id=cur.client_order_id,
            exchange_order_id=cur.exchange_order_id,
            symbol=cur.symbol,
            side=cur.side,
            status="CANCELED",
            orig_qty=cur.orig_qty,
            executed_qty=cur.executed_qty,
            cummulative_quote_qty=cur.cummulative_quote_qty,
            avg_price=cur.avg_price,
            fee_quote=cur.fee_quote,
            fee_asset=cur.fee_asset,
            fills=cur.fills,
        )
        self._orders[client_order_id] = canceled
        return canceled


@dataclass(frozen=True, slots=True)
class SpotCredentials:
    api_key: str
    api_secret: str
    base_url: str = "https://testnet.binance.vision"
    allow_mainnet: bool = False

    @classmethod
    def from_env(cls, *, allow_mainnet: bool = False) -> SpotCredentials:
        key = os.environ.get("BQ_ORDER_API_KEY", "").strip()
        secret = os.environ.get("BQ_ORDER_API_SECRET", "").strip()
        base = os.environ.get("BQ_ORDER_BASE_URL", "https://testnet.binance.vision").strip()
        if not key or not secret:
            raise ValueError("Missing BQ_ORDER_API_KEY / BQ_ORDER_API_SECRET environment variables")
        return cls(api_key=key, api_secret=secret, base_url=base, allow_mainnet=allow_mainnet)


class BinanceSpotRestClient:
    """Signed Binance Spot REST v3 client (defaults to Spot Testnet)."""

    def __init__(self, creds: SpotCredentials, *, timeout_s: float = 5.0) -> None:
        if "testnet.binance.vision" not in creds.base_url and not creds.allow_mainnet:
            raise ValueError(
                f"Refusing non-testnet base_url={creds.base_url!r} without explicit allow_mainnet=True"
            )
        self._creds = creds
        self._timeout_s = timeout_s

    def _signed_request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        path: str,
        params: dict[str, str],
    ) -> dict[str, object] | list[dict[str, object]]:
        p = dict(params)
        p["timestamp"] = str(int(time.time() * 1000))
        p.setdefault("recvWindow", "5000")
        query = urllib.parse.urlencode(p)
        sig = hmac.new(
            self._creds.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        full_url = f"{self._creds.base_url.rstrip('/')}{path}?{query}&signature={sig}"
        req = urllib.request.Request(
            full_url,
            method=method,
            headers={"X-MBX-APIKEY": self._creds.api_key},
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout_s) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            try:
                err = json.loads(body)
                code = int(err.get("code", exc.code))
                msg = str(err.get("msg", body))
            except Exception:
                code = exc.code
                msg = body
            raise ExchangeRejectedError(code, msg) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ExchangeTransportError(f"transport_failure:{type(exc).__name__}:{exc}") from exc

    @staticmethod
    def _parse_order_payload(data: dict[str, object], fallback_cid: str) -> ExchangeOrderSnapshot:
        fills_raw = data.get("fills")
        parsed_fills: list[ExchangeFill] = []
        fee_sum = Decimal(0)
        fee_asset = "USDT"
        side = str(data.get("side", "BUY"))
        sym = str(data.get("symbol", "BTCUSDT"))
        cid = str(data.get("clientOrderId", fallback_cid))
        if isinstance(fills_raw, list):
            for idx, f in enumerate(fills_raw):
                if not isinstance(f, dict):
                    continue
                comm = Decimal(str(f.get("commission", "0")))
                fa = str(f.get("commissionAsset", "USDT"))
                fee_sum += comm
                fee_asset = fa
                parsed_fills.append(
                    ExchangeFill(
                        fill_id=str(f.get("tradeId", f"{cid}-ack-{idx}")),
                        client_order_id=cid,
                        symbol=sym,
                        side="BUY" if side == "BUY" else "SELL",
                        qty=Decimal(str(f.get("qty", "0"))),
                        price=Decimal(str(f.get("price", "0"))),
                        fee=comm,
                        fee_asset=fa,
                        trade_time_ms=int(data.get("transactTime") or data.get("updateTime") or 0),
                    )
                )
        exec_qty = Decimal(str(data.get("executedQty", "0")))
        cumm_quote = Decimal(str(data.get("cummulativeQuoteQty", "0")))
        avg_px = (cumm_quote / exec_qty) if exec_qty > 0 else None
        status_raw = str(data.get("status", "NEW"))
        return ExchangeOrderSnapshot(
            client_order_id=cid,
            exchange_order_id=str(data["orderId"]) if data.get("orderId") is not None else None,
            symbol=sym,
            side="BUY" if side == "BUY" else "SELL",
            status=status_raw,  # type: ignore[arg-type]
            orig_qty=Decimal(str(data.get("origQty", "0"))),
            executed_qty=exec_qty,
            cummulative_quote_qty=cumm_quote,
            avg_price=avg_px,
            fee_quote=fee_sum,
            fee_asset=fee_asset,
            fills=tuple(parsed_fills),
        )

    def submit_order(
        self,
        *,
        symbol: str,
        side: Literal["BUY", "SELL"],
        order_type: Literal["MARKET", "LIMIT"],
        quantity: Decimal,
        client_order_id: str,
        limit_price: Decimal | None = None,
    ) -> ExchangeOrderSnapshot:
        params: dict[str, str] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "quantity": str(quantity),
            "newClientOrderId": client_order_id,
            "newOrderRespType": "FULL",
        }
        if order_type == "LIMIT":
            if limit_price is None:
                raise ValueError("LIMIT order requires limit_price")
            params["price"] = str(limit_price)
            params["timeInForce"] = "GTC"
        res = self._signed_request("POST", "/api/v3/order", params)
        assert isinstance(res, dict)
        return self._parse_order_payload(res, client_order_id)

    def query_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot | None:
        try:
            res = self._signed_request(
                "GET", "/api/v3/order", {"symbol": symbol, "origClientOrderId": client_order_id}
            )
        except ExchangeRejectedError as exc:
            if exc.code == -2013:  # Order does not exist
                return None
            raise
        assert isinstance(res, dict)
        return self._parse_order_payload(res, client_order_id)

    def query_trades(self, *, symbol: str, client_order_id: str) -> tuple[ExchangeFill, ...]:
        snap = self.query_order(symbol=symbol, client_order_id=client_order_id)
        if snap is None or snap.exchange_order_id is None:
            return ()
        res = self._signed_request(
            "GET", "/api/v3/myTrades", {"symbol": symbol, "orderId": snap.exchange_order_id}
        )
        if not isinstance(res, list):
            return ()
        out: list[ExchangeFill] = []
        for t in res:
            if not isinstance(t, dict):
                continue
            out.append(
                ExchangeFill(
                    fill_id=str(t["id"]),
                    client_order_id=client_order_id,
                    symbol=symbol,
                    side="BUY" if t.get("isBuyer") else "SELL",
                    qty=Decimal(str(t.get("qty", "0"))),
                    price=Decimal(str(t.get("price", "0"))),
                    fee=Decimal(str(t.get("commission", "0"))),
                    fee_asset=str(t.get("commissionAsset", "USDT")),
                    trade_time_ms=int(t.get("time", 0)),
                )
            )
        return tuple(out)

    def cancel_order(self, *, symbol: str, client_order_id: str) -> ExchangeOrderSnapshot:
        res = self._signed_request(
            "DELETE", "/api/v3/order", {"symbol": symbol, "origClientOrderId": client_order_id}
        )
        assert isinstance(res, dict)
        return self._parse_order_payload(res, client_order_id)
