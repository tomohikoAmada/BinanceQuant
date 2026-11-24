"""CLI entrypoint for credential-isolated BQOrder execution guard."""

from __future__ import annotations

import argparse
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path
import signal
import threading

from .exchange import BinanceSpotRestClient, FakeSpotExchange, SpotCredentials
from .guard import OrderExecutionGuard
from .ipc import OrderIpcServer
from .ledger import OrderLedger
from .models import HardRiskLimits
from .risk import HardRiskGuard


def _build_guard(args: argparse.Namespace) -> tuple[OrderLedger, OrderExecutionGuard]:
    ledger = OrderLedger(args.ledger_db)
    limits = HardRiskLimits(
        max_order_qty=Decimal(args.max_order_qty),
        max_order_notional_quote=Decimal(args.max_order_notional),
        max_abs_position_qty=Decimal(args.max_abs_position),
    )
    risk = HardRiskGuard(limits)
    if getattr(args, "testnet", False):
        creds = SpotCredentials.from_env(allow_mainnet=False)
        ex = BinanceSpotRestClient(creds)
    else:
        ex = FakeSpotExchange(fill_price=Decimal(getattr(args, "fake_price", "50000")))
    guard = OrderExecutionGuard(ledger=ledger, risk_guard=risk, exchange=ex)
    guard.recover_pending()
    return ledger, guard


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bq-order")
    parser.add_argument("--ledger-db", type=Path, required=True)
    parser.add_argument("--testnet", action="store_true", help="Use Binance Spot Testnet REST API with env creds.")
    parser.add_argument("--fake-price", default="50000")
    parser.add_argument("--max-order-qty", default="0.10")
    parser.add_argument("--max-order-notional", default="10000")
    parser.add_argument("--max-abs-position", default="0.20")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_serve = sub.add_parser("serve", help="Run isolated BQOrder UDS server with startup crash recovery.")
    p_serve.add_argument("--socket", type=Path, required=True)

    sub.add_parser("recover", help="Reconcile pending/unknown orders against exchange.")
    sub.add_parser("status", help="Print Kill Switch state and active/total order counts.")

    p_ks = sub.add_parser("kill-switch", help="Persist Kill Switch and cancel/reconcile active orders.")
    p_ks.add_argument("--reason", required=True)

    p_clear = sub.add_parser("clear-kill-switch", help="Clear persisted Kill Switch after all orders terminate.")
    p_clear.add_argument("--operator", required=True)

    args = parser.parse_args(argv)
    ledger, guard = _build_guard(args)
    try:
        if args.cmd == "serve":
            srv = OrderIpcServer(args.socket, guard, default_ref_price=Decimal(args.fake_price))
            srv.start()
            stop_ev = threading.Event()
            signal.signal(signal.SIGTERM, lambda *_: stop_ev.set())
            signal.signal(signal.SIGINT, lambda *_: stop_ev.set())
            stop_ev.wait()
            srv.stop()
            return 0

        if args.cmd == "recover":
            res = guard.recover_pending()
            print(json.dumps(asdict(res), indent=2, default=str, sort_keys=True))
            return 0 if res.still_pending_count == 0 else 1

        if args.cmd == "status":
            ks = ledger.get_kill_switch()
            orders = ledger.list_orders()
            print(
                json.dumps(
                    {
                        "kill_switch": asdict(ks),
                        "total_orders": len(orders),
                        "active_or_unknown": len(ledger.list_active_or_unknown()),
                    },
                    indent=2,
                    default=str,
                    sort_keys=True,
                )
            )
            return 0

        if args.cmd == "kill-switch":
            ks, summary = guard.trigger_kill_switch(reason=args.reason)
            print(json.dumps({"kill_switch": asdict(ks), "summary": asdict(summary)}, indent=2, default=str, sort_keys=True))
            return 0 if summary.kill_switch_terminal_completed else 1

        if args.cmd == "clear-kill-switch":
            ks = guard.clear_kill_switch(operator=args.operator)
            print(json.dumps(asdict(ks), indent=2, default=str, sort_keys=True))
            return 0
    finally:
        ledger.close()

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
