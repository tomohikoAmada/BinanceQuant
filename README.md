# BinanceQuant

Downstream single repository (per Team C D1) consuming `BinanceMarketDataContracts`, `BinanceMarketDataRecorder`, and `BinanceMarketDataGateway` (`127.0.0.1:50051`).

## Packages
- `bq_nautilus`: Active **NautilusTrader (`2.0.0rc9`, Python 3.13 `uv`)** thin adapter & validation layer (`M-NT1` completed 2026-11-25):
  - `instruments.py`: D4-compliant contemporaneous Spot (`CurrencyPair`) & USD-M (`CryptoPerpetual`) instrument builders from `ReferenceObservationEvent` / `exchangeInfo` (5 BMD symbols).
  - `recorder.py`: Self-contained Python 3.13 verified Recorder raw chunk & `mcp-export` reader (verifying `stored_bytes`, `stored_sha256`, `uncompressed_sha256`, Raw v1 `CRC32C`) + Nautilus converters.
  - `vision.py`: Fixed Binance Vision parser (2025-01-01+ Spot microsecond normalization, strict boolean parsing, coverage registry).
  - `live.py`: `grpc.aio` `BmdDataClient` with Fail-Fast capability preflight, monotonic+anchored-UTC `STALE` watchdog, and `GAP`/`STALE` `InstrumentStatus(HALT)` gating.
  - `spot_runner.py`: Strict Spot F1 `BacktestEngine` & `LiveNode (Sandbox)` (`BookType.L1_MBP`, `liquidity_consumption=True`, `bar_execution=False`), cash/inflight reservation guard, two-stage UID reconciliation, and causality/delay verification.
- `bq_feed`: Unified typed events (`events.py`), Recorder Raw/normalized deterministic replay (`replay.py`, including Vision microsecond & strict bool fixes), Gateway live gRPC client with `enabled_streams` Fail-Fast + `STALE` watchdog (`live.py`), and capability matrix / Recorder-vs-Gateway parity checker (`parity.py`).
- `bq_feature`: Pure causal stateful feature engine (`update(state, event)` + `on_health`) with dual clocks, R1/R7/N5/R9/R6/OFI/Microprice/causal K-lines, and `features.parquet` export.
- `bq_model`: Purged + Embargoed Walk-Forward trainer and `<1ms` inference bundles (`models/<model_id>/`).
- `bq_mcp`: Local stdio MCP server (6 tools, `<64 KiB` responses, AST + causality + parity guard).
- Frozen legacy modules (per 2026-11-25 D1/D9 decision, preserved for regression/reference only): `bq_backtest`, `bq_signal`, `bq_order`.

## Locked Nautilus Environment (Python 3.13)
- Upstream fork: `https://github.com/tomohikoAmada/nautilus_trader` (`v2.0.0rc9`, commit `da6b57aefdd62b44f675e434ffcfefca7f4025dd`).
- Lockfiles: `locks/nautilus-py313.lock` (`uv pip install --require-hashes`) and `locks/nautilus-2.0.0rc9-wheels.json`.
