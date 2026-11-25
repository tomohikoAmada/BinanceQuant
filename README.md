# BinanceQuant

Downstream single repository (per Team C D1) consuming `BinanceMarketDataContracts`, `BinanceMarketDataRecorder`, and `BinanceMarketDataGateway` (`127.0.0.1:50051`).

## Packages
- `bq_nautilus`: Active **NautilusTrader (`2.0.0rc9` / candidate `2.0.0rc9+bmd1`, Python 3.13 `uv`)** thin adapter & validation layer (`M-NT1` + `M-NT2` + `M-NT2.5` + `M-NT2.6` completed 2026-11-25):
  - `instruments.py`: D4-compliant contemporaneous Spot (`CurrencyPair`) & USD-M (`CryptoPerpetual`) instrument builders from `ReferenceObservationEvent` / `exchangeInfo` (5 BMD symbols).
  - `custom_data.py`: `CustomData` datatypes/payloads for sparse `liquidation` (`BmdLiquidationData` with explicit throttled sampling note), `reference_data` (`BmdReferenceData`), isolated cross-asset factor quotes (`BmdFactorQuoteData`), and per-stream feed quality (`BmdStreamQualityData`, decoupled from exchange-wide `InstrumentStatus`).
  - `recorder.py`: Self-contained Python 3.13 verified Recorder raw chunk & `mcp-export` reader (with USD-M `usdm`/`um_perpetual` alias resolution, verifying `stored_bytes`, `stored_sha256`, `uncompressed_sha256`, Raw v1 `CRC32C`), `AggTradeDeduplicator`, Spot/USD-M `L2SequenceBridge` (awaiting-first-diff vs bridged `pu` verification + `OrderBookDeltas` GAP clear/resync), `resolve_event_ts_ns` (USD-M `T`-first alignment), `MarkPriceUpdate`/`FundingRateUpdate` (`interval=None` when unknown), and Nautilus converters.
  - `vision.py`: Fixed Binance Vision parser (2025-01-01+ Spot microsecond normalization, strict boolean parsing, coverage registry).
  - `live.py`: `grpc.aio` `BmdDataClient` with Fail-Fast capability preflight, multi-stream ingestion (`BOOK_TICKER`, `AGG_TRADE`, `DIFF_DEPTH`/`depth_baseline`, `MARK_PRICE`/funding, sparse `LIQUIDATION`/`REFERENCE_DATA` exempt from watchdog `STALE`), and `GAP`/`STALE` `InstrumentStatus(HALT)` gating (retained for low-frequency research/parity).
  - `tonic_live.py` + `rust_bmd_tonic/`: M-NT2.5 Route A Rust `tonic` + `prost` + `PyO3` (`bmd_tonic_ext`) gateway client prototype with Rust-side `AggTrade` dedup & immediate GAP `CLEAR`/`HALT` (archived benchmark prototype; M-NT2.5 locked **Route B/B+**, Nautilus native Rust `BinanceDataClientFactory` + 300s slow REST poller, for `M-NT3`/`M-NT4` production high-freq ingestion).
  - `spot_runner.py`: Strict Spot F1 `BacktestEngine` & `LiveNode (Sandbox)` (`BookType.L1_MBP`, `liquidity_consumption=True`, `bar_execution=False`), cash/inflight reservation guard, two-stage UID reconciliation, and causality/delay verification.
  - `patterns.py`: M-NT2 verified strategy patterns — `MakerLimitPostOnlyStrategy` (with explicit `MAKER_QUEUE_BOUNDARY_NOTICE` requiring `HftBacktest` for queue-sensitive Maker alpha), `CrossAssetFactorStrategy` + `TradingAllowlistGuard`, `L2MicrostructureStrategy`, and `MultiTimescaleHybridStrategy`.
  - `ml_bridge.py`: Causal `FeatureEngine` + `ModelBundle` Nautilus bridge (`Purged+Embargoed Walk-Forward` enforcing `embargo_ns`, contiguous healthy-epoch labeling, `<1ms` inference, fail-closed `ts_init <= trained_until_ns` cutoff).
- `bq_feed`: Unified typed events (`events.py`), Recorder Raw/normalized deterministic replay (`replay.py`, including Vision microsecond & strict bool fixes), Gateway live gRPC client with `enabled_streams` Fail-Fast + `STALE` watchdog (`live.py`), and capability matrix / Recorder-vs-Gateway parity checker (`parity.py`).
- `bq_feature`: Pure causal stateful feature engine (`update(state, event)` + `on_health`) with dual clocks, R1/R7/N5/R9/R6/OFI/Microprice/causal K-lines, and `features.parquet` export.
- `bq_model`: Purged + Embargoed Walk-Forward trainer (enforcing `embargo_ns` across `walk_forward_only=True` folds) and `<1ms` inference bundles (`models/<model_id>/`).
- `bq_mcp`: Local stdio MCP server (6 tools, `<64 KiB` responses, AST + causality + parity guard).
- Frozen legacy modules (per 2026-11-25 D1/D9 decision, preserved for regression/reference only): `bq_backtest`, `bq_signal`, `bq_order`.

## Locked Nautilus Environment (Python 3.13)
- Upstream rollback baseline: `https://github.com/tomohikoAmada/nautilus_trader` (`v2.0.0rc9`, commit `da6b57aefdd62b44f675e434ffcfefca7f4025dd`), `locks/nautilus-py313.lock`, `locks/nautilus-2.0.0rc9-wheels.json`.
- M-NT2.6 candidate fork build: branch `bmd-2.0.0rc9-bmd1` (`2.0.0rc9+bmd1`, commit `d1dee21c17d549d13ff4192b6db92ddf3c2c143e`), `locks/nautilus-bmd1-py313.lock`, `locks/nautilus-2.0.0rc9+bmd1-wheels.json` (`manylinux_2_39_x86_64` + `macosx_11_0_arm64`). Rollback requires `--reinstall-package nautilus-trader` with `locks/nautilus-py313.lock`.
