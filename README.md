# BinanceQuant

Downstream single repository (per Team C D1) consuming `BinanceMarketDataContracts`, `BinanceMarketDataRecorder`, and `BinanceMarketDataGateway` (`127.0.0.1:50051`).

## Packages
- `bq_feed`: Unified typed events (`events.py`), Recorder Raw/normalized deterministic replay (`replay.py`), Gateway live gRPC client with `enabled_streams` Fail-Fast + `STALE` watchdog (`live.py`), and capability matrix / Recorder-vs-Gateway parity checker (`parity.py`).
- `bq_feature`: Pure causal stateful feature engine (`update(state, event)` + `on_health`) with dual clocks, R1/R7/N5/R9/R6/OFI/Microprice/causal K-lines, and `features.parquet` export.
- `bq_model`: Purged + Embargoed Walk-Forward trainer and `<1ms` inference bundles (`models/<model_id>/`).
- `bq_backtest`: 3-stage `Regime -> Alpha -> MetaCostRisk` + deterministic `btc_spot_bbo_rule`, F1/F2 fill simulators, exchange filters, real `fundingTime` settlement, causality perturbation verification, sensitivity matrix, and immutable `runs/<run_id>/`.
- `bq_mcp`: Local stdio MCP server (6 tools, `<64 KiB` responses, AST + causality + parity guard).
- `bq_signal`: Paper/live signal runner (`deploy.toml`) + two-stage `reconcile_paper` (exact journal replay + independent Recorder UID cross-reconciliation).
