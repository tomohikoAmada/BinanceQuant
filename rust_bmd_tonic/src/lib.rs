use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ahash::AHashSet;
use crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use tokio::task::AbortHandle;
use tonic::transport::Channel;

pub mod bmd {
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("binance_market_data.common.v1");
        }
    }
    pub mod market {
        pub mod v1 {
            tonic::include_proto!("binance_market_data.market.v1");
        }
    }
    pub mod projection {
        pub mod v1 {
            tonic::include_proto!("binance_market_data.projection.v1");
        }
    }
    pub mod gateway {
        pub mod v1 {
            tonic::include_proto!("binance_market_data.gateway.v1");
        }
    }
}

use bmd::{
    common::v1::StreamSelector,
    gateway::v1::{
        binance_market_data_gateway_service_client::BinanceMarketDataGatewayServiceClient,
        gateway_event_envelope::Payload, EventSubscriptionRequest, GatewayStatusRequest,
    },
    market::v1::reference_observation::Payload as RefPayload,
};

const KIND_ACCEPTED: u8 = 1;
const KIND_STATUS: u8 = 2;
const KIND_GAP_CLEAR: u8 = 3;
const KIND_BOOK_TICKER: u8 = 4;
const KIND_AGG_TRADE: u8 = 5;
const KIND_DEPTH_SNAPSHOT: u8 = 6;
const KIND_DEPTH_UPDATE: u8 = 7;
const KIND_MARK_PRICE: u8 = 8;

const MARKET_SPOT: i32 = 1;
const STREAM_DIFF_DEPTH: i32 = 1;

fn now_utc_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Default)]
struct L2SeqState {
    last_uid: Option<u64>,
    gap_active: bool,
}

impl L2SeqState {
    fn on_snapshot(&mut self, last_uid: u64) {
        self.last_uid = Some(last_uid);
        self.gap_active = false;
    }

    /// Returns Ok(true) if update accepted, Ok(false) if stale/suppressed, Err(()) if GAP triggered.
    fn on_diff(&mut self, market: i32, u1: u64, u2: u64, pu: Option<u64>) -> Result<bool, ()> {
        let Some(prev) = self.last_uid else {
            return Ok(false);
        };
        if self.gap_active {
            return Ok(false);
        }
        if u2 <= prev {
            return Ok(false);
        }
        let ok = if market == MARKET_SPOT {
            u1 <= prev + 1 && u2 >= prev + 1
        } else {
            pu == Some(prev) || (pu.is_none() && u1 <= prev + 1)
        };
        if !ok {
            self.gap_active = true;
            self.last_uid = None;
            return Err(());
        }
        self.last_uid = Some(u2);
        Ok(true)
    }

    fn on_gap(&mut self) {
        self.gap_active = true;
        self.last_uid = None;
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct TonicEvent {
    pub kind: u8,
    pub inst_idx: usize,
    pub stream_code: i32,
    pub ts_event: u64,
    pub gateway_recv_ns: u64,
    pub rust_read_ns: u64,
    pub u1: u64,
    pub b_flag: bool,
    pub p1: String,
    pub q1: String,
    pub p2: String,
    pub q2: String,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
    pub reason: String,
}

impl TonicEvent {
    fn blank(kind: u8, inst_idx: usize, stream_code: i32) -> Self {
        Self {
            kind,
            inst_idx,
            stream_code,
            ts_event: 0,
            gateway_recv_ns: 0,
            rust_read_ns: now_utc_ns(),
            u1: 0,
            b_flag: false,
            p1: String::new(),
            q1: String::new(),
            p2: String::new(),
            q2: String::new(),
            bids: Vec::new(),
            asks: Vec::new(),
            reason: String::new(),
        }
    }
}

#[pyclass]
pub struct TonicGatewayClient {
    target: String,
    rt: tokio::runtime::Runtime,
    tx: Sender<TonicEvent>,
    rx: Receiver<TonicEvent>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

#[pymethods]
impl TonicGatewayClient {
    #[new]
    fn new(target: String) -> PyResult<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| PyRuntimeError::new_err(format!("tokio runtime error: {e}")))?;
        let (tx, rx) = unbounded();
        Ok(Self {
            target,
            rt,
            tx,
            rx,
            tasks: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn preflight(
        &self,
        py: Python<'_>,
        strict_capabilities: bool,
        required: Vec<(i32, String, i32)>,
    ) -> PyResult<Vec<(i32, String, Vec<i32>)>> {
        let target = if self.target.starts_with("http://") || self.target.starts_with("https://") {
            self.target.clone()
        } else {
            format!("http://{}", self.target)
        };
        py.allow_threads(|| {
            self.rt.block_on(async move {
                let channel = Channel::from_shared(target)
                    .map_err(|e| PyRuntimeError::new_err(format!("invalid URI: {e}")))?
                    .connect()
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("gRPC connect failed: {e}")))?;
                let mut client = BinanceMarketDataGatewayServiceClient::new(channel);
                let resp = client
                    .get_gateway_status(GatewayStatusRequest {
                        request_id: "bmd-tonic-preflight".to_string(),
                        schema_version: "gateway-status-request.v1".to_string(),
                    })
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("GetGatewayStatus failed: {e}")))?
                    .into_inner();

                let mut caps_map: HashMap<(i32, String), Vec<i32>> = HashMap::new();
                for m in &resp.markets {
                    let streams = m
                        .enabled_streams
                        .as_ref()
                        .map(|es| es.streams.clone())
                        .unwrap_or_default();
                    caps_map.insert((m.market, m.symbol.clone()), streams);
                }
                for (mkt, sym, stream_code) in &required {
                    let Some(streams) = caps_map.get(&(*mkt, sym.clone())) else {
                        return Err(PyRuntimeError::new_err(format!(
                            "CapabilityMismatch: product ({mkt}, {sym}) missing from Gateway"
                        )));
                    };
                    if strict_capabilities && !streams.contains(stream_code) {
                        return Err(PyRuntimeError::new_err(format!(
                            "CapabilityMismatch: stream {stream_code} disabled for ({mkt}, {sym})"
                        )));
                    }
                }
                Ok(caps_map
                    .into_iter()
                    .map(|((mkt, sym), s)| (mkt, sym, s))
                    .collect())
            })
        })
    }

    fn subscribe_stream(
        &self,
        inst_idx: usize,
        market: i32,
        symbol: String,
        stream_code: i32,
        schemas: Vec<String>,
    ) -> PyResult<()> {
        let target = if self.target.starts_with("http://") || self.target.starts_with("https://") {
            self.target.clone()
        } else {
            format!("http://{}", self.target)
        };
        let tx = self.tx.clone();
        let req = EventSubscriptionRequest {
            request_id: format!("bmd-tonic-{market}-{symbol}-{stream_code}"),
            schema_version: "event-subscription-request.v1".to_string(),
            selectors: vec![StreamSelector {
                venue: 1,
                market,
                symbol: symbol.clone(),
                stream: stream_code,
            }],
            delivery_mode: 1, // CONTIGUOUS_EVENTS
            supported_payload_schema_versions: schemas,
        };
        let handle = self.rt.spawn(async move {
            let Ok(channel) = Channel::from_shared(target).unwrap().connect().await else {
                let mut ev = TonicEvent::blank(KIND_STATUS, inst_idx, stream_code);
                ev.b_flag = false;
                ev.reason = "DEGRADED:connect_failed".to_string();
                let _ = tx.send(ev);
                return;
            };
            let mut client = BinanceMarketDataGatewayServiceClient::new(channel);
            let Ok(resp) = client.subscribe_events(req).await else {
                let mut ev = TonicEvent::blank(KIND_STATUS, inst_idx, stream_code);
                ev.b_flag = false;
                ev.reason = "DEGRADED:subscribe_failed".to_string();
                let _ = tx.send(ev);
                return;
            };
            let mut stream = resp.into_inner();
            let mut dedup: AHashSet<u64> = AHashSet::with_capacity(4096);
            let mut l2 = L2SeqState::default();

            while let Ok(Some(env)) = stream.message().await {
                let read_ns = now_utc_ns();
                let Some(payload) = env.payload else {
                    continue;
                };
                match payload {
                    Payload::SubscriptionAccepted(_) => {
                        let mut ev = TonicEvent::blank(KIND_ACCEPTED, inst_idx, stream_code);
                        ev.rust_read_ns = read_ns;
                        let _ = tx.send(ev);
                    }
                    Payload::ConsumerGap(gap) => {
                        if stream_code == STREAM_DIFF_DEPTH {
                            l2.on_gap();
                            let mut clr = TonicEvent::blank(KIND_GAP_CLEAR, inst_idx, stream_code);
                            clr.ts_event = gap.detected_time_utc_ns;
                            clr.rust_read_ns = read_ns;
                            let _ = tx.send(clr);
                        }
                        let mut ev = TonicEvent::blank(KIND_STATUS, inst_idx, stream_code);
                        ev.b_flag = false;
                        ev.ts_event = gap.detected_time_utc_ns;
                        ev.rust_read_ns = read_ns;
                        ev.reason = format!("GAP:{}", gap.reason);
                        let _ = tx.send(ev);
                    }
                    Payload::StreamStatus(st) => {
                        let healthy = st.state == 3; // STREAM_LIFECYCLE_STATE_LIVE
                        if !healthy && stream_code == STREAM_DIFF_DEPTH {
                            l2.on_gap();
                            let mut clr = TonicEvent::blank(KIND_GAP_CLEAR, inst_idx, stream_code);
                            clr.ts_event = st.observed_time_utc_ns;
                            clr.rust_read_ns = read_ns;
                            let _ = tx.send(clr);
                        }
                        let mut ev = TonicEvent::blank(KIND_STATUS, inst_idx, stream_code);
                        ev.b_flag = healthy;
                        ev.ts_event = st.observed_time_utc_ns;
                        ev.rust_read_ns = read_ns;
                        ev.reason = st.message.unwrap_or_else(|| format!("state:{}", st.state));
                        let _ = tx.send(ev);
                    }
                    Payload::BookTicker(bt) => {
                        if let Some(meta) = bt.metadata {
                            let recv_ns = meta.receive_time_utc_ns.unwrap_or(read_ns);
                            let ts_ev = meta
                                .exchange_event_time_ms
                                .map(|ms| (ms as u64) * 1_000_000)
                                .unwrap_or(recv_ns);
                            let mut ev = TonicEvent::blank(KIND_BOOK_TICKER, inst_idx, stream_code);
                            ev.ts_event = ts_ev;
                            ev.gateway_recv_ns = recv_ns;
                            ev.rust_read_ns = read_ns;
                            ev.u1 = bt.update_id.unwrap_or(0);
                            ev.p1 = bt.best_bid_price;
                            ev.q1 = bt.best_bid_quantity;
                            ev.p2 = bt.best_ask_price;
                            ev.q2 = bt.best_ask_quantity;
                            let _ = tx.send(ev);
                        }
                    }
                    Payload::AggTrade(at) => {
                        let agg_id = at.aggregate_trade_id;
                        if !dedup.insert(agg_id) {
                            continue;
                        }
                        if dedup.len() > 250_000 {
                            dedup.clear();
                            dedup.insert(agg_id);
                        }
                        let recv_ns = at
                            .metadata
                            .as_ref()
                            .and_then(|m| m.receive_time_utc_ns)
                            .unwrap_or(read_ns);
                        let mut ev = TonicEvent::blank(KIND_AGG_TRADE, inst_idx, stream_code);
                        ev.ts_event = (at.trade_time_ms as u64) * 1_000_000;
                        ev.gateway_recv_ns = recv_ns;
                        ev.rust_read_ns = read_ns;
                        ev.u1 = agg_id;
                        ev.b_flag = at.buyer_is_maker;
                        ev.p1 = at.price;
                        ev.q1 = at.quantity;
                        let _ = tx.send(ev);
                    }
                    Payload::ReferenceObservation(ro) => {
                        if let Some(RefPayload::DepthBaseline(snap)) = ro.payload {
                            l2.on_snapshot(snap.last_update_id);
                            let recv_ns = snap.receive_time_utc_ns.unwrap_or(read_ns);
                            let ts_ev = snap
                                .exchange_event_time_ms
                                .map(|ms| (ms as u64) * 1_000_000)
                                .unwrap_or(recv_ns);
                            let mut ev =
                                TonicEvent::blank(KIND_DEPTH_SNAPSHOT, inst_idx, stream_code);
                            ev.ts_event = ts_ev;
                            ev.gateway_recv_ns = recv_ns;
                            ev.rust_read_ns = read_ns;
                            ev.u1 = snap.last_update_id;
                            ev.bids = snap
                                .bids
                                .into_iter()
                                .map(|l| (l.price, l.quantity))
                                .collect();
                            ev.asks = snap
                                .asks
                                .into_iter()
                                .map(|l| (l.price, l.quantity))
                                .collect();
                            let _ = tx.send(ev);
                        }
                    }
                    Payload::DepthUpdate(du) => {
                        let recv_ns = du
                            .metadata
                            .as_ref()
                            .and_then(|m| m.receive_time_utc_ns)
                            .unwrap_or(read_ns);
                        let ts_ev = du
                            .metadata
                            .as_ref()
                            .and_then(|m| m.exchange_event_time_ms)
                            .map(|ms| (ms as u64) * 1_000_000)
                            .unwrap_or(recv_ns);
                        match l2.on_diff(
                            market,
                            du.first_update_id,
                            du.final_update_id,
                            du.previous_final_update_id,
                        ) {
                            Ok(true) => {
                                let mut ev =
                                    TonicEvent::blank(KIND_DEPTH_UPDATE, inst_idx, stream_code);
                                ev.ts_event = ts_ev;
                                ev.gateway_recv_ns = recv_ns;
                                ev.rust_read_ns = read_ns;
                                ev.u1 = du.final_update_id;
                                ev.bids = du
                                    .bids
                                    .into_iter()
                                    .map(|l| (l.price, l.quantity))
                                    .collect();
                                ev.asks = du
                                    .asks
                                    .into_iter()
                                    .map(|l| (l.price, l.quantity))
                                    .collect();
                                let _ = tx.send(ev);
                            }
                            Ok(false) => {}
                            Err(()) => {
                                let mut clr =
                                    TonicEvent::blank(KIND_GAP_CLEAR, inst_idx, stream_code);
                                clr.ts_event = recv_ns;
                                clr.gateway_recv_ns = recv_ns;
                                clr.rust_read_ns = read_ns;
                                let _ = tx.send(clr);
                                let mut halt =
                                    TonicEvent::blank(KIND_STATUS, inst_idx, stream_code);
                                halt.b_flag = false;
                                halt.ts_event = recv_ns;
                                halt.gateway_recv_ns = recv_ns;
                                halt.rust_read_ns = read_ns;
                                halt.reason = "GAP:l2_sequence_gap".to_string();
                                let _ = tx.send(halt);
                            }
                        }
                    }
                    Payload::MarkPrice(mp) => {
                        let recv_ns = mp
                            .metadata
                            .as_ref()
                            .and_then(|m| m.receive_time_utc_ns)
                            .unwrap_or(read_ns);
                        let ts_ev = mp
                            .metadata
                            .as_ref()
                            .and_then(|m| m.exchange_event_time_ms)
                            .map(|ms| (ms as u64) * 1_000_000)
                            .unwrap_or(recv_ns);
                        let mut ev = TonicEvent::blank(KIND_MARK_PRICE, inst_idx, stream_code);
                        ev.ts_event = ts_ev;
                        ev.gateway_recv_ns = recv_ns;
                        ev.rust_read_ns = read_ns;
                        ev.u1 = (mp.next_funding_time_ms as u64) * 1_000_000;
                        ev.p1 = mp.mark_price;
                        ev.q1 = mp.funding_rate;
                        let _ = tx.send(ev);
                    }
                    Payload::Liquidation(_) => {}
                }
            }
        });
        self.tasks.lock().unwrap().push(handle.abort_handle());
        Ok(())
    }

    fn drain_batch(
        &self,
        py: Python<'_>,
        max_items: usize,
        wait_ms: u64,
    ) -> PyResult<Vec<TonicEvent>> {
        py.allow_threads(|| {
            let mut out = Vec::with_capacity(max_items.min(512));
            match self.rx.recv_timeout(Duration::from_millis(wait_ms)) {
                Ok(first) => out.push(first),
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {
                    return Ok(out);
                }
            }
            while out.len() < max_items {
                match self.rx.try_recv() {
                    Ok(ev) => out.push(ev),
                    Err(_) => break,
                }
            }
            Ok(out)
        })
    }

    fn stop(&self) {
        let mut guard = self.tasks.lock().unwrap();
        for h in guard.drain(..) {
            h.abort();
        }
    }
}

#[pymodule]
fn bmd_tonic_ext(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TonicEvent>()?;
    m.add_class::<TonicGatewayClient>()?;
    Ok(())
}
