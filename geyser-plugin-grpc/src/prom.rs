use {
    crate::version::VERSION as VERSION_INFO,
    futures_util::FutureExt,
    hyper::{
        server::conn::AddrStream,
        service::{make_service_fn, service_fn},
        Body, Request, Response, Server, StatusCode,
    },
    log::*,
    prometheus::{IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder},
    serde::Deserialize,
    std::{net::SocketAddr, sync::Once},
    tokio::{runtime::Runtime, sync::oneshot},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["key", "value"]
    ).unwrap();

    pub static ref SLOTS_LAST_PROCESSED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("slots_last_processed", "Last processed slot by plugin in send loop"),
        &["status"]
    ).unwrap();

    pub static ref BROADCAST_SLOTS_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("broadcast_slots_total", "Total number of broadcasted slot messages"),
        &["status"]
    ).unwrap();

    pub static ref BROADCAST_ACCOUNTS_TOTAL: IntCounter = IntCounter::new(
        "broadcast_slots_total", "Total number of broadcasted slot messages",
    ).unwrap();
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrometheusConfig {
    /// Address of Prometheus service.
    pub address: SocketAddr,
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown_signal: oneshot::Sender<()>,
}

impl PrometheusService {
    pub fn new(runtime: &Runtime, config: Option<PrometheusConfig>) -> Self {
        static REGISTER: Once = Once::new();
        REGISTER.call_once(|| {
            macro_rules! register {
                ($collector:ident) => {
                    REGISTRY
                        .register(Box::new($collector.clone()))
                        .expect("collector can't be registered");
                };
            }
            register!(VERSION);
            register!(SLOTS_LAST_PROCESSED);
            register!(BROADCAST_SLOTS_TOTAL);
            register!(BROADCAST_ACCOUNTS_TOTAL);

            for (key, value) in &[
                ("version", VERSION_INFO.version),
                ("solana", VERSION_INFO.solana),
                ("git", VERSION_INFO.git),
                ("rustc", VERSION_INFO.rustc),
                ("buildts", VERSION_INFO.buildts),
            ] {
                VERSION.with_label_values(&[key, value]).inc()
            }
        });

        let (tx, rx) = oneshot::channel();
        if let Some(PrometheusConfig { address }) = config {
            runtime.spawn(async move {
                let make_service = make_service_fn(move |_: &AddrStream| async move {
                    Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| async move {
                        let response = match req.uri().path() {
                            "/metrics" => metrics_handler(),
                            _ => not_found_handler(),
                        };
                        Ok::<_, hyper::Error>(response)
                    }))
                });
                let server = Server::bind(&address).serve(make_service);
                if let Err(error) = tokio::try_join!(server, rx.map(|_| Ok(()))) {
                    error!("prometheus service failed: {}", error);
                }
            });
        }

        PrometheusService {
            shutdown_signal: tx,
        }
    }

    pub fn shutdown(self) {
        let _ = self.shutdown_signal.send(());
    }
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder().body(Body::from(metrics)).unwrap()
}

fn not_found_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}
