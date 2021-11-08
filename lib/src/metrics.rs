use {
    log::*,
    std::collections::HashMap,
    std::sync::{atomic, Arc, Mutex, RwLock},
    tokio::time,
};

#[derive(Debug)]
enum Value {
    Counter(Arc<atomic::AtomicI64>),
    RateCounter(Arc<atomic::AtomicU64>),
    Tag(Arc<Mutex<String>>),
}

#[derive(Clone)]
pub struct MetricCounter {
    value: Arc<atomic::AtomicI64>,
}
impl MetricCounter {
    pub fn set(&mut self, value: i64) {
        self.value.store(value, atomic::Ordering::Release);
    }

    pub fn increment(&mut self) {
        self.value.fetch_add(1, atomic::Ordering::AcqRel);
    }

    pub fn decrement(&mut self) {
        self.value.fetch_sub(1, atomic::Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct MetricRateCounter {
    value: Arc<atomic::AtomicU64>,
}
impl MetricRateCounter {
    pub fn set(&mut self, value: u64) {
        self.value.store(value, atomic::Ordering::Release);
    }

    pub fn increment(&mut self) {
        self.value.fetch_add(1, atomic::Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub struct MetricTag {
    value: Arc<Mutex<String>>,
}

impl MetricTag {
    pub fn set(&self, value: String) {
        *self.value.lock().unwrap() = value;
    }
}

#[derive(Clone)]
pub struct Metrics {
    registry: Arc<RwLock<HashMap<String, Value>>>,
}

impl Metrics {
    pub fn register_counter(&self, name: String) -> MetricCounter {
        let value = Arc::new(atomic::AtomicI64::new(0));
        self.registry
            .write()
            .unwrap()
            .insert(name, Value::Counter(value.clone()));
        MetricCounter { value }
    }

    pub fn register_rate_counter(&self, name: String) -> MetricRateCounter {
        let value = Arc::new(atomic::AtomicU64::new(0));
        self.registry
            .write()
            .unwrap()
            .insert(name, Value::RateCounter(value.clone()));
        MetricRateCounter { value }
    }

    pub fn register_tag(&self, name: String) -> MetricTag {
        let value = Arc::new(Mutex::new(String::new()));
        self.registry
            .write()
            .unwrap()
            .insert(name, Value::Tag(value.clone()));
        MetricTag { value }
    }
}

pub fn start() -> Metrics {
    let mut write_interval = time::interval(time::Duration::from_secs(60));

    let registry = Arc::new(RwLock::new(HashMap::<String, Value>::new()));
    let registry_c = Arc::clone(&registry);

    tokio::spawn(async move {
        loop {
            write_interval.tick().await;

            // Nested locking! Safe because the only other user locks registry for writing and doesn't
            // acquire any interior locks.
            let metrics = registry_c.read().unwrap();
            for (name, value) in metrics.iter() {
                match value {
                    Value::Counter(v) => {
                        info!("metric: {}: {}", name, v.load(atomic::Ordering::Acquire))
                    }
                    Value::RateCounter(v) => {
                        info!("metric: {}: {}", name, v.load(atomic::Ordering::Acquire))
                    }
                    Value::Tag(v) => info!("metric: {}: {}", name, &*v.lock().unwrap()),
                }
            }
        }
    });

    Metrics { registry }
}
