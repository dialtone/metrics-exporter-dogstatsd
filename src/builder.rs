use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use std::thread;

use indexmap::IndexMap;
use parking_lot::RwLock;

use metrics_util::{
    parse_quantiles,
    registry::{GenerationalStorage, Recency, Registry},
    MetricKindMask, Quantile,
};

use crate::common::Matcher;
use crate::distribution::DistributionBuilder;
use crate::recorder::{Inner, StatsdRecorder};
use crate::registry::AtomicStorage;
use crate::{common::BuildError, StatsdHandle};

use quanta::Clock;
use tokio::net::UdpSocket;
use tokio::runtime;
use tracing::error;

use std::net::SocketAddr;

// type ExporterFuture = Pin<Box<dyn Future<Output = Result<(), hyper::Error>> + Send + 'static>>;
type ExporterFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;

#[derive(Clone)]
enum ExporterConfig {
    PushGateway {
        endpoint: SocketAddr,
        interval: Duration,
    },

    #[allow(dead_code)]
    Unconfigured,
}

impl ExporterConfig {
    fn as_type_str(&self) -> &'static str {
        match self {
            Self::PushGateway { .. } => "push-gateway",
            Self::Unconfigured => "unconfigured,",
        }
    }
}

pub struct StatsdBuilder {
    exporter_config: ExporterConfig,
    quantiles: Vec<Quantile>,
    buckets: Option<Vec<f64>>,
    bucket_overrides: Option<HashMap<Matcher, Vec<f64>>>,
    idle_timeout: Option<Duration>,
    recency_mask: MetricKindMask,
    prefix: Option<String>,
    global_tags: Option<IndexMap<String, String>>,
}

impl StatsdBuilder {
    pub fn new() -> Self {
        let quantiles = parse_quantiles(&[0.0, 0.5, 0.9, 0.95, 0.99, 0.999, 1.0]);
        let exporter_config = ExporterConfig::Unconfigured;
        Self {
            exporter_config,
            quantiles,
            buckets: None,
            bucket_overrides: None,
            idle_timeout: None,
            recency_mask: MetricKindMask::NONE,
            prefix: None,
            global_tags: None,
        }
    }

    pub fn with_push_gateway<T>(
        mut self,
        endpoint: T,
        interval: Duration,
    ) -> Result<Self, BuildError>
    where
        T: AsRef<str>,
    {
        self.exporter_config = ExporterConfig::PushGateway {
            endpoint: endpoint
                .as_ref()
                .parse()
                .map_err(|e: std::net::AddrParseError| {
                    BuildError::InvalidPushGatewayEndpoint(e.to_string())
                })?,
            interval,
        };

        Ok(self)
    }

    pub fn set_quantiles(mut self, quantiles: &[f64]) -> Result<Self, BuildError> {
        if quantiles.is_empty() {
            return Err(BuildError::EmptyBucketsOrQuantiles);
        }

        self.quantiles = parse_quantiles(quantiles);
        Ok(self)
    }

    pub fn set_buckets(mut self, values: &[f64]) -> Result<Self, BuildError> {
        if values.is_empty() {
            return Err(BuildError::EmptyBucketsOrQuantiles);
        }

        self.buckets = Some(values.to_vec());
        Ok(self)
    }

    pub fn set_buckets_for_metric(
        mut self,
        matcher: Matcher,
        values: &[f64],
    ) -> Result<Self, BuildError> {
        if values.is_empty() {
            return Err(BuildError::EmptyBucketsOrQuantiles);
        }

        let buckets = self.bucket_overrides.get_or_insert_with(HashMap::new);
        buckets.insert(matcher.sanitized(), values.to_vec());
        Ok(self)
    }

    #[must_use]
    pub fn add_global_tags<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        let tags = self.global_tags.get_or_insert_with(IndexMap::new);
        tags.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn set_global_prefix<P>(mut self, prefix: P) -> Self
    where
        P: Into<String>,
    {
        self.prefix = Some(prefix.into());
        self
    }

    #[must_use]
    pub fn idle_timeout(mut self, mask: MetricKindMask, timeout: Option<Duration>) -> Self {
        self.idle_timeout = timeout;
        self.recency_mask = if self.idle_timeout.is_none() {
            MetricKindMask::NONE
        } else {
            mask
        };
        self
    }

    pub fn install(self) -> Result<(), BuildError> {
        let recorder = if let Ok(handle) = runtime::Handle::try_current() {
            let (recorder, exporter) = {
                let _g = handle.enter();
                self.build()?
            };

            handle.spawn(exporter);

            recorder
        } else {
            let thread_name = format!(
                "metrics-exporter-statsd-{}",
                self.exporter_config.as_type_str()
            );

            let runtime = runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| BuildError::FailedToCreateRuntime(e.to_string()))?;

            let (recorder, exporter) = {
                let _g = runtime.enter();
                self.build()?
            };

            thread::Builder::new()
                .name(thread_name)
                .spawn(move || runtime.block_on(exporter))
                .map_err(|e| BuildError::FailedToCreateRuntime(e.to_string()))?;

            recorder
        };

        metrics::set_boxed_recorder(Box::new(recorder))?;

        Ok(())
    }

    pub fn build(self) -> Result<(StatsdRecorder, ExporterFuture), BuildError> {
        let exporter_config = self.exporter_config.clone();
        let recorder = self.build_recorder();
        let handle = recorder.handle();

        match exporter_config {
            ExporterConfig::Unconfigured => Err(BuildError::MissingExporterConfiguration),
            ExporterConfig::PushGateway { endpoint, interval } => {
                let exporter = async move {
                    let client = UdpSocket::bind("0.0.0.0:0").await?;

                    loop {
                        // Sleep for `interval` amount of time, and then do a push.
                        tokio::time::sleep(interval).await;

                        let output = handle.render();
                        match send_all(&client, output, &endpoint).await {
                            Ok(_) => (),
                            Err(e) => error!("error sending request to push gateway: {:?}", e),
                        }
                    }
                };

                Ok((recorder, Box::pin(exporter)))
            }
        }
    }

    pub fn build_recorder(self) -> StatsdRecorder {
        self.build_with_clock(Clock::new())
    }

    pub(crate) fn build_with_clock(self, clock: Clock) -> StatsdRecorder {
        let inner = Inner {
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(clock, self.recency_mask, self.idle_timeout),
            distributions: RwLock::new(HashMap::new()),
            distribution_builder: DistributionBuilder::new(
                self.quantiles,
                self.buckets,
                self.bucket_overrides,
            ),
            descriptions: RwLock::new(HashMap::new()),
            global_tags: self.global_tags.unwrap_or_default(),
        };

        StatsdRecorder::from(inner)
    }
}

impl Default for StatsdBuilder {
    fn default() -> Self {
        StatsdBuilder::new()
    }
}

async fn send_all(client: &UdpSocket, body: String, endpoint: &SocketAddr) -> io::Result<()> {
    let buf = body.as_bytes();
    let mut sent = 0;
    while sent < buf.len() {
        match client.send_to(&buf[sent..], endpoint).await {
            Ok(nsent) => {
                sent += nsent;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Matcher, StatsdBuilder};
    use metrics::{Key, Recorder};

    #[test]
    fn test_render() {
        let recorder = StatsdBuilder::new()
            .set_quantiles(&[0.0, 1.0])
            .unwrap()
            .build_recorder();

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected_counter = "basic.counter:42|c\n\n";
        assert_eq!(rendered, expected_counter);
    }
}
