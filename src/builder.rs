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

use std::net::{AddrParseError, SocketAddr};

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
            endpoint: endpoint.as_ref().parse().map_err(|e: AddrParseError| {
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
    pub fn add_global_tag<K, V>(mut self, key: K, value: V) -> Self
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
            prefix: self.prefix.clone(),
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(clock, self.recency_mask, self.idle_timeout),
            distributions: RwLock::new(HashMap::new()),
            distribution_builder: DistributionBuilder::new(
                self.quantiles,
                self.buckets,
                self.bucket_overrides,
            ),
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
    use metrics::{Key, KeyName, Label, Recorder};
    use metrics_util::MetricKindMask;
    use quanta::Clock;
    use std::time::Duration;

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

        let labels = vec![Label::new("wutang", "forever")];
        let key = Key::from_parts("basic.gauge", labels);
        let gauge1 = recorder.register_gauge(&key);
        gauge1.set(-3.44);
        let rendered = handle.render();
        let expected_gauge = format!(
            "{}basic.gauge:-3.44|c|#wutang:forever\n\n",
            expected_counter
        );
        assert_eq!(rendered, expected_gauge);

        let key = Key::from_name("basic.histogram");
        let histogram1 = recorder.register_histogram(&key);
        histogram1.record(12.0);
        let rendered = handle.render();

        let histogram_data = concat!(
            "basic.histogram.0:12|c\n",
            "basic.histogram.1:12|c\n",
            "basic.histogram.sum:12|c\n",
            "basic.histogram.count:1|c\n",
            "\n"
        );
        let expected_histogram = format!("{}{}", expected_gauge, histogram_data);
        assert_eq!(rendered, expected_histogram);
    }

    #[test]
    fn test_buckets() {
        const DEFAULT_VALUES: [f64; 3] = [10.0, 100.0, 1000.0];
        const PREFIX_VALUES: [f64; 3] = [15.0, 105.0, 1005.0];
        const SUFFIX_VALUES: [f64; 3] = [20.0, 110.0, 1010.0];
        const FULL_VALUES: [f64; 3] = [25.0, 115.0, 1015.0];

        let recorder = StatsdBuilder::new()
            .set_buckets_for_metric(
                Matcher::Full("metrics.testing foo".to_owned()),
                &FULL_VALUES[..],
            )
            .expect("bounds should not be empty")
            .set_buckets_for_metric(
                Matcher::Prefix("metrics.testing".to_owned()),
                &PREFIX_VALUES[..],
            )
            .expect("bounds should not be empty")
            .set_buckets_for_metric(Matcher::Suffix("foo".to_owned()), &SUFFIX_VALUES[..])
            .expect("bounds should not be empty")
            .set_buckets(&DEFAULT_VALUES[..])
            .expect("bounds should not be empty")
            .build_recorder();

        let full_key = Key::from_name("metrics.testing_foo");
        let full_key_histo = recorder.register_histogram(&full_key);
        full_key_histo.record(FULL_VALUES[0]);

        let prefix_key = Key::from_name("metrics.testing_bar");
        let prefix_key_histo = recorder.register_histogram(&prefix_key);
        prefix_key_histo.record(PREFIX_VALUES[1]);

        let suffix_key = Key::from_name("metrics.testin_foo");
        let suffix_key_histo = recorder.register_histogram(&suffix_key);
        suffix_key_histo.record(SUFFIX_VALUES[2]);

        let default_key = Key::from_name("metrics.wee");
        let default_key_histo = recorder.register_histogram(&default_key);
        default_key_histo.record(DEFAULT_VALUES[2] + 1.0);

        let full_data = concat!(
            "metrics.testing_foo.25:1|c\n",
            "metrics.testing_foo.115:1|c\n",
            "metrics.testing_foo.1015:1|c\n",
            "metrics.testing_foo._Inf:1|c\n",
            "metrics.testing_foo.sum:25|c\n",
            "metrics.testing_foo.count:1|c\n",
        );

        let prefix_data = concat!(
            "metrics.testing_bar.15:0|c\n",
            "metrics.testing_bar.105:1|c\n",
            "metrics.testing_bar.1005:1|c\n",
            "metrics.testing_bar._Inf:1|c\n",
            "metrics.testing_bar.sum:105|c\n",
            "metrics.testing_bar.count:1|c\n",
        );

        let suffix_data = concat!(
            "metrics.testin_foo.20:0|c\n",
            "metrics.testin_foo.110:0|c\n",
            "metrics.testin_foo.1010:1|c\n",
            "metrics.testin_foo._Inf:1|c\n",
            "metrics.testin_foo.sum:1010|c\n",
            "metrics.testin_foo.count:1|c\n",
        );

        let default_data = concat!(
            "metrics.wee.10:0|c\n",
            "metrics.wee.100:0|c\n",
            "metrics.wee.1000:0|c\n",
            "metrics.wee._Inf:1|c\n",
            "metrics.wee.sum:1001|c\n",
            "metrics.wee.count:1|c\n",
        );

        let handle = recorder.handle();
        let rendered = handle.render();

        assert!(rendered.contains(full_data));
        assert!(rendered.contains(prefix_data));
        assert!(rendered.contains(suffix_data));
        assert!(rendered.contains(default_data));
    }

    #[test]
    fn test_idle_timeout_all() {
        let (clock, mock) = Clock::mock();

        let recorder = StatsdBuilder::new()
            .idle_timeout(MetricKindMask::ALL, Some(Duration::from_secs(10)))
            .set_quantiles(&[0.0, 1.0])
            .unwrap()
            .build_with_clock(clock);

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let key = Key::from_name("basic.gauge");
        let gauge1 = recorder.register_gauge(&key);
        gauge1.set(-3.44);

        let key = Key::from_name("basic.histogram");
        let histo1 = recorder.register_histogram(&key);
        histo1.record(1.0);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected = concat!(
            "basic.counter:42|c\n\n",
            "basic.gauge:-3.44|c\n\n",
            "basic.histogram.0:1|c\n",
            "basic.histogram.1:1|c\n",
            "basic.histogram.sum:1|c\n",
            "basic.histogram.count:1|c\n\n",
        );

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, "");
    }

    #[test]
    fn test_idle_timeout_partial() {
        let (clock, mock) = Clock::mock();

        let recorder = StatsdBuilder::new()
            .idle_timeout(
                MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM,
                Some(Duration::from_secs(10)),
            )
            .set_quantiles(&[0.0, 1.0])
            .unwrap()
            .build_with_clock(clock);

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let key = Key::from_name("basic.gauge");
        let gauge1 = recorder.register_gauge(&key);
        gauge1.set(-3.44);

        let key = Key::from_name("basic.histogram");
        let histo1 = recorder.register_histogram(&key);
        histo1.record(1.0);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected = concat!(
            "basic.counter:42|c\n\n",
            "basic.gauge:-3.44|c\n\n",
            "basic.histogram.0:1|c\n",
            "basic.histogram.1:1|c\n",
            "basic.histogram.sum:1|c\n",
            "basic.histogram.count:1|c\n\n",
        );

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();

        let expected = "basic.gauge:-3.44|c\n\n";
        assert_eq!(rendered, expected);
    }

    #[test]
    fn test_idle_timeout_staggered_distributions() {
        let (clock, mock) = Clock::mock();

        let recorder = StatsdBuilder::new()
            .idle_timeout(MetricKindMask::ALL, Some(Duration::from_secs(10)))
            .set_quantiles(&[0.0, 1.0])
            .unwrap()
            .build_with_clock(clock);

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let key = Key::from_name("basic.gauge");
        let gauge1 = recorder.register_gauge(&key);
        gauge1.set(-3.44);

        let key = Key::from_name("basic.histogram");
        let histo1 = recorder.register_histogram(&key);
        histo1.record(1.0);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected = concat!(
            "basic.counter:42|c\n\n",
            "basic.gauge:-3.44|c\n\n",
            "basic.histogram.0:1|c\n",
            "basic.histogram.1:1|c\n",
            "basic.histogram.sum:1|c\n",
            "basic.histogram.count:1|c\n\n",
        );

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        let key = Key::from_parts("basic.histogram", vec![Label::new("type", "special")]);
        let histo2 = recorder.register_histogram(&key);
        histo2.record(2.0);

        let expected_second = concat!(
            "basic.counter:42|c\n\n",
            "basic.gauge:-3.44|c\n\n",
            "basic.histogram.0:1|c\n",
            "basic.histogram.1:1|c\n",
            "basic.histogram.sum:1|c\n",
            "basic.histogram.count:1|c\n",
            "basic.histogram.0:2|c|#type:special\n",
            "basic.histogram.1:2|c|#type:special\n",
            "basic.histogram.sum:2|c|#type:special\n",
            "basic.histogram.count:1|c|#type:special\n\n",
        );
        let rendered = handle.render();
        assert_eq!(rendered, expected_second);

        let expected_after = concat!(
            "basic.histogram.0:2|c|#type:special\n",
            "basic.histogram.1:2|c|#type:special\n",
            "basic.histogram.sum:2|c|#type:special\n",
            "basic.histogram.count:1|c|#type:special\n\n",
        );

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, expected_after);
    }

    #[test]
    fn test_idle_timeout_doesnt_remove_recents() {
        let (clock, mock) = Clock::mock();

        let recorder = StatsdBuilder::new()
            .idle_timeout(MetricKindMask::ALL, Some(Duration::from_secs(10)))
            .build_with_clock(clock);

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let key = Key::from_name("basic.gauge");
        let gauge1 = recorder.register_gauge(&key);
        gauge1.set(-3.44);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected = concat!("basic.counter:42|c\n\n", "basic.gauge:-3.44|c\n\n",);

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        let expected_second = concat!("basic.counter:42|c\n\n", "basic.gauge:-3.44|c\n\n",);
        let rendered = handle.render();
        assert_eq!(rendered, expected_second);

        counter1.increment(1);

        let expected_after = concat!("basic.counter:43|c\n\n",);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, expected_after);
    }

    #[test]
    fn test_idle_timeout_catches_delayed_idle() {
        let (clock, mock) = Clock::mock();

        let recorder = StatsdBuilder::new()
            .idle_timeout(MetricKindMask::ALL, Some(Duration::from_secs(10)))
            .build_with_clock(clock);

        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        // First render, which starts tracking the counter in the recency state.
        let handle = recorder.handle();
        let rendered = handle.render();
        let expected = concat!("basic.counter:42|c\n\n",);

        assert_eq!(rendered, expected);

        // Now go forward by 9 seconds, which is close but still right unfer the idle timeout.
        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        // Now increment the counter and advance time by two seconds: this pushes it over the idle
        // timeout threshold, but it should not be removed since it has been updated.
        counter1.increment(1);

        let expected_after = concat!("basic.counter:43|c\n\n",);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, expected_after);

        // Now advance by 11 seconds, right past the idle timeout threshold.  We've made no further
        // updates to the counter so it should be properly removed this time.
        mock.increment(Duration::from_secs(11));
        let rendered = handle.render();
        assert_eq!(rendered, "");
    }

    #[test]
    pub fn test_global_labels() {
        let recorder = StatsdBuilder::new()
            .add_global_tag("foo", "foo")
            .add_global_tag("foo", "bar")
            .build_recorder();
        let key = Key::from_name("basic.counter");
        let counter1 = recorder.register_counter(&key);
        counter1.increment(42);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected_counter = "basic.counter:42|c|#foo:bar\n\n";

        assert_eq!(rendered, expected_counter);
    }

    #[test]
    pub fn test_global_labels_overrides() {
        let recorder = StatsdBuilder::new()
            .add_global_tag("foo", "foo")
            .build_recorder();

        let key =
            Key::from_name("overridden").with_extra_labels(vec![Label::new("foo", "overridden")]);
        let counter1 = recorder.register_counter(&key);
        counter1.increment(1);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected_counter = "overridden:1|c|#foo:overridden\n\n";

        assert_eq!(rendered, expected_counter);
    }

    #[test]
    pub fn test_sanitized_render() {
        let recorder = StatsdBuilder::new()
            .add_global_tag("foo:", "foo")
            .build_recorder();

        let key_name = KeyName::from("yee_haw:lets go");
        let key = Key::from_name(key_name.clone())
            .with_extra_labels(vec![Label::new("øhno", "\"yeet\nies\\\"")]);
        recorder.describe_counter(key_name, None, "\"Simplë stuff.\nRëally.\"".into());
        let counter1 = recorder.register_counter(&key);
        counter1.increment(1);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected_counter = "yee_haw_lets_go:1|c|#foo_:foo,_hno:_yeet_ies__\n\n";

        assert_eq!(rendered, expected_counter);
    }
}
