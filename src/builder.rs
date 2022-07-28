use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::time::Duration;

use std::thread;

use indexmap::IndexMap;

use metrics_util::{
    parse_quantiles,
    registry::{GenerationalStorage, Recency, Registry},
    MetricKindMask, Quantile,
};

use crate::common::BuildError;
use crate::common::Matcher;
use crate::distribution::DistributionBuilder;
use crate::recorder::{Inner, StatsdRecorder};
use crate::registry::AtomicStorage;

use quanta::Clock;
use tokio::net::UdpSocket;
use tokio::runtime;
use tracing::error;

use std::net::{AddrParseError, SocketAddr};

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

/// Builder for creating and installing a Statsd recorder/exporter.
pub struct StatsdBuilder {
    exporter_config: ExporterConfig,
    quantiles: Vec<Quantile>,
    buckets: Option<Vec<f64>>,
    bucket_overrides: Option<HashMap<Matcher, Vec<f64>>>,
    idle_timeout: Option<Duration>,
    recency_mask: MetricKindMask,
    prefix: Option<String>,
    global_tags: Option<IndexMap<String, String>>,
    max_packet_size: usize,
}

impl StatsdBuilder {
    /// Creates a new [`StatsdBuilder`].
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
            max_packet_size: 1432,
        }
    }

    /// Configures the exporter to push periodic requests to a statsd agent
    ///
    /// ## Errors
    ///
    /// If the given endpoint cannot be parsed into a valid SocketAddr, an error variant will be
    /// returned describing the error.
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

    /// Sets the quantiles to use when rendering histograms.
    ///
    /// Quantiles represent a scale of 0 to 1, where percentiles represent a scale of 1 to 100, so
    /// a quantile of 0.99 is the 99th percentile, and a quantile of 0.99 is the 99.9th percentile.
    ///
    /// Defaults to a hard-coded set of quantiles: 0.0, 0.5, 0.9, 0.95, 0.99, 0.999, and 1.0. This means
    /// that all histograms will be exposed as Prometheus summaries.
    ///
    /// If buckets are set (via [`set_buckets`][Self::set_buckets] or
    /// [`set_buckets_for_metric`][Self::set_buckets_for_metric]) then all histograms will be exposed
    /// as summaries instead.
    ///
    /// ## Errors
    ///
    /// If `quantiles` is empty, an error variant will be thrown.
    pub fn set_quantiles(mut self, quantiles: &[f64]) -> Result<Self, BuildError> {
        if quantiles.is_empty() {
            return Err(BuildError::EmptyBucketsOrQuantiles);
        }

        self.quantiles = parse_quantiles(quantiles);
        Ok(self)
    }

    /// Sets the buckets to use when rendering histograms.
    ///
    /// Buckets values represent the higher bound of each buckets.  If buckets are set, then all
    /// histograms will be rendered as true Statsd histograms, instead of summaries.
    ///
    /// ## Errors
    ///
    /// If `values` is empty, an error variant will be thrown.
    pub fn set_buckets(mut self, values: &[f64]) -> Result<Self, BuildError> {
        if values.is_empty() {
            return Err(BuildError::EmptyBucketsOrQuantiles);
        }

        self.buckets = Some(values.to_vec());
        Ok(self)
    }

    /// Sets the bucket for a specific pattern.
    ///
    /// The match pattern can be a full match (equality), prefix match, or suffix match.  The
    /// matchers are applied in that order if two or more matchers would apply to a single metric.
    /// That is to say, if a full match and a prefix match applied to a metric, the full match would
    /// win, and if a prefix match and a suffix match applied to a metric, the prefix match would win.
    ///
    /// Buckets values represent the higher bound of each buckets.  If buckets are set, then any
    /// histograms that match will be rendered as true Statsd histograms, instead of summaries.
    ///
    /// This option changes the observer's output of histogram-type metric into summaries.
    /// It only affects matching metrics if [`set_buckets`][Self::set_buckets] was not used.
    ///
    /// ## Errors
    ///
    /// If `values` is empty, an error variant will be thrown.
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

    /// Adds a global tag to this exporter.
    ///
    /// Global tags are applied to all metrics. Tags defined on the metric key itself have precedence
    /// over any global tags.  If this method is called multiple times, the latest value for a given label
    /// key will be used.
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

    pub fn set_max_packet_size(mut self, size: usize) -> Self {
        self.max_packet_size = size;
        self
    }

    /// Adds a global prefix for every metric name.
    ///
    /// Global prefix is applied to all metrics. Its intended use is to introduce a configurable
    /// namespace for every metric generated by the application such that different deployments
    /// can operate on their own family of metrics without overlap.
    #[must_use]
    pub fn set_global_prefix<P>(mut self, prefix: P) -> Self
    where
        P: Into<String>,
    {
        self.prefix = Some(prefix.into());
        self
    }

    /// Sets the idle timeout for metrics.
    ///
    /// If a metric hasn't been updated within this timeout, it will be removed from the registry
    /// This behavior is driven by requests to generate rendered output, and so metrics will not be
    /// removed unless a request has been made recently enough to prune the idle metrics.
    ///
    /// Further, the metric kind "mask" configures which metrics will be considered by the idle
    /// timeout.  If the kind of a metric being considered for idle timeout is not of a kind
    /// represented by the mask, it will not be affected, even if it would have othered been removed
    /// for exceeding the idle timeout.
    ///
    /// Refer to the documentation for [`MetricKindMask`](metrics_util::MetricKindMask) for more
    /// information on defining a metric kind mask.
    ///
    /// When a metric is rendered its value is replaced with a "zero-value" for that `MetricKind`
    /// however any metric with a state "zero-value" will not be rendered and will be cleaned up
    /// when its corresponding idle timeout expires.
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

    /// Builds the recorder and exporter and installs them globally.
    ///
    /// When called from within a Tokio runtime, the exporter future is spawned directly
    /// into the runtime.  Otherwise, a new single-threaded Tokio runtime is created
    /// on a background thread, and the exporter is spawned there.
    ///
    /// ## Errors
    ///
    /// If there is an error while either building the recorder and exporter, or installing the
    /// recorder and exporter, an error variant will be returned describing the error.
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

    /// Builds the recorder and exporter and returns them both.
    ///
    /// In most cases, users should prefer to use [`install`][StatsdBuilder::install] to create
    /// and install the recorder and exporter automatically for them.  If a caller is combining
    /// recorders, or needs to schedule the exporter to run in a particular way, this method, or
    /// [`build_recorder`][StatsdBuilder::build_recorder], provide the flexibility to do so.
    ///
    /// ## Panics
    ///
    /// This method must be called from within an existing Tokio runtime or it will panic.
    ///
    /// ## Errors
    ///
    /// If there is an error while building the recorder and exporter, an error variant will be
    /// returned describing the error.
    pub fn build(self) -> Result<(StatsdRecorder, ExporterFuture), BuildError> {
        let max_packet_size = self.max_packet_size;
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
                        match send_all(&client, output, &endpoint, max_packet_size).await {
                            Ok(_) => (),
                            Err(e) => error!("error sending request to push gateway: {:?}", e),
                        }
                    }
                };

                Ok((recorder, Box::pin(exporter)))
            }
        }
    }

    /// Builds the recorder and returns it.
    pub fn build_recorder(self) -> StatsdRecorder {
        self.build_with_clock(Clock::new())
    }

    pub(crate) fn build_with_clock(self, clock: Clock) -> StatsdRecorder {
        let inner = Inner {
            prefix: self.prefix.clone(),
            registry: Registry::new(GenerationalStorage::new(AtomicStorage)),
            recency: Recency::new(clock, self.recency_mask, self.idle_timeout),
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

// Packets are split alone new lines because that's how the dogstatsd protocol works
// so we look for \n in the buffer and try to put them together at that delimiter.
// it would be nicer if the handler rendered the metrics already at the packet size.
fn split_in_packets(buf: &[u8], max_packet_size: usize) -> Vec<(usize, usize)> {
    let mut n_pos_iter = buf.iter();
    let mut last_sent = 0;
    let mut packets = vec![];
    let mut acc = 0;

    while let Some(next_send_candidate) = n_pos_iter.position(|&c| c == b'\n') {
        acc += next_send_candidate + 1;
        match acc.cmp(&max_packet_size) {
            std::cmp::Ordering::Less => (), // check if there's a bigger opportunity
            std::cmp::Ordering::Equal => {
                // we can't be any bigger so save this position
                packets.push((last_sent, last_sent + acc));
                last_sent += acc;
                acc = 0;
            }
            std::cmp::Ordering::Greater => {
                // we've overshot, go back to the last value we could have sent
                packets.push((last_sent, last_sent + acc - next_send_candidate - 1));
                last_sent += acc - next_send_candidate - 1;
                acc = 0;
            }
        }
    }

    // just in case we never found a big enough package to split as the last package
    if last_sent < buf.len() {
        packets.push((last_sent, buf.len()));
    }

    packets
}

async fn send_all(
    client: &UdpSocket,
    body: String,
    endpoint: &SocketAddr,
    max_packet_size: usize,
) -> io::Result<()> {
    let buf = body.as_bytes();

    let mut sent = 0;
    let packets = split_in_packets(buf, max_packet_size);
    for (start, end) in packets {
        match client.send_to(&buf[start..end], endpoint).await {
            Ok(nsent) => {
                sent += nsent;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    if sent != buf.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "sent different size than received",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{split_in_packets, Matcher, StatsdBuilder};
    use metrics::{Key, KeyName, Label, Recorder};
    use metrics_util::MetricKindMask;
    use quanta::Clock;
    use std::time::Duration;

    #[test]
    fn test_split_packet() {
        let data = "123456789\n12345\n678\n";
        let bytes = data.as_bytes();
        let packets = split_in_packets(bytes, 10);
        assert_eq!(packets, [(0, 10), (10, 20)]);

        let bytes = "12345\n".as_bytes();
        let packets = split_in_packets(bytes, 10);
        assert_eq!(packets, [(0, 6)]);

        let data = "123456789\n12345\n6789\n";
        let bytes = data.as_bytes();
        let packets = split_in_packets(bytes, 10);
        assert_eq!(packets, [(0, 10), (10, 16), (16, 21)]);

        let data = "12345";
        let bytes = data.as_bytes();
        let packets = split_in_packets(bytes, 10);
        assert_eq!(packets, [(0, 5)]);
    }

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
        // each render call will reset the value of the counter
        let expected_gauge = "basic.gauge:-3.44|g|#wutang:forever\n\n";
        assert_eq!(rendered, expected_gauge);

        let key = Key::from_name("basic.histogram");
        let histogram1 = recorder.register_histogram(&key);
        histogram1.record(12.0);
        let rendered = handle.render();

        let histogram_data = concat!(
            "basic.histogram.min:12|g\n",
            "basic.histogram.max:12|g\n",
            "basic.histogram.avg:12|g\n",
            "basic.histogram.sum:12|g\n",
            "basic.histogram.count:1|g\n",
            "\n"
        );
        // let expected_histogram = format!("{}{}", expected_gauge, histogram_data);
        assert_eq!(rendered, histogram_data);
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
            "metrics.testing_foo.25:1|g\n",
            "metrics.testing_foo.115:1|g\n",
            "metrics.testing_foo.1015:1|g\n",
            "metrics.testing_foo._Inf:1|g\n",
            "metrics.testing_foo.avg:25|g\n",
            "metrics.testing_foo.sum:25|g\n",
            "metrics.testing_foo.count:1|g\n",
        );

        let prefix_data = concat!(
            "metrics.testing_bar.15:0|g\n",
            "metrics.testing_bar.105:1|g\n",
            "metrics.testing_bar.1005:1|g\n",
            "metrics.testing_bar._Inf:1|g\n",
            "metrics.testing_bar.avg:105|g\n",
            "metrics.testing_bar.sum:105|g\n",
            "metrics.testing_bar.count:1|g\n",
        );

        let suffix_data = concat!(
            "metrics.testin_foo.20:0|g\n",
            "metrics.testin_foo.110:0|g\n",
            "metrics.testin_foo.1010:1|g\n",
            "metrics.testin_foo._Inf:1|g\n",
            "metrics.testin_foo.avg:1010|g\n",
            "metrics.testin_foo.sum:1010|g\n",
            "metrics.testin_foo.count:1|g\n",
        );

        let default_data = concat!(
            "metrics.wee.10:0|g\n",
            "metrics.wee.100:0|g\n",
            "metrics.wee.1000:0|g\n",
            "metrics.wee._Inf:1|g\n",
            "metrics.wee.avg:1001|g\n",
            "metrics.wee.sum:1001|g\n",
            "metrics.wee.count:1|g\n",
        );

        let handle = recorder.handle();
        let rendered = handle.render();

        assert!(rendered.contains(full_data));
        assert!(rendered.contains(prefix_data));
        assert!(rendered.contains(suffix_data));
        assert!(rendered.contains(default_data));
    }

    #[ignore] // these idle timeout tests are funky with statsd, but will need to test some other way
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
            "basic.gauge:-3.44|g\n\n",
            "basic.histogram.min:1|g\n",
            "basic.histogram.max:1|g\n",
            "basic.histogram.avg:1|g\n",
            "basic.histogram.sum:1|g\n",
            "basic.histogram.count:1|g\n\n",
        );

        assert_eq!(rendered, expected);

        counter1.increment(42);
        gauge1.set(-3.44);
        histo1.record(1.0);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        counter1.increment(42);
        gauge1.set(-3.44);
        histo1.record(1.0);

        mock.increment(Duration::from_secs(11));
        let rendered = handle.render();
        assert_eq!(rendered, "");
    }

    #[ignore] // see above
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
            "basic.gauge:-3.44|g\n\n",
            "basic.histogram.min:1|g\n",
            "basic.histogram.max:1|g\n",
            "basic.histogram.avg:1|g\n",
            "basic.histogram.sum:1|g\n",
            "basic.histogram.count:1|g\n\n",
        );

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();

        let expected = "basic.gauge:-3.44|g\n\n";
        assert_eq!(rendered, expected);
    }

    #[ignore] // see above
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
            "basic.gauge:-3.44|g\n\n",
            "basic.histogram.min:1|g\n",
            "basic.histogram.max:1|g\n",
            "basic.histogram.avg:1|g\n",
            "basic.histogram.sum:1|g\n",
            "basic.histogram.count:1|g\n\n",
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
            "basic.gauge:-3.44|g\n\n",
            "basic.histogram.min:1|g\n",
            "basic.histogram.max:1|g\n",
            "basic.histogram.avg:1|g\n",
            "basic.histogram.sum:1|g\n",
            "basic.histogram.count:1|g\n",
            "basic.histogram.min:2|g|#type:special\n",
            "basic.histogram.max:2|g|#type:special\n",
            "basic.histogram.avg:2|g|#type:special\n",
            "basic.histogram.sum:2|g|#type:special\n",
            "basic.histogram.count:1|g|#type:special\n\n",
        );
        let rendered = handle.render();
        assert_eq!(rendered, expected_second);

        let expected_after = concat!(
            "basic.histogram.min:2|g|#type:special\n",
            "basic.histogram.max:2|g|#type:special\n",
            "basic.histogram.avg:2|g|#type:special\n",
            "basic.histogram.sum:2|g|#type:special\n",
            "basic.histogram.count:1|g|#type:special\n\n",
        );

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, expected_after);
    }

    #[ignore] // see above
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
        let expected = concat!("basic.counter:42|c\n\n", "basic.gauge:-3.44|g\n\n",);

        assert_eq!(rendered, expected);

        mock.increment(Duration::from_secs(9));
        let rendered = handle.render();
        assert_eq!(rendered, expected);

        let expected_second = concat!("basic.counter:42|c\n\n", "basic.gauge:-3.44|g\n\n",);
        let rendered = handle.render();
        assert_eq!(rendered, expected_second);

        counter1.increment(1);

        let expected_after = concat!("basic.counter:43|c\n\n",);

        mock.increment(Duration::from_secs(2));
        let rendered = handle.render();
        assert_eq!(rendered, expected_after);
    }

    #[ignore] // see above
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
        let key =
            Key::from_name(key_name).with_extra_labels(vec![Label::new("øhno", "\"yeet\nies\\\"")]);
        let counter1 = recorder.register_counter(&key);
        counter1.increment(1);

        let handle = recorder.handle();
        let rendered = handle.render();
        let expected_counter = "yee_haw_lets_go:1|c|#foo_:foo,øhno:_yeet_ies__\n\n";

        assert_eq!(rendered, expected_counter);
    }
}
