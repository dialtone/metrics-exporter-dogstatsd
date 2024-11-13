use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::common::Snapshot;
use crate::distribution::{Distribution, DistributionBuilder};
use crate::formatting::{key_to_parts, write_metric_line};
use crate::registry::GenerationalAtomicStorage;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use metrics_util::registry::{Recency, Registry};

use indexmap::IndexMap;
use quanta::Instant;

pub(crate) struct Inner {
    pub prefix: Option<String>,
    pub registry: Registry<Key, GenerationalAtomicStorage>,
    pub recency: Recency<Key>,
    pub distribution_builder: DistributionBuilder,
    pub global_tags: IndexMap<String, String>,
}

type Distributions = HashMap<String, IndexMap<Vec<String>, Distribution>>;

impl Inner {
    fn get_recent_metrics(&self) -> Snapshot {
        let mut counters = HashMap::new();
        let counter_handles = self.registry.get_counter_handles();
        for (key, counter) in counter_handles {
            let gen = counter.get_generation();
            if !self.recency.should_store_counter(&key, gen, &self.registry) {
                continue;
            }
            let (name, labels) = key_to_parts(&key, Some(&self.global_tags));
            let value = counter.get_inner().swap(0, Ordering::Acquire);
            let entry = counters
                .entry(name)
                .or_insert_with(HashMap::new)
                .entry(labels)
                .or_insert(0);
            *entry = value;
        }

        let mut gauges = HashMap::new();
        let gauge_handles = self.registry.get_gauge_handles();
        for (key, gauge) in gauge_handles {
            let gen = gauge.get_generation();
            if !self.recency.should_store_gauge(&key, gen, &self.registry) {
                continue;
            }

            let (name, labels) = key_to_parts(&key, Some(&self.global_tags));
            let value = f64::from_bits(gauge.get_inner().swap(0, Ordering::Acquire));
            let entry = gauges
                .entry(name)
                .or_insert_with(HashMap::new)
                .entry(labels)
                .or_insert(0.0);
            *entry = value;
        }

        let histogram_handles = self.registry.get_histogram_handles();
        let mut distributions: Distributions = HashMap::new();
        for (key, histogram) in histogram_handles {
            let gen = histogram.get_generation();
            if !self
                .recency
                .should_store_histogram(&key, gen, &self.registry)
            {
                continue;
            }

            let (name, labels) = key_to_parts(&key, Some(&self.global_tags));

            let entry = distributions
                .entry(name.clone())
                .or_insert_with(IndexMap::new)
                .entry(labels)
                .or_insert_with(|| self.distribution_builder.get_distribution(name.as_str()));

            histogram
                .get_inner()
                .clear_with(|samples| entry.record_samples(samples));
        }

        Snapshot {
            counters,
            gauges,
            distributions,
        }
    }

    fn render(&self) -> String {
        let Snapshot {
            mut counters,
            mut distributions,
            mut gauges,
        } = self.get_recent_metrics();

        let mut output = String::new();

        for (name, mut by_labels) in counters.drain() {
            let mut wrote = false;
            for (labels, value) in by_labels.drain() {
                if value == 0 {
                    continue;
                }
                wrote = true;
                write_metric_line::<&str, u64>(
                    &mut output,
                    self.prefix.as_deref(),
                    &name,
                    None,
                    "c",
                    &labels,
                    None,
                    value,
                    None,
                    None,
                );
            }
            if wrote {
                output.push('\n');
            }
        }

        for (name, mut by_labels) in gauges.drain() {
            let mut wrote = false;
            for (labels, value) in by_labels.drain() {
                if value == 0.0 {
                    continue;
                }
                wrote = true;
                write_metric_line::<&str, f64>(
                    &mut output,
                    self.prefix.as_deref(),
                    &name,
                    None,
                    "g",
                    &labels,
                    None,
                    value,
                    None,
                    None,
                );
            }
            if wrote {
                output.push('\n');
            }
        }

        for (name, mut by_labels) in distributions.drain() {
            let mut wrote = false;
            for (labels, distribution) in by_labels.drain(..) {
                let (sum, count) = match distribution {
                    Distribution::Summary(summary, quantiles, sum) => {
                        let count = summary.count();
                        if count == 0 {
                            continue;
                        }
                        wrote = true;
                        let snapshot = summary.snapshot(Instant::now());
                        for quantile in quantiles.iter() {
                            let value = snapshot.quantile(quantile.value()).unwrap_or(0.0);
                            let qv = quantile.value().to_string();
                            let quantile_name = if qv == "0" {
                                "min"
                            } else if qv == "0.5" {
                                "median"
                            } else if qv == "1" {
                                "max"
                            } else {
                                qv.as_str()
                            };

                            write_metric_line(
                                &mut output,
                                self.prefix.as_deref(),
                                &name,
                                None,
                                "g",
                                &labels,
                                Some(quantile_name),
                                value,
                                None,
                                None,
                            );
                        }

                        (sum, count as u64)
                    }
                    Distribution::Histogram(histogram) => {
                        let count = histogram.count();
                        if count == 0 {
                            continue;
                        }
                        wrote = true;
                        for (le, count) in histogram.buckets() {
                            write_metric_line(
                                &mut output,
                                self.prefix.as_deref(),
                                &name,
                                None,
                                "g",
                                &labels,
                                Some(le),
                                count,
                                None,
                                None,
                            );
                        }
                        write_metric_line(
                            &mut output,
                            self.prefix.as_deref(),
                            &name,
                            None,
                            "g",
                            &labels,
                            Some("+Inf"),
                            histogram.count(),
                            None,
                            None,
                        );

                        (histogram.sum(), count)
                    }
                    Distribution::Distribution(dist) => {
                        let count = dist.len();
                        let mut sum = 0.0;
                        for v in dist.iter().copied() {
                            sum += v;
                            wrote = true;
                            write_metric_line::<f64, f64>(
                                &mut output,
                                self.prefix.as_deref(),
                                &name,
                                None,
                                "d",
                                &labels,
                                None,
                                v,
                                None,
                                None,
                            );
                        }
                        (sum, count as u64)
                    }
                };

                write_metric_line::<&str, f64>(
                    &mut output,
                    self.prefix.as_deref(),
                    &name,
                    Some("avg"),
                    "g",
                    &labels,
                    None,
                    sum / count as f64,
                    None,
                    None,
                );
                write_metric_line::<&str, f64>(
                    &mut output,
                    self.prefix.as_deref(),
                    &name,
                    Some("sum"),
                    "g",
                    &labels,
                    None,
                    sum,
                    None,
                    None,
                );
                write_metric_line::<&str, u64>(
                    &mut output,
                    self.prefix.as_deref(),
                    &name,
                    Some("count"),
                    "g",
                    &labels,
                    None,
                    count,
                    None,
                    None,
                );
            }
            if wrote {
                output.push('\n');
            }
        }

        output
    }
}

pub struct StatsdRecorder {
    inner: Arc<Inner>,
}

impl StatsdRecorder {
    pub fn handle(&self) -> StatsdHandle {
        StatsdHandle {
            inner: self.inner.clone(),
        }
    }
}

impl From<Inner> for StatsdRecorder {
    fn from(inner: Inner) -> Self {
        StatsdRecorder {
            inner: Arc::new(inner),
        }
    }
}

impl Recorder for StatsdRecorder {
    fn describe_counter(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
    fn describe_gauge(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}
    fn describe_histogram(&self, _k: KeyName, _u: Option<Unit>, _d: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        self.inner
            .registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

/// Handle for accessing metrics stored via [`StatsdRecorder`].
///
/// In certain scenarios, it may be necessary to directly handle requests that would otherwise be
/// handled directly by the HTTP listener, or push gateway background task.  [`StatsdHandle`]
/// allows rendering a snapshot of the current metrics stored by an installed [`StatsdRecorder`]
/// as a payload conforming to the Statsd exposition format.
#[derive(Clone)]
pub struct StatsdHandle {
    inner: Arc<Inner>,
}

impl StatsdHandle {
    /// Takes a snapshot of the metrics held by the recorder and generates a payload conforming to
    /// the Statsd exposition format.
    pub fn render(&self) -> String {
        self.inner.render()
    }
}
