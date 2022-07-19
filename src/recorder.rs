use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::common::Snapshot;
use crate::distribution::{Distribution, DistributionBuilder};
use crate::formatting::{key_to_parts, sanitize_metric_name, write_metric_line};
use crate::registry::GenerationalAtomicStorage;

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Recorder, SharedString, Unit};
use metrics_util::registry::{Recency, Registry};

use indexmap::IndexMap;
use parking_lot::RwLock;
use quanta::Instant;

pub(crate) struct Inner {
    pub registry: Registry<Key, GenerationalAtomicStorage>,
    pub recency: Recency<Key>,
    pub distributions: RwLock<HashMap<String, IndexMap<Vec<String>, Distribution>>>,
    pub distribution_builder: DistributionBuilder,
    pub descriptions: RwLock<HashMap<String, SharedString>>,
    pub global_tags: IndexMap<String, String>,
}

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
            let value = counter.get_inner().load(Ordering::Acquire);
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
            let value = f64::from_bits(gauge.get_inner().load(Ordering::Acquire));
            let entry = gauges
                .entry(name)
                .or_insert_with(HashMap::new)
                .entry(labels)
                .or_insert(0.0);
            *entry = value;
        }

        let histogram_handles = self.registry.get_histogram_handles();
        for (key, histogram) in histogram_handles {
            let gen = histogram.get_generation();
            if !self
                .recency
                .should_store_histogram(&key, gen, &self.registry)
            {
                // Since we store aggregated distributions directly, when we're told that a metric
                // is not recent enough and should be/was deleted from the registry, we also need to
                // delete it on our side as well.
                let (name, labels) = key_to_parts(&key, Some(&self.global_tags));
                let mut wg = self.distributions.write();
                let delete_by_name = if let Some(by_name) = wg.get_mut(&name) {
                    by_name.remove(&labels);
                    by_name.is_empty()
                } else {
                    false
                };

                // If there's no more variants in the per-metric-name distribution map, then delete
                // it entirely, otherwise we end up with weird empty output during render.
                if delete_by_name {
                    wg.remove(&name);
                }

                continue;
            }

            let (name, labels) = key_to_parts(&key, Some(&self.global_tags));

            let mut wg = self.distributions.write();
            let entry = wg
                .entry(name.clone())
                .or_insert_with(IndexMap::new)
                .entry(labels)
                .or_insert_with(|| self.distribution_builder.get_distribution(name.as_str()));

            histogram
                .get_inner()
                .clear_with(|samples| entry.record_samples(samples));
        }

        let distributions = self.distributions.read().clone();

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
            for (labels, value) in by_labels.drain() {
                write_metric_line::<&str, u64>(
                    &mut output,
                    &name,
                    "c",
                    &labels,
                    None,
                    value,
                    None,
                    None,
                );
            }
            output.push('\n');
        }

        for (name, mut by_labels) in gauges.drain() {
            for (labels, value) in by_labels.drain() {
                write_metric_line::<&str, f64>(
                    &mut output,
                    &name,
                    "g",
                    &labels,
                    None,
                    value,
                    None,
                    None,
                );
            }
            output.push('\n');
        }

        for (name, mut by_labels) in distributions.drain() {
            for (labels, distribution) in by_labels.drain(..) {
                let (sum, count) = match distribution {
                    Distribution::Summary(summary, quantiles, sum) => {
                        let snapshot = summary.snapshot(Instant::now());
                        for quantile in quantiles.iter() {
                            let value = snapshot.quantile(quantile.value()).unwrap_or(0.0);
                            write_metric_line(
                                &mut output,
                                &name,
                                "h",
                                &labels,
                                Some(quantile.value()),
                                value,
                                None,
                                None,
                            );
                        }

                        (sum, summary.count() as u64)
                    }
                    Distribution::Histogram(_) => (0 as f64, 0 as u64),
                };
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

    fn add_description_if_missing(&self, key_name: &KeyName, description: SharedString) {
        let sanitized = sanitize_metric_name(key_name.as_str());
        let mut descriptions = self.inner.descriptions.write();
        descriptions.entry(sanitized).or_insert(description);
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
    fn describe_counter(&self, key_name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.add_description_if_missing(&key_name, description);
    }

    fn describe_gauge(&self, key_name: KeyName, _unit: Option<Unit>, description: SharedString) {
        self.add_description_if_missing(&key_name, description);
    }

    fn describe_histogram(
        &self,
        key_name: KeyName,
        _unit: Option<Unit>,
        description: SharedString,
    ) {
        self.add_description_if_missing(&key_name, description);
    }

    fn register_counter(&self, key: &Key) -> Counter {
        self.inner
            .registry
            .get_or_create_counter(key, |c| c.clone().into())
    }

    fn register_gauge(&self, key: &Key) -> Gauge {
        self.inner
            .registry
            .get_or_create_gauge(key, |c| c.clone().into())
    }

    fn register_histogram(&self, key: &Key) -> Histogram {
        self.inner
            .registry
            .get_or_create_histogram(key, |c| c.clone().into())
    }
}

/// Handle for accessing metrics stored via [`PrometheusRecorder`].
///
/// In certain scenarios, it may be necessary to directly handle requests that would otherwise be
/// handled directly by the HTTP listener, or push gateway background task.  [`PrometheusHandle`]
/// allows rendering a snapshot of the current metrics stored by an installed [`PrometheusRecorder`]
/// as a payload conforming to the Prometheus exposition format.
#[derive(Clone)]
pub struct StatsdHandle {
    inner: Arc<Inner>,
}

impl StatsdHandle {
    /// Takes a snapshot of the metrics held by the recorder and generates a payload conforming to
    /// the Prometheus exposition format.
    pub fn render(&self) -> String {
        self.inner.render()
    }
}
