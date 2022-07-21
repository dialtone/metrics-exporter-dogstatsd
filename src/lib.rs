//! A [`metrics`]-compatible exporter for sending metrics to statsd/datadog.
//!
//! ## Basics
//!
//! `metrics-exporter-dogstatsd` is a [`metrics`]-compatible exporter that can push metrics to
//! a statsd or datadog agent running locally or remotely via UDP.
//!
//! ## High-level features
//!
//! - push gateway support via UDP
//! - ability to push histograms as either aggregated summaries or aggregated histograms, with
//!   configurable quantiles/buckets
//! - ability to control bucket configuration on a per-metric basis
//! - configurable global labels (applied to all metrics, overridden by metric's own labels if present)
//!
//! ## Behavior
//!
//! This exporter makes some explicit trade-offs to accomplish its task:
//!
//! - Aggregated histograms or summaries are exported as a series of gauges
//! - Each interval of the exporter will reset any rendered metric
//! - All metrics are first aggregated locally and then pushed to the endpoint
//! - There is currently no support for sampling
//! - There is currently no support for container id in the metric definition
//! - Aggregated metrics are pushed to a network service via UDP using the tokio async framework
//!
//! All naming conversions for [metrics] and [tags] are compliant with DataDog requirements.
//!
//! ## Usage
//!
//! Using the exporter is straightforward:
//!
//! ```ignore
//! // First, create a builder.
//! //
//! // The builder can configure many aspects of the exporter, setting global tags,
//! // adjusting how histograms will be reported, changing how long metrics
//! // can be idle before being removed, and more.
//! let builder = StatsdBuilder::new();
//!
//! // Normally, most users will want to "install" the exporter which sets it as the
//! // global recorder for all `metrics` calls, and installs a simple asynchronous
//! // task which pushes to the configured push gateway on the given interval.
//! //
//! // If you're already inside a Tokio runtime, this will spawn a task for the
//! // exporter on that runtime, and otherwise, a new background thread will be
//! // spawned which a Tokio single-threaded runtime is launched on to, where we then
//! // finally launch the exporter:
//! builder.install().expect("failed to install recorder/exporter");
//!
//! // Maybe you have a more complicated setup and want to be handed back the recorder
//! // object and a future that can run the HTTP listener / push gateway so you can
//! // install/spawn them in a specific way.. also not a problem!
//! //
//! // As this is a more advanced method, it _must_ be called from within an existing
//! // Tokio runtime when the exporter is running in HTTP listener/scrape endpoint mode.
//! let (recorder, exporter) = builder.build().expect("failed to build recorder/exporter");
//!
//! // Finally, maybe you literally only want to build the recorder and nothing else,
//! // and we've got you covered there, too:
//! let recorder = builder.build_recorder().expect("failed to build recorder");
//! ```
//!
//! [tags]: https://docs.datadoghq.com/getting_started/tagging/#define-tags
//! [metrics]: https://docs.datadoghq.com/metrics/custom_metrics/#naming-custom-metrics
mod common;
pub use self::common::{BuildError, Matcher};

mod distribution;
pub use distribution::{Distribution, DistributionBuilder};

mod builder;
pub use self::builder::StatsdBuilder;

pub mod formatting;
mod recorder;

mod registry;

pub use self::recorder::{StatsdHandle, StatsdRecorder};
