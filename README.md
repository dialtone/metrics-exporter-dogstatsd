# metrics-exporter-dogstatsd

__metrics-exporter-dogstatsd__ is a `metrics`-compatible exporter that
aggregates metrics and pushes them to a statsd/dogstatsd agent.

A good chunk of this code is taken from the [Prometheus exporter](https://github.com/metrics-rs/metrics/tree/main/metrics-exporter-prometheus) with some important changes in behavior.

Compared with other statsd metrics systems like [cadence] or
[metrics-exporter-statsd], this crate is fully asynchronous in how it
communicates data to the statsd agent via Tokio. This crate also pre-aggregates
all of the metrics and communicates them to the agent on an interval limiting
the amount of network traffic and system calls.

This crate takes full advantage of atomics and the performance of the `metrics` crate.

## superceded by metrics-rs

This crate has now (January 2025) been [superceded](https://github.com/metrics/metrics-rs) by the upstream version of the crate, maintained by the `metrics-rs` team. Version 0.8.0 is the last release based on _this_ repository, and users are encouraged to upgrade to 0.9.0 where possible, and file issues upstream if there are any identified gaps.


[cadence]: https://github.com/56quarters/cadence/
[metrics-exporter-statsd]: https://github.com/github/metrics-exporter-statsd
