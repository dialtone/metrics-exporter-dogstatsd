#![allow(clippy::too_many_arguments)]
use indexmap::IndexMap;
use metrics::Key;

// <METRIC_NAME>:<VALUE>|<TYPE>|@<SAMPLE_RATE>|#<TAG_&KEY_1>:<TAG_VALUE_1>,<TAG_2>
// v1.1
// <METRIC_NAME>:<VALUE1>:<VALUE2>:<VALUE3>|<TYPE>|@<SAMPLE_RATE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>
// v1.2
// <METRIC_NAME>:<VALUE>|<TYPE>|#<TAG_KEY_1>:<TAG_VALUE_1>,<TAG_2>|c:<CONTAINER_ID>
pub fn key_to_parts(
    key: &Key,
    default_labels: Option<&IndexMap<String, String>>,
) -> (String, Vec<String>) {
    let name = sanitize_metric_name(key.name());
    let mut values = default_labels.cloned().unwrap_or_default();
    key.labels().into_iter().for_each(|label| {
        values.insert(label.key().to_string(), label.value().to_string());
    });
    let labels = values
        .iter()
        .map(|(k, v)| format!("{}:{}", sanitize_label(k), sanitize_label_value(v)))
        .collect();

    (name, labels)
}

/// Sanitizes a label key to be valid under the datadog [data model].
///
/// [data model]: https://docs.datadoghq.com/getting_started/tagging/
pub fn sanitize_label(key: &str) -> String {
    // The first character must be [a-zA-Z_], and all subsequent characters must be [a-zA-Z0-9_].
    let mut out = String::with_capacity(key.len());
    let mut is_invalid: fn(char) -> bool = invalid_label_start_character;
    let mut key_chars = key.chars().peekable();
    let mut first = true;
    while let Some(c) = key_chars.next() {
        if !first && key_chars.peek().is_none() {
            is_invalid = invalid_label_last_character;
        }
        if is_invalid(c) {
            out.push('_');
        } else {
            out.push(c);
        }
        is_invalid = invalid_label_character;
        first = false;
    }
    out
}

pub fn sanitize_label_value(key: &str) -> String {
    // The first character must be [a-zA-Z_], and all subsequent characters must be [a-zA-Z0-9_].
    let mut out = String::with_capacity(key.len());
    for c in key.chars() {
        if invalid_label_character(c) {
            out.push('_');
        } else {
            out.push(c);
        }
    }
    out
}

/// Metrics naming [rules](https://docs.datadoghq.com/developers/dogstatsd/datagram_shell?tabs=metrics)
/// - Metric names must start with a letter.
/// - Metric names must only contain ASCII alphanumerics, underscores, and periods.
/// - Other characters, including spaces, are converted to underscores.
/// - Unicode is not supported.
/// - Metric names must not exceed 200 characters. Fewer than 100 is preferred from a UI perspective.
pub fn sanitize_metric_name(name: &str) -> String {
    // The first character must be [a-zA-Z_:], and all subsequent characters must be [a-zA-Z0-9_:].
    let mut out = String::with_capacity(name.len());
    let mut is_invalid: fn(char) -> bool = invalid_metric_name_start_character;
    for c in name.chars() {
        if is_invalid(c) {
            out.push('_');
        } else {
            out.push(c);
        }
        is_invalid = invalid_metric_name_character;
    }
    out
}

pub fn sanitize_metric_suffix(name: &str) -> String {
    // All subsequent characters must be [a-zA-Z0-9_:].
    let mut out = String::with_capacity(name.len());
    for c in name.chars() {
        if invalid_metric_segment_character(c) {
            out.push('_');
        } else {
            out.push(c);
        }
    }
    out
}

pub fn write_metric_line<T, T2>(
    buffer: &mut String,
    prefix: Option<&str>,
    name: &str,
    suffix: Option<&str>,
    mtype: &str,
    labels: &[String],
    quantile: Option<T>,
    value: T2,
    container: Option<&str>,
    sample_rate: Option<&str>,
) where
    T: std::fmt::Display,
    T2: std::fmt::Display,
{
    if let Some(pref) = prefix {
        buffer.push_str(sanitize_metric_name(pref).as_str());
        buffer.push('.');
    }
    buffer.push_str(name);

    if let Some(suf) = suffix {
        buffer.push('.');
        buffer.push_str(sanitize_metric_suffix(suf).as_str());
    }

    if let Some(qnt) = quantile {
        buffer.push('.');
        buffer.push_str(sanitize_metric_suffix(qnt.to_string().as_str()).as_str());
    }

    buffer.push(':');
    buffer.push_str(value.to_string().as_str());
    buffer.push('|');
    buffer.push_str(mtype);

    if let Some(rate) = sample_rate {
        buffer.push_str("|@");
        buffer.push_str(rate);
    }

    if !labels.is_empty() {
        buffer.push_str("|#");

        let mut first = true;
        for label in labels {
            if first {
                first = false;
            } else {
                buffer.push(',');
            }
            buffer.push_str(label);
        }
    }

    if let Some(cntr_id) = container {
        buffer.push_str("|c:");
        buffer.push_str(cntr_id);
    }
    buffer.push('\n');
}

#[inline]
fn invalid_metric_name_start_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z_:].
    !(c.is_ascii_alphabetic() || c == '_')
}

#[inline]
fn invalid_metric_name_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z0-9_.].
    !(c.is_ascii_alphanumeric() || c == '_' || c == '.')
}

#[inline]
fn invalid_metric_segment_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z0-9_.].
    !(c.is_ascii_alphanumeric() || c == '_')
}

// 1. Tags must start with a letter and after that may contain the characters listed below:

// Alphanumerics
// Underscores
// Minuses
// Colons
// Periods
// Slashes
// Other special characters are converted to underscores.
// Note: A tag cannot end with a colon, for example tag:.

// Tags can be up to 200 characters long and support Unicode (which includes most character sets, including languages such as Japanese).

// Tags are converted to lowercase. Therefore, CamelCase tags are not recommended. Authentication (crawler) based integrations convert camel case tags to underscores, for example TestTag –> test_tag. Note: host and device tags are excluded from this conversion.

// A tag can be in the format value or <KEY>:<VALUE>. Commonly used tag keys are env, instance, and name. The key always precedes the first colon of the global tag definition, for example:

// TAG	KEY	VALUE
// env:staging:east	env	staging:east
// env_staging:east	env_staging	east
// Tags should not originate from unbounded sources, such as epoch timestamps, user IDs, or request IDs. Doing so may infinitely increase the number of metrics for your organization and impact your billing.

// Limitations (such as downcasing) only apply to metric tags, not log attributes or span tags.

#[inline]
fn invalid_label_start_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z].
    !c.is_alphabetic()
}

#[inline]
fn invalid_label_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z0-9_.].
    !(c.is_alphanumeric() || c == '_' || c == '.' || c == ':' || c == '/' || c == '-')
}

#[inline]
fn invalid_label_last_character(c: char) -> bool {
    // Essentially, needs to match the regex pattern of [a-zA-Z0-9_.].
    !(c.is_alphanumeric() || c == '_' || c == '.' || c == '/' || c == '-')
}
