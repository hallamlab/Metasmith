//! The wire format between the Python planner and this binary.
//!
//! It is a real interface with two independent implementations, so it carries a
//! version from the first commit rather than from the first time it breaks. The
//! scar tissue is `LIN_PAYLOAD_VERSION`, which drifted from its second
//! (Groovy) implementation and failed every containerized task while the fast
//! suite stayed green: a version constant only helps if it is shared and
//! checked, and a *single* constant covering two things that can move
//! independently is how the last desync went unnoticed. Hence two constants.
//!
//! `WIRE_VERSION` covers the envelope -- field names, framing, how a request
//! and a reply are shaped. `SOLVER_RNG_VERSION` (in `rng`) covers the decision
//! contract. A change to either is a change the other side must agree to, and
//! they move for different reasons.
//!
//! Floats have to survive the crossing unchanged, and by default they did not.
//! `serde_json` documents its float parsing as *best effort* -- roughly twice as
//! fast, and not always the double the writer wrote. It read
//! `0.9999999999999999` as exactly `1.0`, and read other perfectly ordinary
//! values a few ulps off. That is not a rounding curiosity: the scores these
//! numbers become are compared, and comparisons decide plans. The
//! `float_roundtrip` feature in `Cargo.toml` is therefore load-bearing, and
//! `tests` below pins it.
//!
//! Non-finite floats are the one place the format is not plain JSON: `NaN` and
//! `Infinity` are not JSON, Python's `json` emits them anyway and `serde_json`
//! rejects them. Since NaN *ranking* is part of the decision contract, the wire
//! cannot quietly not support NaN -- so a score is either a JSON number or one
//! of the strings below, on both sides.

use serde::{Deserialize, Serialize};

/// Bump when the envelope changes shape. Independent of `SOLVER_RNG_VERSION`.
///
/// v2 adds the score primitives (`entropy`, `log2`) to the trace request. A new
/// op variant is an envelope change like any other: an older engine handed one
/// fails to deserialize the request rather than answering it, and "fails rather
/// than answers" is only a good outcome if the handshake caught it first.
pub const WIRE_VERSION: u32 = 2;

/// What this binary can actually be asked to do. The Python side falls back to
/// its own implementation for anything not advertised here, which is how the
/// port ships one capability at a time instead of all at once.
pub const CAPABILITIES: &[&str] = &["rng", "solve"];

pub const ENGINE_NAME: &str = "msm_solver";
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// A float on the wire: a number, or a name for the three values JSON has none.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Scalar {
    Num(f64),
    Name(String),
}

impl Scalar {
    pub fn to_f64(&self) -> Result<f64, String> {
        match self {
            Scalar::Num(v) => Ok(*v),
            Scalar::Name(s) => match s.as_str() {
                "nan" => Ok(f64::NAN),
                "inf" => Ok(f64::INFINITY),
                "-inf" => Ok(f64::NEG_INFINITY),
                other => Err(format!("not a float: {other:?}")),
            },
        }
    }
}

pub fn decode_scalars(xs: &[Scalar]) -> Result<Vec<f64>, String> {
    xs.iter().map(|x| x.to_f64()).collect()
}

/// The inverse, for results rather than arguments. `serde_json` refuses to
/// serialize a non-finite float at all, so a reply carrying one has to name it
/// the same way a request does.
pub fn encode_scalar(v: f64) -> serde_json::Value {
    if v.is_nan() { return serde_json::json!("nan"); }
    if v.is_infinite() { return serde_json::json!(if v > 0.0 { "inf" } else { "-inf" }); }
    serde_json::json!(v)
}

#[derive(Debug, Serialize)]
pub struct VersionReply {
    pub engine: &'static str,
    pub engine_version: &'static str,
    pub wire_version: u32,
    pub rng_version: u32,
    pub capabilities: Vec<&'static str>,
}

/// One decision to make, or one pure ranking to perform.
///
/// The pure ops carry no randomness but still report the draw counter, because
/// a differential test that only compares *values* cannot see the two streams
/// drifting apart by a word until the drift finally changes an answer.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    RawWords { n: usize },
    BoundedInt { n: u64 },
    WeightedIndex { weights: Vec<i64> },
    PickTopK { scores: Vec<Scalar>, k: usize },
    TopK { scores: Vec<Scalar>, k: usize },
    Argmax { scores: Vec<Scalar> },
    Argmin { values: Vec<Scalar> },
    /// The score's own float rules, which are a contract for the same reason
    /// the decision rules are: the entropy feeds `score_node`, and the selection
    /// rules compare scores. A last-bit disagreement is a different plan.
    Entropy { counts: Vec<i64> },
    /// The one function both implementations delegate to a C library rather than
    /// defining. Probed directly so that a musl-vs-glibc divergence is a failing
    /// test naming the input, not a plan that is subtly wrong on one platform.
    Log2 { values: Vec<Scalar> },
}

#[derive(Debug, Deserialize)]
pub struct TraceRequest {
    pub wire_version: u32,
    pub seed: u64,
    pub ops: Vec<Op>,
}

#[derive(Debug, Serialize)]
pub struct OpResult {
    pub value: serde_json::Value,
    /// Words consumed *in total* up to and including this op.
    pub draws: u64,
}

#[derive(Debug, Serialize)]
pub struct TraceReply {
    pub wire_version: u32,
    pub rng_version: u32,
    pub results: Vec<OpResult>,
    pub draws: u64,
}

/// What the engine derived from a problem, before it made a single decision.
///
/// Not a capability -- nothing at plan time asks for this. It is a seam for the
/// differential tests: the maps below are pure functions of the problem, so the
/// two implementations can be shown to agree on them *before* either one starts
/// searching. A disagreement here is a bug with an address; the same
/// disagreement found through a differing plan is a bug with a haystack.
#[derive(Debug, Serialize)]
pub struct DescribeReply {
    pub wire_version: u32,
    pub no_path_possible: bool,
    pub max_distance: i64,
    pub relevant_transforms: Vec<u32>,
    pub free_transforms: Vec<u32>,
    /// `[node, rank]`, ascending by rank.
    pub dep_rank: Vec<(u32, u32)>,
    /// `[transform, value]`, ascending by transform.
    pub distance: Vec<(u32, i64)>,
    pub opportunity: Vec<(u32, i64)>,
    /// `[dependency, [...]]`, ascending by dependency; inner lists in the order
    /// the search reads them, which for the two demand maps is rank order.
    pub demand2product: Vec<(u32, Vec<u32>)>,
    pub demand2producer: Vec<(u32, Vec<u32>)>,
    pub product2consumer: Vec<(u32, Vec<u32>)>,
    pub inherent_parents: Vec<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `float_roundtrip` feature, pinned where dropping it fails loudly.
    ///
    /// Without it these literals parse to a *different double* than the one
    /// Python wrote -- the first to exactly 1.0, which is not even close. The
    /// comparison is against `f64::from_str`, which is correctly rounded and is
    /// what CPython's `float()` also does, so agreeing with it is agreeing with
    /// the other side of the wire.
    #[test]
    fn a_float_arrives_as_the_float_that_was_sent() {
        for lit in [
            "0.9999999999999999", // one ulp below 1.0
            "0.10669708251953125",
            "0.9640369415283203",
            "1e-300",
            "5e-324",             // the smallest denormal
            "-0.0",
            "1.7976931348623157e308",
        ] {
            let via_json: f64 = serde_json::from_str(lit).unwrap();
            let direct: f64 = lit.parse().unwrap();
            assert_eq!(
                via_json.to_bits(), direct.to_bits(),
                "serde_json read {lit} as {via_json:?}, not {direct:?}",
            );
        }
    }

    /// And the return trip. `serde_json` writes the shortest representation that
    /// round-trips, which is what `float()` reads back exactly -- so this
    /// direction was never the broken one, and is pinned so it stays that way.
    #[test]
    fn a_float_leaves_as_the_float_that_was_computed() {
        for bits in [0x3fefffffffffffffu64, 0x3fbb508000000000, 0x8000000000000000, 1] {
            let v = f64::from_bits(bits);
            let text = serde_json::to_string(&encode_scalar(v)).unwrap();
            let back: f64 = text.parse().unwrap();
            assert_eq!(back.to_bits(), bits, "{v:?} serialized to {text}");
        }
    }

    #[test]
    fn the_three_values_json_cannot_write_have_names() {
        assert!(encode_scalar(f64::NAN).is_string());
        assert_eq!(encode_scalar(f64::INFINITY), serde_json::json!("inf"));
        assert_eq!(encode_scalar(f64::NEG_INFINITY), serde_json::json!("-inf"));
        assert!(Scalar::Name("nan".into()).to_f64().unwrap().is_nan());
        assert!(Scalar::Name("bogus".into()).to_f64().is_err());
    }
}
