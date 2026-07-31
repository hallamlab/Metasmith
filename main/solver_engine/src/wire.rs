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
//! Non-finite floats are the one place the format is not plain JSON: `NaN` and
//! `Infinity` are not JSON, Python's `json` emits them anyway and `serde_json`
//! rejects them. Since NaN *ranking* is part of the decision contract, the wire
//! cannot quietly not support NaN -- so a score is either a JSON number or one
//! of the strings below, on both sides.

use serde::{Deserialize, Serialize};

/// Bump when the envelope changes shape. Independent of `SOLVER_RNG_VERSION`.
pub const WIRE_VERSION: u32 = 1;

/// What this binary can actually be asked to do. The Python side falls back to
/// its own implementation for anything not advertised here, which is how the
/// port ships one capability at a time instead of all at once.
pub const CAPABILITIES: &[&str] = &["rng"];

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
