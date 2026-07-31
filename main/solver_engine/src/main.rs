//! `msm_solver` -- the Rust half of the metasmith plan solver.
//!
//! Unlike `msm_relay`, this binary runs *locally*, in whatever process is
//! planning (CLI, GUI, notebook), so it ships inside the pip wheel and conda
//! package rather than only inside the agent image. Presence means use it;
//! absence means the Python solver runs instead. `version` is how the caller
//! finds out which of those it is looking at, and what this build can be asked
//! to do.
//!
//! What it can be asked to do today is `rng` and nothing else: the decision
//! contract, ported and differentially tested, with the search still to come.
//! The Python side reads `capabilities` and falls back for the rest, so the
//! delivery path -- four targets, packaging, resolution, fallback -- is proven
//! before the search depends on it.

mod det;
mod model;
mod problem;
mod rng;
mod smath;
mod wire;

use clap::{Parser, Subcommand};
use std::io::{self, Read, Write};

use crate::rng::{DecisionStream, argmax_index, argmin_index, top_k_indices};
use crate::wire::{
    CAPABILITIES, ENGINE_NAME, ENGINE_VERSION, Op, OpResult, TraceReply, TraceRequest,
    VersionReply, WIRE_VERSION, decode_scalars, encode_scalar,
};

#[derive(Parser, Debug)]
#[command(author, version, about = "metasmith plan solver engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Report the versions and capabilities of this build, as JSON on stdout.
    Version,
    /// Replay a script of decisions and report each answer. The differential
    /// harness; not used at plan time.
    RngTrace,
    /// Read a problem and report what was derived from it, without searching.
    /// Also the differential harness -- see `wire::DescribeReply`.
    Describe,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.command {
        Commands::Version => cmd_version(),
        Commands::RngTrace => cmd_rng_trace(),
        Commands::Describe => cmd_describe(),
    };
    if let Err(e) = result {
        eprintln!("msm_solver: {e}");
        std::process::exit(1);
    }
}

fn emit<T: serde::Serialize>(value: &T) -> Result<(), String> {
    let s = serde_json::to_string(value).map_err(|e| e.to_string())?;
    let mut out = io::stdout();
    out.write_all(s.as_bytes()).map_err(|e| e.to_string())?;
    out.write_all(b"\n").map_err(|e| e.to_string())?;
    out.flush().map_err(|e| e.to_string())
}

fn cmd_version() -> Result<(), String> {
    emit(&VersionReply {
        engine: ENGINE_NAME,
        engine_version: ENGINE_VERSION,
        wire_version: WIRE_VERSION,
        rng_version: rng::SOLVER_RNG_VERSION,
        capabilities: CAPABILITIES.to_vec(),
    })
}

fn cmd_rng_trace() -> Result<(), String> {
    let mut raw = String::new();
    io::stdin().read_to_string(&mut raw).map_err(|e| e.to_string())?;
    let req: TraceRequest = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    // Refuse rather than guess. A caller on a different envelope may be sending
    // fields this build will silently ignore, and silently ignoring a field is
    // exactly how the last version constant stopped meaning anything.
    if req.wire_version != WIRE_VERSION {
        return Err(format!(
            "wire version mismatch: request {} vs engine {WIRE_VERSION}",
            req.wire_version
        ));
    }

    let mut stream = DecisionStream::new(req.seed);
    let mut results = Vec::with_capacity(req.ops.len());
    for op in &req.ops {
        let value = match op {
            Op::RawWords { n } => serde_json::json!(stream.raw_words(*n)),
            Op::BoundedInt { n } => serde_json::json!(stream.bounded_int(*n)),
            Op::WeightedIndex { weights } => serde_json::json!(stream.weighted_index(weights)),
            Op::PickTopK { scores, k } => {
                serde_json::json!(stream.pick_top_k(&decode_scalars(scores)?, *k))
            }
            Op::TopK { scores, k } => {
                serde_json::json!(top_k_indices(&decode_scalars(scores)?, *k))
            }
            Op::Argmax { scores } => serde_json::json!(argmax_index(&decode_scalars(scores)?)),
            Op::Argmin { values } => serde_json::json!(argmin_index(&decode_scalars(values)?)),
            Op::Entropy { counts } => encode_scalar(smath::entropy(counts)),
            Op::Log2 { values } => serde_json::Value::Array(
                decode_scalars(values)?.into_iter().map(|v| encode_scalar(v.log2())).collect(),
            ),
        };
        results.push(OpResult { value, draws: stream.draws });
    }

    emit(&TraceReply {
        wire_version: WIRE_VERSION,
        rng_version: rng::SOLVER_RNG_VERSION,
        draws: stream.draws,
        results,
    })
}

/// Read a problem from stdin and report the derived maps, without searching.
fn cmd_describe() -> Result<(), String> {
    let mut raw = String::new();
    io::stdin().read_to_string(&mut raw).map_err(|e| e.to_string())?;
    let enc: problem::EncodedProblem = serde_json::from_str(&raw).map_err(|e| e.to_string())?;
    if enc.wire_version != WIRE_VERSION {
        return Err(format!(
            "wire version mismatch: request {} vs engine {WIRE_VERSION}",
            enc.wire_version
        ));
    }
    let p = problem::Problem::load(&enc)?;

    // Sorted on the way out, every one of them. The maps are `det::Map`s, whose
    // iteration order is at least reproducible, but "reproducible" is not
    // "meaningful": a comparison against Python has to be against a stated
    // order or it is comparing two hash tables.
    fn pairs<V: Clone>(m: &crate::det::Map<u32, V>) -> Vec<(u32, V)> {
        let mut v: Vec<(u32, V)> = m.iter().map(|(&k, val)| (k, val.clone())).collect();
        v.sort_unstable_by_key(|(k, _)| *k);
        v
    }
    let mut inherent: Vec<u32> = p.inherent_parents.iter().copied().collect();
    inherent.sort_unstable();
    let _ = &p.endpoints;

    emit(&wire::DescribeReply {
        wire_version: WIRE_VERSION,
        no_path_possible: p.no_path_possible,
        max_distance: p.max_distance,
        relevant_transforms: p.relevant_transforms.clone(),
        free_transforms: p.free_transforms.clone(),
        dep_rank: {
            let mut v = pairs(&p.dep_rank);
            v.sort_unstable_by_key(|(_, r)| *r);
            v
        },
        distance: pairs(&p.distance),
        opportunity: pairs(&p.opportunity),
        demand2product: pairs(&p.demand2product),
        demand2producer: pairs(&p.demand2producer),
        product2consumer: pairs(&p.product2consumer),
        inherent_parents: inherent,
    })
}
