//! Handing the encoded problem and the encoded reply to the plan witness.
//!
//! This adapter is the residual trust in the whole arrangement, and it stays
//! trivial on purpose: field-for-field moves and nothing else. Every judgement
//! lives in `solver_witness`, which is the crate the Lean proof is about. If a
//! decision ever appears in this file, it has escaped the proof.
//!
//! It converts from the *wire* types rather than from the search's arenas, so
//! what gets adjudicated is the bytes that leave the process rather than an
//! in-memory structure that happens to agree with them.

use solver_witness as w;

use crate::problem::EncodedProblem;
use crate::reply::SolveReply;

pub fn problem_of(enc: &EncodedProblem) -> w::Problem {
    let mut nodes = Vec::with_capacity(enc.nodes.len());
    for n in &enc.nodes {
        nodes.push(w::Node { props: n.props.clone(), parents: n.parents.clone() });
    }
    let mut transforms = Vec::with_capacity(enc.transforms.len());
    for t in &enc.transforms {
        transforms.push(w::Transform {
            requires: t.requires.clone(),
            produces: t.produces.clone(),
        });
    }
    w::Problem {
        nodes,
        transforms,
        given_tr: enc.given_index,
        caller_trs: enc.caller_transforms.clone(),
        target_tr: enc.target_index,
        given: enc.given.clone(),
    }
}

pub fn plan_of(rep: &SolveReply) -> w::Plan {
    let mut endpoints = Vec::with_capacity(rep.endpoints.len());
    for e in &rep.endpoints {
        // `source_node` is deliberately dropped. It exists so the Python
        // decoder can hand back the caller's own object instead of an equal
        // copy, which is a decoding convenience and says nothing about whether
        // the plan is sound. The row still carries its own props and parents
        // either way -- a witness that trusted an empty row would pass
        // everything, so this is asserted in the tests rather than assumed.
        endpoints.push(w::Endpoint { props: e.props.clone(), parents: e.parents.clone() });
    }
    let mut steps = Vec::with_capacity(rep.steps.len());
    for s in &rep.steps {
        steps.push(w::Step {
            transform: s.transform,
            used: s.used.clone(),
            produced: s.produced.clone(),
        });
    }
    w::Plan { endpoints, steps, complete: rep.complete }
}

/// One line per violated clause, for stderr. The witness itself carries no
/// strings, so the formatting lives out here where it cannot reach the proof.
pub fn render(v: &w::Verdict) -> String {
    let mut out = String::new();
    for x in &v.violations {
        out.push_str(x.clause.name());
        if x.step != w::NA {
            out.push_str(&format!(" step={}", x.step));
        }
        if x.slot != w::NA {
            out.push_str(&format!(" slot={}", x.slot));
        }
        if x.endpoint != w::NA {
            out.push_str(&format!(" endpoint={}", x.endpoint));
        }
        out.push('\n');
    }
    out
}
