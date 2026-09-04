//! Handing the encoded problem and the encoded reply to the plan witness.
//!
//! This adapter is the residual trust in the whole arrangement, and it stays
//! mechanical on purpose. Every judgement lives in `solver_witness`, which is
//! the crate the Lean proof is about. If a decision ever appears in this file,
//! it has escaped the proof.
//!
//! It converts from the *wire* types rather than from the search's arenas, so
//! what gets adjudicated is the bytes that leave the process.
//!
//! **The one structural move it makes is stripping the given step.** The
//! specification has no such step: the givens are a parameter, and a plan is the
//! transforms the search chose. Leaving the step in forced four exemptions and a
//! boundary conjunct, because after a timeline merge it emits one sample's
//! endpoint under another sample's slot and the property subset genuinely does
//! not hold there. Stripping it dissolves all of that.
//!
//! Stripping is also the one thing here that can be wrong, so it may **fail**
//! rather than guess: a zero-requirement transform and the given transform are
//! shape-identical on the wire, and the only thing that tells them apart is
//! `given_index`. A reply without exactly one such step is malformed, and saying
//! so is a different answer from calling the plan unsound.

use solver_witness as w;
use solver_witness_audit as audit;

use crate::problem::EncodedProblem;
use crate::reply::SolveReply;

pub fn problem_of(enc: &EncodedProblem) -> w::Problem {
    let mut nodes = Vec::with_capacity(enc.nodes.len());
    for n in &enc.nodes {
        nodes.push(w::Node {
            props: n.props.iter().map(|&x| x as usize).collect(),
            parents: n.parents.iter().map(|&x| x as usize).collect(),
        });
    }
    let mut transforms = Vec::with_capacity(enc.transforms.len());
    for t in &enc.transforms {
        transforms.push(w::Transform {
            requires: t.requires.iter().map(|&x| x as usize).collect(),
            produces: t
                .produces
                .iter()
                .map(|g| g.iter().map(|&x| x as usize).collect())
                .collect(),
        });
    }
    w::Problem {
        n_props: enc.n_properties as usize,
        nodes,
        transforms,
        // The groups are kept. They are per-sample alternatives, and flattening
        // them lets a plan draw one input from one sample and another from
        // another -- a crossover no other clause catches.
        given: enc
            .given
            .iter()
            .map(|g| g.iter().map(|&x| x as usize).collect())
            .collect(),
        given_tr: enc.given_index as usize,
        target_tr: enc.target_index as usize,
    }
}

pub fn plan_of(enc: &EncodedProblem, rep: &SolveReply) -> Result<w::Plan, String> {
    let given_tr = enc.given_index;
    let mut given_steps = 0;
    for s in &rep.steps {
        if s.transform == given_tr {
            given_steps += 1;
        }
    }
    if given_steps != 1 {
        return Err(format!(
            "the reply carries {given_steps} steps applying the given transform, not 1; \
             the specification has no given step and the adapter cannot tell which to strip"
        ));
    }

    let mut endpoints = Vec::with_capacity(rep.endpoints.len());
    for e in &rep.endpoints {
        endpoints.push(w::Endpoint {
            props: e.props.iter().map(|&x| x as usize).collect(),
            parents: e.parents.iter().map(|&x| x as usize).collect(),
        });
    }

    let mut givens: Vec<(usize, usize)> = Vec::new();
    let mut steps = Vec::new();
    for s in &rep.steps {
        if s.transform == given_tr {
            for g in &s.produced {
                for &(_, e) in g {
                    // `source_node` is the pairing, and the `givens` clause checks
                    // it rather than trusting it. A given without one is a reply
                    // that cannot say which declared input it is presenting.
                    match rep.endpoints[e as usize].source_node {
                        // Deduplicated. Two samples may share a structurally
                        // identical given -- two `read_metadata` endpoints with
                        // no lineage intern to one node -- and the given step
                        // then presents that one pair once per group. That is one
                        // presented given, and counting it twice reads to the
                        // `givens` clause as a pairing that is not injective.
                        Some(n) => {
                            let pair = (e as usize, n as usize);
                            if !givens.contains(&pair) {
                                givens.push(pair);
                            }
                        }
                        None => {
                            return Err(format!(
                                "given endpoint {e} names no source node; the reply cannot \
                                 say which declared input it presents"
                            ))
                        }
                    }
                }
            }
            continue;
        }
        steps.push(w::Step {
            transform: s.transform as usize,
            used: s.used.iter().map(|&(d, e)| (d as usize, e as usize)).collect(),
            produced: s
                .produced
                .iter()
                .map(|g| g.iter().map(|&(d, e)| (d as usize, e as usize)).collect())
                .collect(),
        });
    }

    Ok(w::Plan { endpoints, givens, steps })
}

/// One line per violated clause, for stderr. The witness carries no strings, so
/// formatting lives out here where it cannot reach the proof.
pub fn render(v: &audit::Verdict) -> String {
    let mut out = String::new();
    for x in &v.violations {
        out.push_str(audit::name(x.clause));
        if x.step != audit::NA {
            out.push_str(&format!(" step={}", x.step));
        }
        if x.slot != audit::NA {
            out.push_str(&format!(" slot={}", x.slot));
        }
        if x.endpoint != audit::NA {
            out.push_str(&format!(" endpoint={}", x.endpoint));
        }
        out.push('\n');
    }
    out
}
