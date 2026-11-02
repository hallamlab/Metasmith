//! Which clause a plan failed, and where. Never extracted.
//!
//! This crate contains **no judgement**. It supplies loops and names, and every
//! verdict it reports comes from calling `solver_witness`'s own pointwise
//! predicates. There is therefore one implementation of each condition, and a
//! report cannot disagree with the thing the proof is about.
//!
//! The remaining seam is that the quantifiers are written twice -- once as
//! `cl_*` in the witness, once as the loops below. `audit_agrees_with_check`
//! closes it by construction on every call in a debug build, and the corpus
//! tests close it on real plans.

use solver_witness as w;

pub fn name(c: w::Clause) -> &'static str {
    match c {
        w::Clause::Indexed => "indexed",
        w::Clause::Shape => "shape",
        w::Clause::Conformance => "conformance",
        w::Clause::Emission => "emission",
        w::Clause::Derived => "derived",
        w::Clause::UniqueProducer => "uniqueProducer",
        w::Clause::Provenance => "provenance",
        w::Clause::Givens => "givens",
        w::Clause::Schedulable => "schedulable",
        w::Clause::Target => "target",
    }
}

pub const NA: usize = usize::MAX;

/// A violated clause, located by integers. `step`, `slot` and `endpoint` are
/// `NA` when they do not apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Violation {
    pub clause: w::Clause,
    pub step: usize,
    pub slot: w::NodeId,
    pub endpoint: w::EpId,
}

impl Violation {
    fn bare(clause: w::Clause) -> Self {
        Violation { clause, step: NA, slot: NA, endpoint: NA }
    }
    fn at(clause: w::Clause, step: usize) -> Self {
        Violation { clause, step, slot: NA, endpoint: NA }
    }
    fn bind(clause: w::Clause, step: usize, slot: w::NodeId, endpoint: w::EpId) -> Self {
        Violation { clause, step, slot, endpoint }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Verdict {
    pub violations: Vec<Violation>,
}

impl Verdict {
    pub fn ok(&self) -> bool {
        self.violations.is_empty()
    }
    /// Whether this specific clause was violated. The decoy tests assert on this
    /// rather than on `ok`, because a decoy that trips the wrong clause proves
    /// nothing about the clause it was built for.
    pub fn violated(&self, c: w::Clause) -> bool {
        self.violations.iter().any(|v| v.clause == c)
    }
}

pub fn audit(p: &w::Problem, q: &w::Plan) -> Verdict {
    let anc = w::clauses::ancestors(q);
    let mut v = Verdict::default();

    // Indexed first and alone. Every position reported below is an index into
    // the reply, and locating a violation in a plan whose ids name nothing would
    // be reporting a coordinate in a space that does not exist.
    if !w::clauses::cl_indexed(p, q) {
        v.violations.push(Violation::bare(w::Clause::Indexed));
        return v;
    }

    for si in 0..q.steps.len() {
        if !w::clauses::shape_at(p, q, si) {
            v.violations.push(Violation::at(w::Clause::Shape, si));
        }
        for bi in 0..q.steps[si].used.len() {
            let (slot, ep) = q.steps[si].used[bi];
            if !w::clauses::conformance_at(p, q, &anc, si, bi) {
                v.violations.push(Violation::bind(w::Clause::Conformance, si, slot, ep));
            }
            if !w::clauses::provenance_at(q, si, bi) {
                v.violations.push(Violation::bind(w::Clause::Provenance, si, slot, ep));
            }
        }
        for gi in 0..q.steps[si].produced.len() {
            for ki in 0..q.steps[si].produced[gi].len() {
                let (slot, ep) = q.steps[si].produced[gi][ki];
                if !w::clauses::emission_at(p, q, &anc, si, gi, ki) {
                    v.violations.push(Violation::bind(w::Clause::Emission, si, slot, ep));
                }
                if !w::clauses::derived_at(q, si, gi, ki) {
                    v.violations.push(Violation::bind(w::Clause::Derived, si, slot, ep));
                }
            }
        }
    }

    if !w::clauses::cl_unique_producer(q) {
        for e in 0..q.endpoints.len() {
            for i in 0..q.steps.len() {
                for j in (i + 1)..q.steps.len() {
                    if !w::clauses::unique_producer_at(q, i, j, e) {
                        v.violations.push(Violation::bind(
                            w::Clause::UniqueProducer, i, NA, e));
                    }
                }
            }
        }
    }

    if !w::clauses::cl_givens(p, q) {
        let mut located = false;
        for gi in 0..q.givens.len() {
            if !w::clauses::givens_at(p, q, gi) {
                located = true;
                v.violations.push(Violation::bind(
                    w::Clause::Givens, NA, q.givens[gi].1, q.givens[gi].0));
            }
        }
        // The pairing being non-injective, or spanning two declared groups, is a
        // property of the whole set rather than of any one pair.
        if !located {
            v.violations.push(Violation::bare(w::Clause::Givens));
        }
    }

    for cj in 0..q.steps.len() {
        for bi in 0..q.steps[cj].used.len() {
            for pi in 0..q.steps.len() {
                if !w::clauses::schedulable_at(q, cj, bi, pi) {
                    let (slot, ep) = q.steps[cj].used[bi];
                    v.violations.push(Violation::bind(w::Clause::Schedulable, cj, slot, ep));
                }
            }
        }
    }

    if !w::clauses::cl_target(p, q) {
        v.violations.push(Violation::bare(w::Clause::Target));
    }

    debug_assert_eq!(v.ok(), w::check(p, q), "audit and check disagree");
    v
}

/// The seam between the two crates, as a predicate a test can call: the audit's
/// loops and the witness's quantifiers must agree on every plan.
pub fn audit_agrees_with_check(p: &w::Problem, q: &w::Plan) -> bool {
    audit(p, q).ok() == w::check(p, q)
}
