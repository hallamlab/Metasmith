//! Adjudicating a plan against the problem it claims to answer.
//!
//! This is the trusted computing base. The search is not: it may explore however
//! it likes, and a plan it returns is believed only because this predicate
//! accepts it. `docs/metasmith/solver-spec.md` is the prose and `lean/Spec.lean`
//! is the normative statement of what `check` is supposed to mean.
//!
//! Three constraints shape every line, and all three come from wanting the Lean
//! proof to be possible:
//!
//! - **No dependencies, no `String`, no `&'static str`, no `HashMap`.** Charon
//!   has to lower this whole crate. A `&'static str` is one of the shapes it
//!   cannot, which is why clause names live in `solver_witness_audit` instead.
//! - **No `&mut` parameter anywhere.** A `&mut` collection mutated inside a
//!   nested loop is the shape Aeneas cannot lower, and it is what put two
//!   functions of the previous version beyond reach. Sets are dense `Vec<bool>`
//!   moved through their loops instead; see `bits`.
//! - **Judgement only.** `check` answers yes or no and mutates nothing. Which
//!   clause failed, and where, is `solver_witness_audit`'s job -- a separate
//!   crate that depends on this one, so Charon cannot reach it and "never
//!   extracted" is structural rather than a flag someone can flip.
//!
//! Every clause is a pointwise predicate plus a mechanical quantifier, and the
//! audit crate calls those same pointwise predicates. There is one
//! implementation of each judgement, so the reporting cannot drift from the
//! thing that was proved.

#![forbid(unsafe_code)]

pub mod access;
pub mod bits;
pub mod clauses;
pub mod types;

pub use clauses::N_CLAUSES;
pub use types::*;

/// The ten conditions, numbered as `clause_holds` dispatches them. Fieldless and
/// method-free on purpose: a `name()` returning `&'static str` is one of the
/// shapes that does not survive extraction, so naming lives in the audit crate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Clause {
    Indexed = 0,
    Shape = 1,
    Conformance = 2,
    Emission = 3,
    Derived = 4,
    UniqueProducer = 5,
    Provenance = 6,
    Givens = 7,
    Schedulable = 8,
    Target = 9,
}

/// Adjudicate a plan against its problem.
///
/// Not a completeness claim: a rejection says this plan is wrong, never that no
/// plan exists.
///
/// There is deliberately no early return once a clause fails. Every accessor is
/// total, so a malformed plan produces a meaningless ancestry table and is
/// rejected by `Indexed` anyway -- and paying for that keeps the ten clauses
/// independent, which is what lets the proof be one equation with ten lemmas
/// rather than a chain where each rests on the last.
pub fn check(p: &Problem, q: &Plan) -> bool {
    let anc = clauses::ancestors(q);
    let mut ok = true;
    let mut k = 0;
    while k < N_CLAUSES {
        if !clauses::clause_holds(p, q, &anc, k) {
            ok = false;
        }
        k += 1;
    }
    ok
}
