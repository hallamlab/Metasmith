//! The wire, as the witness reads it.
//!
//! These mirror the encoded request and reply, so what is adjudicated is the
//! bytes that leave the process rather than an in-memory structure that happens
//! to agree with them.
//!
//! **An endpoint's identity is its index, and that is the only way this crate
//! compares two endpoints.** Nothing here identifies two rows by their contents.
//! Two files made by different steps are two files even at the same type and the
//! same lineage, and a structural comparison cannot say so -- which is what let a
//! step anchor to one file and consume something derived from another.
//!
//! The givens are a **parameter**, not a step. `Plan::givens` pairs each endpoint
//! the plan presents with the node the problem declared it as; the adapter
//! supplies the pairing from `source_node` and the `givens` clause checks it
//! rather than trusting it.

pub type PropId = usize;
/// Index into `Problem::nodes`. The table is overloaded: a node is a slot on a
/// transform, and it is also the declared type of a given input.
pub type NodeId = usize;
/// Index into `Plan::endpoints`. Identity.
pub type EpId = usize;
pub type TrId = usize;

// No `derive` on anything below. `Debug` on a struct holding `Vec<(NodeId, EpId)>`
// makes Charon emit the tuple's `Debug` impl, which it cannot lower -- so the
// only hole left in an otherwise complete extraction was a formatter nothing
// here calls. Deriving nothing is free: the judgement reads fields, and every
// name and message lives in `solver_witness_audit`.

pub struct Node {
    pub props: Vec<PropId>,
    /// On a `requires` slot these are ANCHORS -- other slots of the same
    /// transform whose bindings this one must descend from. On a given they are
    /// that input's own lineage, belonging to no transform.
    pub parents: Vec<NodeId>,
}

pub struct Transform {
    pub requires: Vec<NodeId>,
    pub produces: Vec<Vec<NodeId>>,
}

pub struct Problem {
    pub n_props: usize,
    pub nodes: Vec<Node>,
    pub transforms: Vec<Transform>,
    /// One group per sample. The groups are alternatives, and a plan drawing
    /// from two of them mixes samples, so they are kept rather than flattened.
    pub given: Vec<Vec<NodeId>>,
    pub given_tr: TrId,
    pub target_tr: TrId,
}

pub struct Endpoint {
    pub props: Vec<PropId>,
    pub parents: Vec<EpId>,
}

pub struct Step {
    pub transform: TrId,
    pub used: Vec<(NodeId, EpId)>,
    pub produced: Vec<Vec<(NodeId, EpId)>>,
}

pub struct Plan {
    pub endpoints: Vec<Endpoint>,
    /// `(endpoint, the node it is)`. The given step is not in `steps`.
    pub givens: Vec<(EpId, NodeId)>,
    pub steps: Vec<Step>,
}
