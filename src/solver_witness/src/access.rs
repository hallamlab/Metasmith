//! Every index in this crate goes through here, and every one of these is total.
//!
//! Two reasons. A panic is not a verdict: a checker that aborts on a malformed
//! reply has not rejected it. And the specification reads an out-of-range index
//! as the `Inhabited` default, so the two only agree if this side is total too.
//! Out of range yields `NONE` or a zero count, and `cl_indexed` is what turns
//! that into a rejection.
//!
//! **Every accessor returns a scalar, never a borrowed slice.** The obvious
//! shape -- `if d < len { &self.nodes[d].props } else { &[] }` -- does not
//! extract: Charon rejects a shared borrow whose provenance is a branch with
//! "There should be no bottoms in the value", and every function reaching one
//! comes out as a hole. A slice *parameter* is fine; a slice *return from a
//! conditional* is not. Counts and element lookups avoid the question entirely.

use crate::types::*;

pub const NONE: usize = usize::MAX;

pub fn n_nodes(p: &Problem) -> usize { p.nodes.len() }
pub fn n_transforms(p: &Problem) -> usize { p.transforms.len() }
pub fn n_endpoints(q: &Plan) -> usize { q.endpoints.len() }

pub fn node_nprops(p: &Problem, d: NodeId) -> usize {
    if d < p.nodes.len() { p.nodes[d].props.len() } else { 0 }
}

pub fn node_prop(p: &Problem, d: NodeId, i: usize) -> PropId {
    if d < p.nodes.len() && i < p.nodes[d].props.len() { p.nodes[d].props[i] } else { NONE }
}

pub fn node_nparents(p: &Problem, d: NodeId) -> usize {
    if d < p.nodes.len() { p.nodes[d].parents.len() } else { 0 }
}

pub fn node_parent(p: &Problem, d: NodeId, i: usize) -> NodeId {
    if d < p.nodes.len() && i < p.nodes[d].parents.len() { p.nodes[d].parents[i] } else { NONE }
}

pub fn ep_nprops(q: &Plan, e: EpId) -> usize {
    if e < q.endpoints.len() { q.endpoints[e].props.len() } else { 0 }
}

pub fn ep_prop(q: &Plan, e: EpId, i: usize) -> PropId {
    if e < q.endpoints.len() && i < q.endpoints[e].props.len() {
        q.endpoints[e].props[i]
    } else {
        NONE
    }
}

pub fn ep_nparents(q: &Plan, e: EpId) -> usize {
    if e < q.endpoints.len() { q.endpoints[e].parents.len() } else { 0 }
}

pub fn ep_parent(q: &Plan, e: EpId, i: usize) -> EpId {
    if e < q.endpoints.len() && i < q.endpoints[e].parents.len() {
        q.endpoints[e].parents[i]
    } else {
        NONE
    }
}

pub fn tr_nrequires(p: &Problem, t: TrId) -> usize {
    if t < p.transforms.len() { p.transforms[t].requires.len() } else { 0 }
}

pub fn tr_require(p: &Problem, t: TrId, i: usize) -> NodeId {
    if t < p.transforms.len() && i < p.transforms[t].requires.len() {
        p.transforms[t].requires[i]
    } else {
        NONE
    }
}

pub fn tr_ngroups(p: &Problem, t: TrId) -> usize {
    if t < p.transforms.len() { p.transforms[t].produces.len() } else { 0 }
}

pub fn tr_ngroup_slots(p: &Problem, t: TrId, g: usize) -> usize {
    if t < p.transforms.len() && g < p.transforms[t].produces.len() {
        p.transforms[t].produces[g].len()
    } else {
        0
    }
}

pub fn tr_group_slot(p: &Problem, t: TrId, g: usize, i: usize) -> NodeId {
    if t < p.transforms.len()
        && g < p.transforms[t].produces.len()
        && i < p.transforms[t].produces[g].len()
    {
        p.transforms[t].produces[g][i]
    } else {
        NONE
    }
}

/// What this step bound to slot `a`, or `NONE`. The first match, so no clause
/// needs `shape` to hold before a binding can be resolved.
pub fn bound_to(used: &[(NodeId, EpId)], a: NodeId) -> usize {
    let mut i = 0;
    while i < used.len() {
        if used[i].0 == a {
            return used[i].1;
        }
        i += 1;
    }
    NONE
}
