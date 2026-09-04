//! The ten conditions, each a pointwise predicate plus a mechanical quantifier.
//!
//! The split is what keeps the reporting wrapper honest. `solver_witness_audit`
//! calls these same pointwise predicates to find out *where* a plan failed, so
//! there is exactly one implementation of every judgement and the wrapper cannot
//! disagree with the thing that was proved.
//!
//! **Three shapes are avoided everywhere, and all three are extraction limits
//! rather than taste.** Charon and Aeneas reject each with a clear error, and a
//! function that hits one comes back as a hole while the Lean still compiles:
//!
//! - **No `&mut` parameter.** A `&mut` collection mutated inside a nested loop
//!   is the shape whose borrow context the loop fixed point cannot unify across
//!   two depths. Sets are dense `Vec<bool>` moved through their loops instead.
//! - **No `return` inside a nested loop** -- "Returns inside of nested loops are
//!   not supported yet". Every predicate below carries an `ok` flag in its loop
//!   conditions and returns once, at the end.
//! - **No `break` out of an outer loop** -- "Breaks to outer loops are not
//!   supported yet". Same flag does that job.
//!
//! The flag idiom also short-circuits, so it costs nothing at runtime: a loop
//! guarded by `ok &&` stops advancing the moment the answer is settled.

use crate::access::{self, NONE};
use crate::bits;
use crate::types::*;

pub const N_CLAUSES: usize = 10;

// ---------------------------------------------------------------------------
// ancestry: the reflexive-transitive closure of declared parents, BY INDEX
// ---------------------------------------------------------------------------

/// One dense row per endpoint. Parents precede their child, so one increasing
/// pass is the whole closure -- no worklist, no visited set, no recursion. A
/// parent that does not precede its child is skipped rather than chased, which
/// under-approximates ancestry and so fails closed; `cl_indexed` is what turns
/// that malformed ordering into a rejection.
pub fn ancestors(q: &Plan) -> Vec<Vec<bool>> {
    let n = access::n_endpoints(q);
    let mut rows: Vec<Vec<bool>> = Vec::new();
    let mut x = 0;
    while x < n {
        rows.push(anc_row(q, &rows, n, x));
        x += 1;
    }
    rows
}

fn anc_row(q: &Plan, rows: &[Vec<bool>], n: usize, x: usize) -> Vec<bool> {
    let mut acc = bits::zeros(n);
    acc = bits::set(acc, x);
    let k = access::ep_nparents(q, x);
    let mut j = 0;
    while j < k {
        let p = access::ep_parent(q, x, j);
        if p < rows.len() {
            acc = bits::union(acc, &rows[p]);
        }
        j += 1;
    }
    acc
}

fn descends(anc: &[Vec<bool>], e: EpId, f: EpId) -> bool {
    if e < anc.len() { bits::get(&anc[e], f) } else { false }
}

// ---------------------------------------------------------------------------
// satisfies: properties and lineage, against THIS step's bindings
// ---------------------------------------------------------------------------

fn carries(p: &Problem, q: &Plan, e: EpId, d: NodeId) -> bool {
    let nwant = access::node_nprops(p, d);
    let nhave = access::ep_nprops(q, e);
    let mut ok = true;
    let mut i = 0;
    while ok && i < nwant {
        let want = access::node_prop(p, d, i);
        let mut hit = false;
        let mut j = 0;
        while !hit && j < nhave {
            if access::ep_prop(q, e, j) == want {
                hit = true;
            }
            j += 1;
        }
        if !hit {
            ok = false;
        }
        i += 1;
    }
    ok
}

/// May endpoint `e` fill slot `d`, in a step whose bindings are `used`?
///
/// Each anchor resolves to the endpoint THIS step bound to it, and `e` must
/// descend from that very endpoint. Instancing moves the anchor from a type to
/// an instance; comparing by index is what makes it a particular file rather
/// than any file that looks like it.
pub fn satisfies(
    p: &Problem, q: &Plan, anc: &[Vec<bool>], used: &[(NodeId, EpId)], e: EpId, d: NodeId,
) -> bool {
    let mut ok = carries(p, q, e, d);
    let na = access::node_nparents(p, d);
    let mut i = 0;
    while ok && i < na {
        let f = access::bound_to(used, access::node_parent(p, d, i));
        if f == NONE || !descends(anc, e, f) {
            ok = false;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 0. indexed
// ---------------------------------------------------------------------------

pub fn cl_indexed(p: &Problem, q: &Plan) -> bool {
    let nn = access::n_nodes(p);
    let ne = access::n_endpoints(q);
    let nt = access::n_transforms(p);
    let mut ok = p.target_tr < nt && p.given_tr < nt;

    let mut i = 0;
    while ok && i < p.given.len() {
        if !given_group_indexed(p, i, nn) {
            ok = false;
        }
        i += 1;
    }
    i = 0;
    while ok && i < q.givens.len() {
        if q.givens[i].0 >= ne || q.givens[i].1 >= nn {
            ok = false;
        }
        i += 1;
    }

    // Parents precede their child, in both tables. This is what makes the
    // closure above one pass, and what makes the specification's relations well
    // founded. The encoder emits rows this way, so a violation is malformed.
    i = 0;
    while ok && i < nn {
        if !node_indexed(p, i) {
            ok = false;
        }
        i += 1;
    }
    i = 0;
    while ok && i < ne {
        if !endpoint_indexed(p, q, i) {
            ok = false;
        }
        i += 1;
    }

    // A transform must have unique requirements. Two structurally identical ones
    // intern to a single node id, and `shape` would then accept one binding for
    // two inputs.
    i = 0;
    while ok && i < nt {
        if !transform_indexed(p, i, nn) {
            ok = false;
        }
        i += 1;
    }

    i = 0;
    while ok && i < q.steps.len() {
        if !step_indexed(q, i, nn, ne, nt) {
            ok = false;
        }
        i += 1;
    }
    ok
}

/// One step's ids. Its own function so `cl_indexed`'s outer loop does not hold a
/// borrow of the step across the inner loops -- that is the borrow context
/// Aeneas could not match.
fn step_indexed(q: &Plan, si: usize, nn: usize, ne: usize, nt: usize) -> bool {
    let mut ok = q.steps[si].transform < nt;
    let mut j = 0;
    while ok && j < q.steps[si].used.len() {
        if q.steps[si].used[j].0 >= nn || q.steps[si].used[j].1 >= ne {
            ok = false;
        }
        j += 1;
    }
    j = 0;
    while ok && j < q.steps[si].produced.len() {
        if !group_indexed(q, si, j, nn, ne) {
            ok = false;
        }
        j += 1;
    }
    ok
}

fn group_indexed(q: &Plan, si: usize, gi: usize, nn: usize, ne: usize) -> bool {
    let mut ok = true;
    let mut k = 0;
    while ok && k < q.steps[si].produced[gi].len() {
        if q.steps[si].produced[gi][k].0 >= nn || q.steps[si].produced[gi][k].1 >= ne {
            ok = false;
        }
        k += 1;
    }
    ok
}

fn given_group_indexed(p: &Problem, g: usize, nn: usize) -> bool {
    let mut ok = true;
    let mut j = 0;
    while ok && j < p.given[g].len() {
        if p.given[g][j] >= nn {
            ok = false;
        }
        j += 1;
    }
    ok
}

/// One node's parents and properties. Parents must precede their child.
fn node_indexed(p: &Problem, d: NodeId) -> bool {
    let mut ok = true;
    let np = access::node_nparents(p, d);
    let mut j = 0;
    while ok && j < np {
        if access::node_parent(p, d, j) >= d {
            ok = false;
        }
        j += 1;
    }
    let nx = access::node_nprops(p, d);
    j = 0;
    while ok && j < nx {
        if access::node_prop(p, d, j) >= p.n_props {
            ok = false;
        }
        j += 1;
    }
    ok
}

/// One endpoint's parents and properties. Parents must precede their child --
/// this is what makes the ancestor closure one increasing pass.
fn endpoint_indexed(p: &Problem, q: &Plan, e: EpId) -> bool {
    let mut ok = true;
    let np = access::ep_nparents(q, e);
    let mut j = 0;
    while ok && j < np {
        if access::ep_parent(q, e, j) >= e {
            ok = false;
        }
        j += 1;
    }
    let nx = access::ep_nprops(q, e);
    j = 0;
    while ok && j < nx {
        if access::ep_prop(q, e, j) >= p.n_props {
            ok = false;
        }
        j += 1;
    }
    ok
}

/// One transform's ids, and its requirement uniqueness. A transform must have
/// unique requirements: two structurally identical ones intern to a single node
/// id, and `shape` would then accept one binding for two inputs.
fn transform_indexed(p: &Problem, t: TrId, nn: usize) -> bool {
    let mut ok = no_repeat(&required_slots(p, t));
    let nr = access::tr_nrequires(p, t);
    let mut j = 0;
    while ok && j < nr {
        if access::tr_require(p, t, j) >= nn {
            ok = false;
        }
        j += 1;
    }
    let ng = access::tr_ngroups(p, t);
    j = 0;
    while ok && j < ng {
        if !group_slots_indexed(p, t, j, nn) {
            ok = false;
        }
        j += 1;
    }
    ok
}

fn group_slots_indexed(p: &Problem, t: TrId, gi: usize, nn: usize) -> bool {
    let mut ok = true;
    let ns = access::tr_ngroup_slots(p, t, gi);
    let mut k = 0;
    while ok && k < ns {
        if access::tr_group_slot(p, t, gi, k) >= nn {
            ok = false;
        }
        k += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 1. shape
// ---------------------------------------------------------------------------

fn same_slots(a: &[NodeId], b: &[NodeId], nn: usize) -> bool {
    bits::eq(&bits::of_ids(nn, a), &bits::of_ids(nn, b))
}

/// Collect a step's bound slots. A `push` in a loop is fine; a `push` in a loop
/// that *inlines* another loop is not -- Aeneas cannot match the borrow contexts
/// across the two depths. Each inner loop is therefore its own function, which
/// is the shape the extraction spike proved.
fn used_slots(q: &Plan, si: usize) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut i = 0;
    while i < q.steps[si].used.len() {
        out.push(q.steps[si].used[i].0);
        i += 1;
    }
    out
}

fn produced_slots(q: &Plan, si: usize, gi: usize) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let mut i = 0;
    while i < q.steps[si].produced[gi].len() {
        out.push(q.steps[si].produced[gi][i].0);
        i += 1;
    }
    out
}

fn required_slots(p: &Problem, t: TrId) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let n = access::tr_nrequires(p, t);
    let mut i = 0;
    while i < n {
        out.push(access::tr_require(p, t, i));
        i += 1;
    }
    out
}

fn declared_slots(p: &Problem, t: TrId, gi: usize) -> Vec<NodeId> {
    let mut out: Vec<NodeId> = Vec::new();
    let n = access::tr_ngroup_slots(p, t, gi);
    let mut i = 0;
    while i < n {
        out.push(access::tr_group_slot(p, t, gi, i));
        i += 1;
    }
    out
}

fn no_repeat(a: &[NodeId]) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < a.len() {
        let mut j = i + 1;
        while ok && j < a.len() {
            if a[i] == a[j] {
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    ok
}

fn groups_match(p: &Problem, q: &Plan, si: usize, nn: usize) -> bool {
    let t = q.steps[si].transform;
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps[si].produced.len() {
        if !same_slots(&produced_slots(q, si, i), &declared_slots(p, t, i), nn) {
            ok = false;
        }
        i += 1;
    }
    ok
}

pub fn shape_at(p: &Problem, q: &Plan, si: usize) -> bool {
    let mut ok = true;
    if si < q.steps.len() {
        let t = q.steps[si].transform;
        let nn = access::n_nodes(p);
        let slots = used_slots(q, si);
        ok = no_repeat(&slots)
            && same_slots(&slots, &required_slots(p, t), nn)
            && q.steps[si].produced.len() == access::tr_ngroups(p, t)
            && groups_match(p, q, si, nn);
    }
    ok
}

pub fn cl_shape(p: &Problem, q: &Plan) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps.len() {
        if !shape_at(p, q, i) {
            ok = false;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 2. conformance, 3. emission, 4. derived
// ---------------------------------------------------------------------------

pub fn conformance_at(p: &Problem, q: &Plan, anc: &[Vec<bool>], si: usize, bi: usize) -> bool {
    let mut ok = true;
    if si < q.steps.len() && bi < q.steps[si].used.len() {
        let s = &q.steps[si];
        ok = satisfies(p, q, anc, &s.used, s.used[bi].1, s.used[bi].0);
    }
    ok
}

pub fn cl_conformance(p: &Problem, q: &Plan, anc: &[Vec<bool>]) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps.len() {
        let mut j = 0;
        while ok && j < q.steps[i].used.len() {
            if !conformance_at(p, q, anc, i, j) {
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    ok
}

pub fn emission_at(
    p: &Problem, q: &Plan, anc: &[Vec<bool>], si: usize, gi: usize, ki: usize,
) -> bool {
    let mut ok = true;
    if si < q.steps.len() && gi < q.steps[si].produced.len()
        && ki < q.steps[si].produced[gi].len()
    {
        let s = &q.steps[si];
        ok = satisfies(p, q, anc, &s.used, s.produced[gi][ki].1, s.produced[gi][ki].0);
    }
    ok
}

pub fn cl_emission(p: &Problem, q: &Plan, anc: &[Vec<bool>]) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps.len() {
        let mut g = 0;
        while ok && g < q.steps[i].produced.len() {
            let mut k = 0;
            while ok && k < q.steps[i].produced[g].len() {
                if !emission_at(p, q, anc, i, g, k) {
                    ok = false;
                }
                k += 1;
            }
            g += 1;
        }
        i += 1;
    }
    ok
}

/// What a step confers: everything it consumed, and those endpoints' own
/// parents. One hop, which is why ancestry above has to be transitive.
fn confers(q: &Plan, used: &[(NodeId, EpId)], ne: usize) -> Vec<bool> {
    let mut acc = bits::zeros(ne);
    let mut i = 0;
    while i < used.len() {
        let e = used[i].1;
        acc = bits::set(acc, e);
        let k = access::ep_nparents(q, e);
        let mut j = 0;
        while j < k {
            acc = bits::set(acc, access::ep_parent(q, e, j));
            j += 1;
        }
        i += 1;
    }
    acc
}

pub fn derived_at(q: &Plan, si: usize, gi: usize, ki: usize) -> bool {
    let mut ok = true;
    if si < q.steps.len() && gi < q.steps[si].produced.len()
        && ki < q.steps[si].produced[gi].len()
    {
        let ne = access::n_endpoints(q);
        let s = &q.steps[si];
        let e = s.produced[gi][ki].1;
        let mut declared = bits::zeros(ne);
        let k = access::ep_nparents(q, e);
        let mut j = 0;
        while j < k {
            declared = bits::set(declared, access::ep_parent(q, e, j));
            j += 1;
        }
        // Equality, not containment. A product free to declare extra parents can
        // buy any anchor it likes, which is a crossover written by hand.
        ok = bits::eq(&declared, &confers(q, &s.used, ne));
    }
    ok
}

pub fn cl_derived(q: &Plan) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps.len() {
        let mut g = 0;
        while ok && g < q.steps[i].produced.len() {
            let mut k = 0;
            while ok && k < q.steps[i].produced[g].len() {
                if !derived_at(q, i, g, k) {
                    ok = false;
                }
                k += 1;
            }
            g += 1;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 5. uniqueProducer, 6. provenance
// ---------------------------------------------------------------------------

fn emits(s: &Step, e: EpId) -> bool {
    let mut hit = false;
    let mut g = 0;
    while !hit && g < s.produced.len() {
        let mut k = 0;
        while !hit && k < s.produced[g].len() {
            if s.produced[g][k].1 == e {
                hit = true;
            }
            k += 1;
        }
        g += 1;
    }
    hit
}

/// Two steps emitting one endpoint is two processes writing one file.
pub fn unique_producer_at(q: &Plan, i: usize, j: usize, e: EpId) -> bool {
    let mut ok = true;
    if i < q.steps.len() && j < q.steps.len() && i != j {
        ok = !(emits(&q.steps[i], e) && emits(&q.steps[j], e));
    }
    ok
}

pub fn cl_unique_producer(q: &Plan) -> bool {
    let ne = access::n_endpoints(q);
    let mut ok = true;
    let mut e = 0;
    while ok && e < ne {
        let mut seen = NONE;
        let mut i = 0;
        while ok && i < q.steps.len() {
            if emits(&q.steps[i], e) {
                if seen != NONE {
                    ok = false;
                }
                seen = i;
            }
            i += 1;
        }
        e += 1;
    }
    ok
}

fn is_given(q: &Plan, e: EpId) -> bool {
    let mut hit = false;
    let mut i = 0;
    while !hit && i < q.givens.len() {
        if q.givens[i].0 == e {
            hit = true;
        }
        i += 1;
    }
    hit
}

pub fn provenance_at(q: &Plan, si: usize, bi: usize) -> bool {
    let mut ok = true;
    if si < q.steps.len() && bi < q.steps[si].used.len() {
        let e = q.steps[si].used[bi].1;
        let mut found = is_given(q, e);
        let mut i = 0;
        while !found && i < q.steps.len() {
            if emits(&q.steps[i], e) {
                found = true;
            }
            i += 1;
        }
        ok = found;
    }
    ok
}

pub fn cl_provenance(q: &Plan) -> bool {
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.steps.len() {
        let mut j = 0;
        while ok && j < q.steps[i].used.len() {
            if !provenance_at(q, i, j) {
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 7. givens
// ---------------------------------------------------------------------------

fn same_props(p: &Problem, q: &Plan, n: NodeId, e: EpId) -> bool {
    let mut a = bits::zeros(p.n_props);
    let na = access::node_nprops(p, n);
    let mut i = 0;
    while i < na {
        a = bits::set(a, access::node_prop(p, n, i));
        i += 1;
    }
    let mut b = bits::zeros(p.n_props);
    let nb = access::ep_nprops(q, e);
    i = 0;
    while i < nb {
        b = bits::set(b, access::ep_prop(q, e, i));
        i += 1;
    }
    bits::eq(&a, &b)
}

/// The node this pairing gives for endpoint `f`, or `NONE`.
fn given_node_of(q: &Plan, f: EpId) -> usize {
    let mut out = NONE;
    let mut j = 0;
    while out == NONE && j < q.givens.len() {
        if q.givens[j].0 == f {
            out = q.givens[j].1;
        }
        j += 1;
    }
    out
}

/// The endpoint this pairing gives for node `a`, or `NONE`.
fn given_ep_of(q: &Plan, a: NodeId) -> usize {
    let mut out = NONE;
    let mut j = 0;
    while out == NONE && j < q.givens.len() {
        if q.givens[j].1 == a {
            out = q.givens[j].0;
        }
        j += 1;
    }
    out
}

fn node_has_parent(p: &Problem, n: NodeId, a: NodeId) -> bool {
    let mut hit = false;
    let k = access::node_nparents(p, n);
    let mut i = 0;
    while !hit && i < k {
        if access::node_parent(p, n, i) == a {
            hit = true;
        }
        i += 1;
    }
    hit
}

fn ep_has_parent(q: &Plan, e: EpId, f: EpId) -> bool {
    let mut hit = false;
    let k = access::ep_nparents(q, e);
    let mut i = 0;
    while !hit && i < k {
        if access::ep_parent(q, e, i) == f {
            hit = true;
        }
        i += 1;
    }
    hit
}

/// The pairing is a correspondence, not a claim: equal properties, and a lineage
/// edge on one side exactly when there is one on the other. The declared givens
/// are closed under parents, so no edge dangles.
pub fn givens_at(p: &Problem, q: &Plan, gi: usize) -> bool {
    let mut ok = true;
    if gi < q.givens.len() {
        let e = q.givens[gi].0;
        let n = q.givens[gi].1;
        ok = same_props(p, q, n, e);
        let neps = access::ep_nparents(q, e);
        let mut i = 0;
        while ok && i < neps {
            let f = access::ep_parent(q, e, i);
            let a = given_node_of(q, f);
            if a == NONE || !node_has_parent(p, n, a) {
                ok = false;
            }
            i += 1;
        }
        let nnds = access::node_nparents(p, n);
        i = 0;
        while ok && i < nnds {
            let a = access::node_parent(p, n, i);
            let f = given_ep_of(q, a);
            if f == NONE || !ep_has_parent(q, e, f) {
                ok = false;
            }
            i += 1;
        }
    }
    ok
}

/// Is `n` a given the problem declared, in any group?
fn is_declared_given(p: &Problem, n: NodeId) -> bool {
    let mut hit = false;
    let mut g = 0;
    while !hit && g < p.given.len() {
        let mut m = 0;
        while !hit && m < p.given[g].len() {
            if p.given[g][m] == n {
                hit = true;
            }
            m += 1;
        }
        g += 1;
    }
    hit
}

/// The givens a plan presents are ones the problem declared, paired one to one,
/// and each pairing is a real correspondence rather than a claim.
///
/// **There is deliberately no condition confining a plan to one declared
/// group.** The groups are one per sample, and a plan legitimately spans all of
/// them: a multi-sample workflow processes every sample, and `test_solver`'s
/// multi-given cases build exactly that. What keeps one step from mixing two
/// samples is the lineage anchors in `conformance`, not group membership -- so a
/// problem whose transforms declare no anchors has not asked for the samples to
/// be kept apart, and a plan that interleaves them violates nothing.
pub fn cl_givens(p: &Problem, q: &Plan) -> bool {
    // Injective on both sides, so the pairing is a correspondence rather than a
    // many-to-one "matches something".
    let mut ok = true;
    let mut i = 0;
    while ok && i < q.givens.len() {
        let mut j = i + 1;
        while ok && j < q.givens.len() {
            if q.givens[i].0 == q.givens[j].0 || q.givens[i].1 == q.givens[j].1 {
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    i = 0;
    while ok && i < q.givens.len() {
        if !is_declared_given(p, q.givens[i].1) || !givens_at(p, q, i) {
            ok = false;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// 8. schedulable, 9. target
// ---------------------------------------------------------------------------

/// A list that is a topological order is itself the proof that the step graph is
/// acyclic, so this replaces a separate cycle check. Declared LINEAGE may still
/// contain cycles, and does; this is about the step graph only.
pub fn schedulable_at(q: &Plan, cj: usize, bi: usize, pi: usize) -> bool {
    let mut ok = true;
    if cj < q.steps.len() && pi < q.steps.len() && bi < q.steps[cj].used.len()
        && emits(&q.steps[pi], q.steps[cj].used[bi].1)
    {
        ok = pi < cj;
    }
    ok
}

pub fn cl_schedulable(q: &Plan) -> bool {
    let mut ok = true;
    let mut j = 0;
    while ok && j < q.steps.len() {
        let mut b = 0;
        while ok && b < q.steps[j].used.len() {
            let mut i = 0;
            while ok && i < q.steps.len() {
                if !schedulable_at(q, j, b, i) {
                    ok = false;
                }
                i += 1;
            }
            b += 1;
        }
        j += 1;
    }
    ok
}

/// Exactly one application of the target. This also rules out the empty plan,
/// which is why there is no separate `nonempty` clause.
pub fn cl_target(p: &Problem, q: &Plan) -> bool {
    let mut n = 0;
    let mut i = 0;
    while i < q.steps.len() {
        if q.steps[i].transform == p.target_tr {
            n += 1;
        }
        i += 1;
    }
    n == 1
}

// ---------------------------------------------------------------------------

pub fn clause_holds(p: &Problem, q: &Plan, anc: &[Vec<bool>], k: usize) -> bool {
    if k == 0 {
        cl_indexed(p, q)
    } else if k == 1 {
        cl_shape(p, q)
    } else if k == 2 {
        cl_conformance(p, q, anc)
    } else if k == 3 {
        cl_emission(p, q, anc)
    } else if k == 4 {
        cl_derived(q)
    } else if k == 5 {
        cl_unique_producer(q)
    } else if k == 6 {
        cl_provenance(q)
    } else if k == 7 {
        cl_givens(p, q)
    } else if k == 8 {
        cl_schedulable(q)
    } else if k == 9 {
        cl_target(p, q)
    } else {
        true
    }
}
