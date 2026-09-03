//! Adjudicating a plan against the problem it claims to answer.
//!
//! This is the trusted computing base. The search is not: it may explore however
//! it likes, and a plan it returns is believed only because this predicate
//! accepts it. `docs/metasmith/solver-spec.md` is the prose, and `Spec.lean` is
//! the normative statement of what `check` is supposed to mean.
//!
//! Three constraints shape every line here, and all three come from wanting the
//! Lean proof to be possible:
//!
//! - **No dependencies, no `String`, no `HashMap`.** Charon has to lower this
//!   whole crate, and Aeneas has no model of a hash map worth relying on.
//!   Violations are integers naming a clause and a position, and formatting them
//!   is the caller's job.
//! - **Arrays indexed by id, never a map.** `scratch.rs` on the solver side
//!   already works this way, for the same reason.
//! - **Explicit loops.** Iterator chains lower badly and read worse in the
//!   extracted output.
//!
//! The types mirror the wire format exactly. The witness reads the encoded
//! request and reply rather than the solver's arenas, so what it adjudicates is
//! the bytes that leave the process.

#![forbid(unsafe_code)]

pub type PropId = u32;
/// Index into `Problem::nodes`. A slot on a transform, or the declared type of
/// a given input -- the node table serves both roles.
pub type NodeId = u32;
/// Index into `Plan::endpoints`. This is *identity*, not structure: two
/// endpoints with the same properties and lineage are two endpoints when the
/// solver built two.
pub type EpId = u32;
/// Index into `Problem::transforms`.
pub type TrId = u32;

#[derive(Debug, Clone)]
pub struct Node {
    pub props: Vec<PropId>,
    /// Lineage anchors. For a `requires` slot these are earlier slots of the
    /// same transform; for a given node they are that endpoint's real lineage
    /// and belong to no transform at all.
    pub parents: Vec<NodeId>,
}

#[derive(Debug, Clone)]
pub struct Transform {
    pub requires: Vec<NodeId>,
    /// One inner list per product group. For the given transform these groups
    /// are *alternatives*, one per sample; for every other transform they are
    /// conjunctive and all of them are emitted.
    pub produces: Vec<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct Problem {
    pub nodes: Vec<Node>,
    pub transforms: Vec<Transform>,
    pub given_tr: TrId,
    pub caller_trs: Vec<TrId>,
    pub target_tr: TrId,
    pub given: Vec<Vec<NodeId>>,
}

#[derive(Debug, Clone)]
pub struct Endpoint {
    pub props: Vec<PropId>,
    pub parents: Vec<EpId>,
}

#[derive(Debug, Clone)]
pub struct Step {
    pub transform: TrId,
    /// Which endpoint filled which input slot.
    pub used: Vec<(NodeId, EpId)>,
    /// Which endpoint left which output slot, grouped by product group.
    pub produced: Vec<Vec<(NodeId, EpId)>>,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub endpoints: Vec<Endpoint>,
    pub steps: Vec<Step>,
    /// Whether the search finished. An incomplete plan is a search that gave
    /// up, which is a different answer from an unsound one.
    pub complete: bool,
}

/// The ten conditions of `Sound`. Numbered as in the specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clause {
    Indexed,
    Nonempty,
    Shape,
    Provenance,
    Conformance,
    Emission,
    Rooted,
    Givens,
    Schedulable,
    Boundary,
}

impl Clause {
    pub fn name(self) -> &'static str {
        match self {
            Clause::Indexed => "indexed",
            Clause::Nonempty => "nonempty",
            Clause::Shape => "shape",
            Clause::Provenance => "provenance",
            Clause::Conformance => "conformance",
            Clause::Emission => "emission",
            Clause::Rooted => "rooted",
            Clause::Givens => "givens",
            Clause::Schedulable => "schedulable",
            Clause::Boundary => "boundary",
        }
    }
}

/// A violated clause, located by integers. `step`, `slot` and `endpoint` are
/// `u32::MAX` when they do not apply.
#[derive(Debug, Clone, Copy)]
pub struct Violation {
    pub clause: Clause,
    pub step: u32,
    pub slot: NodeId,
    pub endpoint: EpId,
}

pub const NA: u32 = u32::MAX;

impl Violation {
    fn at(clause: Clause, step: usize) -> Self {
        Violation { clause, step: step as u32, slot: NA, endpoint: NA }
    }
    fn bind(clause: Clause, step: usize, slot: NodeId, endpoint: EpId) -> Self {
        Violation { clause, step: step as u32, slot, endpoint }
    }
    fn bare(clause: Clause) -> Self {
        Violation { clause, step: NA, slot: NA, endpoint: NA }
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
    /// Whether this specific clause was violated. The mutation harness asserts
    /// on this rather than on `ok`, because a decoy that trips the wrong clause
    /// proves nothing about the clause it was built for.
    pub fn violated(&self, c: Clause) -> bool {
        let mut i = 0;
        while i < self.violations.len() {
            if self.violations[i].clause == c {
                return true;
            }
            i += 1;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// clause 0: every id names something that exists
// ---------------------------------------------------------------------------

fn check_indexed(p: &Problem, q: &Plan, v: &mut Verdict) -> bool {
    let nn = p.nodes.len() as u32;
    let nt = p.transforms.len() as u32;
    let ne = q.endpoints.len() as u32;
    let mut ok = true;

    if p.given_tr >= nt || p.target_tr >= nt {
        v.violations.push(Violation::bare(Clause::Indexed));
        // Nothing below can be evaluated without these two.
        return false;
    }
    let mut i = 0;
    while i < p.nodes.len() {
        let mut j = 0;
        while j < p.nodes[i].parents.len() {
            if p.nodes[i].parents[j] >= nn {
                v.violations.push(Violation::bind(Clause::Indexed, NA as usize, i as u32, NA));
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    i = 0;
    while i < p.transforms.len() {
        let t = &p.transforms[i];
        let mut j = 0;
        while j < t.requires.len() {
            if t.requires[j] >= nn {
                v.violations.push(Violation::at(Clause::Indexed, i));
                ok = false;
            }
            j += 1;
        }
        j = 0;
        while j < t.produces.len() {
            let mut k = 0;
            while k < t.produces[j].len() {
                if t.produces[j][k] >= nn {
                    v.violations.push(Violation::at(Clause::Indexed, i));
                    ok = false;
                }
                k += 1;
            }
            j += 1;
        }
        i += 1;
    }
    i = 0;
    while i < q.endpoints.len() {
        let mut j = 0;
        while j < q.endpoints[i].parents.len() {
            if q.endpoints[i].parents[j] >= ne {
                v.violations.push(Violation::bind(Clause::Indexed, NA as usize, NA, i as u32));
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    i = 0;
    while i < p.given.len() {
        let mut j = 0;
        while j < p.given[i].len() {
            if p.given[i][j] >= nn {
                v.violations.push(Violation::bare(Clause::Indexed));
                ok = false;
            }
            j += 1;
        }
        i += 1;
    }
    i = 0;
    while i < q.steps.len() {
        let s = &q.steps[i];
        if s.transform >= nt {
            v.violations.push(Violation::at(Clause::Indexed, i));
            ok = false;
        }
        let mut j = 0;
        while j < s.used.len() {
            if s.used[j].0 >= nn || s.used[j].1 >= ne {
                v.violations.push(Violation::bind(Clause::Indexed, i, s.used[j].0, s.used[j].1));
                ok = false;
            }
            j += 1;
        }
        j = 0;
        while j < s.produced.len() {
            let mut k = 0;
            while k < s.produced[j].len() {
                let b = s.produced[j][k];
                if b.0 >= nn || b.1 >= ne {
                    v.violations.push(Violation::bind(Clause::Indexed, i, b.0, b.1));
                    ok = false;
                }
                k += 1;
            }
            j += 1;
        }
        i += 1;
    }
    ok
}

// ---------------------------------------------------------------------------
// ancestry: the transitive closure of an endpoint's DECLARED parents
// ---------------------------------------------------------------------------

/// Canonical structural class per endpoint: equal classes mean equal
/// properties and equal lineage, all the way down.
///
/// **Lineage compares endpoints by structure, not by arena position, and this
/// is the one place identity is the wrong answer.** The solver legitimately
/// emits structural twins -- one endpoint carried over from the caller's own
/// objects and one minted during the search, with identical properties and
/// identical parents. A produced endpoint then records one twin in its lineage
/// while its step consumed the other. Comparing positions rejects seven of the
/// eleven shipped templates; comparing structure accepts exactly what
/// `check_plan` accepts, whose `is_ancestor` has always tested
/// `p is a or p == a` with `==` being signature equality.
///
/// Identity still decides *provenance*: an endpoint must be emitted by some
/// step, and a structurally equal one elsewhere does not discharge that.
///
/// Returns `None` when some endpoint names a parent at or after its own index.
/// Reply rows are emitted parents-first, so this is a malformed reply rather
/// than a case to handle, and asserting it is cheaper than a fixpoint.
fn structural_classes(q: &Plan) -> Option<Vec<u32>> {
    let n = q.endpoints.len();
    let mut class = vec![0u32; n];
    // Representative key per class, in class order.
    let mut keys: Vec<(Vec<PropId>, Vec<u32>)> = Vec::new();
    let mut i = 0;
    while i < n {
        let e = &q.endpoints[i];
        let mut props = e.props.clone();
        props.sort_unstable();
        props.dedup();
        let mut pcls: Vec<u32> = Vec::with_capacity(e.parents.len());
        let mut j = 0;
        while j < e.parents.len() {
            let pa = e.parents[j] as usize;
            if pa >= i {
                return None;
            }
            pcls.push(class[pa]);
            j += 1;
        }
        pcls.sort_unstable();
        pcls.dedup();
        let mut found = u32::MAX;
        let mut k = 0;
        while k < keys.len() {
            if keys[k].0 == props && keys[k].1 == pcls {
                found = k as u32;
                break;
            }
            k += 1;
        }
        if found == u32::MAX {
            found = keys.len() as u32;
            keys.push((props, pcls));
        }
        class[i] = found;
        i += 1;
    }
    Some(class)
}

/// Whether `e` descends from `f`, comparing by structural class.
///
/// Strict: an endpoint is not its own ancestor unless it genuinely names itself
/// or a twin. Callers wanting the reflexive relation test the classes for
/// equality first, which is what the lineage clause does.
///
/// Iterative, with an explicit stack, because the declared-parent graph is
/// acyclic only by construction and a recursive walk would not terminate on a
/// malformed reply. `seen` bounds the work at one visit per endpoint.
fn descends_from(q: &Plan, class: &[u32], e: EpId, f: EpId) -> bool {
    let n = q.endpoints.len();
    let target = class[f as usize];
    let mut seen = vec![false; n];
    let mut stack: Vec<EpId> = Vec::new();
    stack.push(e);
    seen[e as usize] = true;
    while let Some(x) = stack.pop() {
        let ps = &q.endpoints[x as usize].parents;
        let mut i = 0;
        while i < ps.len() {
            let pa = ps[i];
            if class[pa as usize] == target {
                return true;
            }
            if !seen[pa as usize] {
                seen[pa as usize] = true;
                stack.push(pa);
            }
            i += 1;
        }
    }
    false
}

// ---------------------------------------------------------------------------
// Fills: properties and lineage, together, against THIS step's bindings
// ---------------------------------------------------------------------------

fn carries(node: &Node, ep: &Endpoint) -> bool {
    let mut i = 0;
    while i < node.props.len() {
        let want = node.props[i];
        let mut hit = false;
        let mut j = 0;
        while j < ep.props.len() {
            if ep.props[j] == want {
                hit = true;
                break;
            }
            j += 1;
        }
        if !hit {
            return false;
        }
        i += 1;
    }
    true
}

/// May endpoint `e` fill slot `d`, in a step whose bindings are `used`?
fn fills(
    p: &Problem, q: &Plan, class: &[u32], used: &[(NodeId, EpId)], e: EpId, d: NodeId,
) -> bool {
    let node = &p.nodes[d as usize];
    if !carries(node, &q.endpoints[e as usize]) {
        return false;
    }
    let mut i = 0;
    while i < node.parents.len() {
        let anchor = node.parents[i];
        // The anchor must be bound in THIS step. Without that, "descends from
        // an endpoint that satisfies the anchor" lets one sample's data fill a
        // slot anchored to another sample's.
        let mut bound = false;
        let mut j = 0;
        while j < used.len() {
            if used[j].0 == anchor {
                let f = used[j].1;
                // Reflexive on the class, not on the index: `Lin`'s bound is
                // `i <= n`, and check_plan short-circuits the same way.
                if class[f as usize] == class[e as usize] || descends_from(q, class, e, f) {
                    bound = true;
                    break;
                }
            }
            j += 1;
        }
        if !bound {
            return false;
        }
        i += 1;
    }
    true
}

// ---------------------------------------------------------------------------
// clause 2: shape
// ---------------------------------------------------------------------------

fn same_slot_set(a: &[NodeId], b: &[NodeId], n_nodes: usize) -> bool {
    // Sets, not positions. A transform may legally name the same slot twice in
    // `requires`, and after a merge a step's product groups are not in
    // declaration order.
    let mut in_a = vec![false; n_nodes];
    let mut in_b = vec![false; n_nodes];
    let mut i = 0;
    while i < a.len() {
        in_a[a[i] as usize] = true;
        i += 1;
    }
    i = 0;
    while i < b.len() {
        in_b[b[i] as usize] = true;
        i += 1;
    }
    i = 0;
    while i < n_nodes {
        if in_a[i] != in_b[i] {
            return false;
        }
        i += 1;
    }
    true
}

fn slots_of(bs: &[(NodeId, EpId)]) -> Vec<NodeId> {
    let mut out = Vec::with_capacity(bs.len());
    let mut i = 0;
    while i < bs.len() {
        out.push(bs[i].0);
        i += 1;
    }
    out
}

fn check_shape(p: &Problem, q: &Plan, v: &mut Verdict) {
    let n_nodes = p.nodes.len();
    let mut i = 0;
    while i < q.steps.len() {
        let s = &q.steps[i];
        let t = &p.transforms[s.transform as usize];

        // no slot bound twice
        let mut j = 0;
        while j < s.used.len() {
            let mut k = j + 1;
            while k < s.used.len() {
                if s.used[j].0 == s.used[k].0 {
                    v.violations.push(Violation::bind(Clause::Shape, i, s.used[j].0, NA));
                }
                k += 1;
            }
            j += 1;
        }

        if !same_slot_set(&slots_of(&s.used), &t.requires, n_nodes) {
            v.violations.push(Violation::at(Clause::Shape, i));
        }

        if s.transform == p.given_tr {
            // Alternatives: branching hands the given step one group per
            // timeline, so it emits fewer groups than it declares. Each group
            // it does emit must match some declared group.
            let mut g = 0;
            while g < s.produced.len() {
                let got = slots_of(&s.produced[g]);
                let mut matched = false;
                let mut d = 0;
                while d < t.produces.len() {
                    if same_slot_set(&got, &t.produces[d], n_nodes) {
                        matched = true;
                        break;
                    }
                    d += 1;
                }
                if !matched {
                    v.violations.push(Violation::at(Clause::Shape, i));
                }
                g += 1;
            }
        } else {
            // Conjunctive: a multi-output tool emits all of its groups.
            if s.produced.len() != t.produces.len() {
                v.violations.push(Violation::at(Clause::Shape, i));
            } else {
                let mut g = 0;
                while g < s.produced.len() {
                    if !same_slot_set(&slots_of(&s.produced[g]), &t.produces[g], n_nodes) {
                        v.violations.push(Violation::at(Clause::Shape, i));
                    }
                    g += 1;
                }
            }
        }
        i += 1;
    }
}

// ---------------------------------------------------------------------------
// the remaining clauses
// ---------------------------------------------------------------------------

fn emits(s: &Step, e: EpId) -> bool {
    let mut g = 0;
    while g < s.produced.len() {
        let mut k = 0;
        while k < s.produced[g].len() {
            if s.produced[g][k].1 == e {
                return true;
            }
            k += 1;
        }
        g += 1;
    }
    false
}

fn check_rest(p: &Problem, q: &Plan, class: &[u32], v: &mut Verdict) {
    let mut i = 0;
    while i < q.steps.len() {
        let s = &q.steps[i];

        // 3. provenance, 4. conformance
        let mut j = 0;
        while j < s.used.len() {
            let (slot, ep) = s.used[j];
            let mut produced_somewhere = false;
            let mut k = 0;
            while k < q.steps.len() {
                if emits(&q.steps[k], ep) {
                    produced_somewhere = true;
                    break;
                }
                k += 1;
            }
            if !produced_somewhere {
                v.violations.push(Violation::bind(Clause::Provenance, i, slot, ep));
            }
            if !fills(p, q, class, &s.used, ep, slot) {
                v.violations.push(Violation::bind(Clause::Conformance, i, slot, ep));
            }
            j += 1;
        }

        // 5. emission and 6. rooted. Both exempt the given step: after a
        // timeline merge it emits one sample's endpoint under another sample's
        // slot, so the property subset genuinely does not hold there, and it
        // consumes nothing to be rooted in. Clause 7 covers it instead.
        if s.transform != p.given_tr {
            let mut g = 0;
            while g < s.produced.len() {
                let mut k = 0;
                while k < s.produced[g].len() {
                    let (slot, ep) = s.produced[g][k];
                    if !fills(p, q, class, &s.used, ep, slot) {
                        v.violations.push(Violation::bind(Clause::Emission, i, slot, ep));
                    }
                    let mut c = 0;
                    while c < s.used.len() {
                        let consumed = class[s.used[c].1 as usize];
                        let mut declared = false;
                        let ps = &q.endpoints[ep as usize].parents;
                        let mut z = 0;
                        while z < ps.len() {
                            if class[ps[z] as usize] == consumed {
                                declared = true;
                                break;
                            }
                            z += 1;
                        }
                        if !declared {
                            v.violations.push(Violation::bind(Clause::Rooted, i, slot, ep));
                        }
                        c += 1;
                    }
                    k += 1;
                }
                g += 1;
            }
        } else {
            // 7. givens: the given step may present only endpoints whose
            // properties match something the problem declared as an input.
            let mut g = 0;
            while g < s.produced.len() {
                let mut k = 0;
                while k < s.produced[g].len() {
                    let (slot, ep) = s.produced[g][k];
                    let mut matched = false;
                    let mut a = 0;
                    'outer: while a < p.given.len() {
                        let mut b = 0;
                        while b < p.given[a].len() {
                            let n = &p.nodes[p.given[a][b] as usize];
                            if same_prop_set(&n.props, &q.endpoints[ep as usize].props) {
                                matched = true;
                                break 'outer;
                            }
                            b += 1;
                        }
                        a += 1;
                    }
                    if !matched {
                        v.violations.push(Violation::bind(Clause::Givens, i, slot, ep));
                    }
                    k += 1;
                }
                g += 1;
            }
        }
        i += 1;
    }

    // 8. schedulable: every emitter of a consumed endpoint precedes the
    // consumer. A list that is a topological order is itself the proof that the
    // producer-consumer graph is acyclic, so this replaces the separate
    // ordering and cycle checks.
    let mut jx = 0;
    while jx < q.steps.len() {
        let mut b = 0;
        while b < q.steps[jx].used.len() {
            let ep = q.steps[jx].used[b].1;
            let mut ix = 0;
            while ix < q.steps.len() {
                if emits(&q.steps[ix], ep) && ix >= jx {
                    v.violations.push(Violation::bind(
                        Clause::Schedulable, jx, q.steps[jx].used[b].0, ep));
                }
                ix += 1;
            }
            b += 1;
        }
        jx += 1;
    }

    // 9. boundary
    let mut n_target = 0;
    let mut n_given = 0;
    let mut i2 = 0;
    while i2 < q.steps.len() {
        if q.steps[i2].transform == p.target_tr {
            n_target += 1;
        }
        if q.steps[i2].transform == p.given_tr {
            n_given += 1;
            if !q.steps[i2].used.is_empty() {
                v.violations.push(Violation::at(Clause::Boundary, i2));
            }
        }
        i2 += 1;
    }
    if n_target != 1 || n_given != 1 {
        v.violations.push(Violation::bare(Clause::Boundary));
    }
}

fn same_prop_set(a: &[PropId], b: &[PropId]) -> bool {
    if a.len() != b.len() {
        // Property lists are interned and deduplicated upstream, so equal sets
        // have equal lengths. Compare as sets anyway, below, to avoid depending
        // on the order.
    }
    let mut i = 0;
    while i < a.len() {
        let mut hit = false;
        let mut j = 0;
        while j < b.len() {
            if b[j] == a[i] {
                hit = true;
                break;
            }
            j += 1;
        }
        if !hit {
            return false;
        }
        i += 1;
    }
    i = 0;
    while i < b.len() {
        let mut hit = false;
        let mut j = 0;
        while j < a.len() {
            if a[j] == b[i] {
                hit = true;
                break;
            }
            j += 1;
        }
        if !hit {
            return false;
        }
        i += 1;
    }
    true
}

/// Adjudicate a plan against its problem.
///
/// `Verdict::ok` is the soundness claim. It is not a completeness claim: a
/// rejection says this plan is wrong, never that no plan exists.
pub fn check(p: &Problem, q: &Plan) -> Verdict {
    let mut v = Verdict::default();
    // Indexing first, and bail on failure. Every clause below indexes with the
    // ids the reply supplied, and an out-of-range one would either panic or --
    // worse -- read a default that satisfies everything.
    if !check_indexed(p, q, &mut v) {
        return v;
    }
    if q.steps.is_empty() {
        v.violations.push(Violation::bare(Clause::Nonempty));
        return v;
    }
    let class = match structural_classes(q) {
        Some(c) => c,
        None => {
            // An endpoint naming a parent at or after its own index. The
            // encoder emits rows parents-first, so this is a malformed reply.
            v.violations.push(Violation::bare(Clause::Indexed));
            return v;
        }
    };
    check_shape(p, q, &mut v);
    check_rest(p, q, &class, &mut v);
    v
}
