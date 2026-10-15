//! Applications, and the pieces both search phases are built from.
//!
//! An `Application` in Python is a transform plus the endpoints bound to its
//! requirements, and it is the object the whole solver is keyed on, by a string
//! signature rebuilt and re-hashed on every use. Here it is an arena entry with
//! an interned signature, and the two are different things on purpose.
//!
//! **`ApplId` is identity; `ApplSig` is structure.** Python conflates them
//! behind one string, and that conflation is a live defect: `expand_node`
//! removes the step it is swapping with
//! `[s for s in state.steps if s.Signature() != step.Signature()]`, which drops
//! *both* members of a colliding pair. Collisions are not hash accidents -- two
//! duplicate transforms genuinely share a structural key, and applications of
//! them with the same inputs genuinely have one signature -- so they are
//! semantics and they survive the port. The defect survives with them,
//! deliberately: this is a port, and a port that fixes things cannot be checked
//! against what it replaced.
//!
//! `SolverState.have` is built and copied on every expansion in Python and never
//! read. It is not carried here.

use crate::det::{self, Map, Set};
use crate::model::{DepId, EpId, EpSig, Endpoints, TransformId, TransformSig};
use crate::problem::Problem;

pub type ApplId = u32;
pub type ApplSig = u32;
/// The multiset of an entire state's application signatures, interned.
pub type StateSig = u32;

/// A binding of dependencies to endpoints, in insertion order.
///
/// A `dict[Dependency, Endpoint]`, and the two properties that matter are the
/// ones a `Vec` of pairs gets for free and a map would not: iteration is
/// insertion-ordered, and re-binding a dependency already present replaces its
/// value *in place* rather than appending. The second is not hypothetical --
/// a transform can require the same dependency twice, in which case Python's
/// `used | {p: e}` leaves one entry and the signature repeats it.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Bindings(pub Vec<(DepId, EpId)>);

impl Bindings {
    #[inline]
    pub fn get(&self, d: DepId) -> Option<EpId> {
        self.0.iter().find(|(k, _)| *k == d).map(|(_, v)| *v)
    }
    #[inline]
    pub fn set(&mut self, d: DepId, e: EpId) {
        match self.0.iter_mut().find(|(k, _)| *k == d) {
            Some(slot) => slot.1 = e,
            None => self.0.push((d, e)),
        }
    }
    #[inline]
    pub fn len(&self) -> usize { self.0.len() }
    #[inline]
    pub fn values(&self) -> impl Iterator<Item = EpId> + '_ { self.0.iter().map(|(_, v)| *v) }
    pub fn with(&self, d: DepId, e: EpId) -> Self {
        let mut c = self.clone();
        c.set(d, e);
        c
    }
}

/// A product group: `dict[Dependency, Endpoint]`, same shape and same reasons.
pub type Group = Vec<(DepId, EpId)>;

#[derive(Clone, Debug)]
pub struct Appl {
    /// Which timeline this application was born into. `initial_timeline`.
    pub timeline: i64,
    pub transform: TransformId,
    pub used: Bindings,
    pub produced: Vec<Group>,
    pub score: [f64; 2],
    pub iteration: i64,
    pub sig: ApplSig,
}

impl Appl {
    /// Every endpoint this application produces, across every group.
    pub fn products(&self) -> impl Iterator<Item = EpId> + '_ {
        self.produced.iter().flatten().map(|(_, e)| *e)
    }
    /// The target step is the one that produces nothing at all.
    pub fn is_terminal(&self) -> bool { self.produced.iter().all(|g| g.is_empty()) }
}

/// The arenas the search allocates into, and the interners that give them
/// meaning. Separate from `Problem` because all of this grows during a solve
/// while the problem does not.
pub struct Arena {
    pub eps: Endpoints,
    pub appls: Vec<Appl>,
    appl_sigs: Map<(TransformSig, Vec<(DepId, EpSig)>), ApplSig>,
    state_sigs: Map<Vec<ApplSig>, StateSig>,
}

impl Arena {
    pub fn new(eps: Endpoints) -> Self {
        Self { eps, appls: Vec::new(), appl_sigs: det::map(), state_sigs: det::map() }
    }

    #[inline]
    pub fn appl(&self, a: ApplId) -> &Appl { &self.appls[a as usize] }

    /// `Application.Signature()`: the transform's structural key, then one
    /// `dep:endpoint` pair per *requirement*, in requirement order, skipping any
    /// requirement that is unbound.
    ///
    /// Requirement order rather than binding order, and repeats rather than
    /// distinct keys -- a transform requiring the same dependency twice emits
    /// the pair twice off a single binding. Both are Python's, verbatim.
    pub fn sign(&mut self, p: &Problem, transform: TransformId, used: &Bindings) -> ApplSig {
        let mut parts: Vec<(DepId, EpSig)> = Vec::with_capacity(used.len());
        for &d in &p.transforms[transform as usize].requires {
            if let Some(e) = used.get(d) { parts.push((d, self.eps.sig(e))); }
        }
        let key = (p.transforms[transform as usize].sig, parts);
        let next = self.appl_sigs.len() as ApplSig;
        *self.appl_sigs.entry(key).or_insert(next)
    }

    pub fn new_appl(
        &mut self, p: &Problem, timeline: i64, transform: TransformId, used: Bindings,
    ) -> ApplId {
        let sig = self.sign(p, transform, &used);
        let id = self.appls.len() as ApplId;
        self.appls.push(Appl {
            timeline, transform, used, produced: Vec::new(),
            score: [0.0, 0.0], iteration: -1, sig,
        });
        id
    }

    /// A copy with its own identity -- `Application(...)` in `rectify`.
    pub fn clone_appl(&mut self, a: ApplId) -> ApplId {
        let c = self.appls[a as usize].clone();
        let id = self.appls.len() as ApplId;
        self.appls.push(c);
        id
    }

    /// Recompute a signature after `used` changed: `appl._sig = None; ...`.
    pub fn resign(&mut self, p: &Problem, a: ApplId) {
        let (tr, used) = {
            let x = &self.appls[a as usize];
            (x.transform, x.used.clone())
        };
        let sig = self.sign(p, tr, &used);
        self.appls[a as usize].sig = sig;
    }

    /// `RefinerState.Signature()`: the multiset of its steps' signatures.
    ///
    /// Python sorts the signature *strings* and concatenates them. A sorted
    /// multiset of interned ids is the same identity, minus the one thing the
    /// concatenation could in principle get wrong -- two different multisets
    /// joining to the same string.
    pub fn state_sig(&mut self, steps: &[ApplSig]) -> StateSig {
        let mut key = steps.to_vec();
        key.sort_unstable();
        let next = self.state_sigs.len() as StateSig;
        *self.state_sigs.entry(key).or_insert(next)
    }

    /// A Python `set[Endpoint]`: deduplicated by equality, holding objects.
    ///
    /// Which object survives among equals is not observable -- an endpoint's
    /// signature is built from its parents' *keys*, and every membership test on
    /// a lineage set compares by equality -- so first-seen wins and the order is
    /// this function's, not a hash table's.
    pub fn ep_set(&self, of: impl IntoIterator<Item = EpId>) -> Vec<EpId> {
        let mut seen: Set<EpSig> = det::set();
        let mut out = Vec::new();
        for e in of {
            if seen.insert(self.eps.sig(e)) { out.push(e); }
        }
        out
    }
}

/// The endpoints that could satisfy one requirement, given what has been
/// produced so far. `_find_endpoints`.
///
/// The given-first preference is *per requirement*: if any given endpoint
/// satisfies this dependency, only givens are offered here, regardless of
/// whether some other requirement of the same transform needs a produced
/// upstream. `_resolve`'s two-pass scaffold is what rescues the case where the
/// givens all fail a lineage constraint.
fn find_endpoints(
    p: &Problem, ar: &Arena, production: &Map<DepId, Vec<EpId>>, d: DepId,
    include_produced: bool,
) -> Vec<EpId> {
    let mut given = Vec::new();
    let mut produced = Vec::new();
    let Some(products) = p.demand2product.get(&d) else { return Vec::new() };
    for &product in products { // already in rank order
        let Some(eps) = production.get(&product) else { continue };
        for &e in eps {
            debug_assert!(p.ep_is_a(&ar.eps, e, d));
            if p.given_endpoints.contains(&ar.eps.sig(e)) { given.push(e); } else { produced.push(e); }
        }
    }
    if include_produced {
        given.extend(produced);
        return given;
    }
    if !given.is_empty() { return given; }
    produced
}

/// Does `e` satisfy every lineage constraint `d` declares, given the bindings
/// made so far? `_satisfies_lineage`.
fn satisfies_lineage(
    p: &Problem, ar: &Arena, d: DepId, e: EpId, used: &Bindings,
) -> Result<bool, String> {
    for &parent in &p.dep_parents_ranked[d as usize] {
        let Some(matched) = used.get(parent) else {
            // Python raises `KeyError` here, and it means the same thing: a
            // requirement declared a lineage constraint on a requirement that
            // comes *after* it, so the constraint cannot be evaluated when it
            // is reached. Named rather than guessed at, because guessing would
            // make the two implementations disagree about a malformed transform.
            return Err(format!(
                "requirement {d} constrains lineage on requirement {parent}, which it \
                 precedes; reorder the transform's requirements"
            ));
        };
        let want = ar.eps.sig(matched);
        let mut seen: Set<EpSig> = det::set();
        if !ar.eps.is_ancestor(want, e, &mut seen) { return Ok(false); }
    }
    Ok(true)
}

/// Every application of one transform that the current production allows.
/// `generate_applications_of_transform`.
///
/// With `mock_produced`, lineage is not checked and the given products are
/// reused rather than minted -- that is the refiner's mode, where the point is
/// to try a different *binding* of the same step and rectify afterwards.
///
/// **CAUTION** The lineage check is the constraint, not an optimisation.
/// Skipping it is why one refiner iteration enumerates 110,866 candidates on the
/// unpinned metagenomics workflow where the specification admits 33 -- a factor
/// of 3,360, paid in full before anything is scored.
pub fn generate_applications(
    p: &Problem, ar: &mut Arena, timeline: i64, production: &Map<DepId, Vec<EpId>>,
    blacklist: &Set<ApplSig>, tr: TransformId, mock_produced: Option<&Vec<Group>>,
) -> Result<Vec<ApplId>, String> {
    let requires = p.transforms[tr as usize].requires.clone();
    if requires.is_empty() {
        let used = Bindings::default();
        let sig = ar.sign(p, tr, &used);
        if blacklist.contains(&sig) { return Ok(Vec::new()); }
        let a = ar.new_appl(p, timeline, tr, used);
        // No inputs, so no lineage: these endpoints have no parents at all.
        let groups = p.transforms[tr as usize].produces.clone();
        let produced = groups
            .iter()
            .map(|g| {
                g.iter()
                    .map(|&d| (d, ar.eps.new_endpoint(p.deps.ty(d), &[])))
                    .collect::<Group>()
            })
            .collect();
        ar.appls[a as usize].produced = produced;
        return Ok(vec![a]);
    }
    // The lineage prune runs on both paths. It used to be switched off whenever
    // `mock_produced` was passed, which is the refiner, and that is the whole
    // reason a refiner iteration enumerates 110,866 candidate plans on the
    // unpinned metagenomics workflow where the specification admits 33.
    // `mock_produced` still decides where a candidate's *products* come from; it
    // no longer decides whether its *inputs* are checked.
    let mint_from_transform = mock_produced.is_none();

    // Two passes: prefer given. Fall back to given+produced only when the
    // given-only pass could not reach a leaf *for lineage reasons* -- reaching a
    // leaf and finding it blacklisted is MCTS revisiting a transform, and
    // falling back there would flood the frontier with redundant variants.
    for include_produced in [false, true] {
        let mut matches: Map<DepId, Vec<EpId>> = det::map();
        let mut missing = false;
        for &d in &requires {
            let c = find_endpoints(p, ar, production, d, include_produced);
            if c.is_empty() { missing = true; break; }
            matches.insert(d, c);
        }
        if missing {
            if include_produced { return Ok(Vec::new()); }
            continue;
        }

        let mut viable = Vec::new();
        let mut reached_leaf = false;
        let mut todo: Vec<(usize, EpId, Bindings)> = matches[&requires[0]]
            .iter()
            .map(|&e| (0usize, e, Bindings::default()))
            .collect();
        while let Some((di, e, used)) = todo.pop() {
            let d = requires[di];
            let used = used.with(d, e);
            if !satisfies_lineage(p, ar, d, e, &used)? { continue; }
            if di + 1 < requires.len() {
                let next = di + 1;
                for &ne in &matches[&requires[next]] {
                    todo.push((next, ne, used.clone()));
                }
                continue;
            }
            reached_leaf = true;
            let sig = ar.sign(p, tr, &used);
            if blacklist.contains(&sig) { continue; }
            let a = ar.new_appl(p, timeline, tr, used);
            // The lineage a product inherits: every input, plus every input's
            // own parents -- one flattened hop per step, which is why
            // `is_ancestor` has to walk rather than test membership. Computed on
            // both paths now, because a refiner candidate mints its products too.
            let inputs: Vec<EpId> = ar.appls[a as usize].used.values().collect();
            let mut lin: Vec<EpId> = Vec::new();
            for &i in &inputs { lin.extend_from_slice(ar.eps.parents(i)); }
            lin.extend_from_slice(&inputs);
            let lin = ar.ep_set(lin);
            let produced: Vec<Group> = if mint_from_transform {
                p.transforms[tr as usize]
                    .produces
                    .clone()
                    .iter()
                    .map(|g| {
                        g.iter()
                            .map(|&d| (d, ar.eps.new_endpoint(p.deps.ty(d), &lin)))
                            .collect::<Group>()
                    })
                    .collect()
            } else {
                // Mint rather than reuse. Reusing the mock leaves a product
                // declaring the parents of inputs this candidate no longer
                // consumes, which is what `derived` states the negation of. The
                // mock still supplies the *slots*: `zip(tr.produces,
                // mock_produced)` -- the shorter one wins, and it is the mock
                // when a branched given application carries one group where its
                // transform declares several.
                let mock = mock_produced.unwrap().clone();
                p.transforms[tr as usize]
                    .produces
                    .clone()
                    .iter()
                    .zip(mock.iter())
                    .map(|(_, m)| {
                        m.iter()
                            .map(|&(d, _)| (d, ar.eps.new_endpoint(p.deps.ty(d), &lin)))
                            .collect::<Group>()
                    })
                    .collect()
            };
            ar.appls[a as usize].produced = produced;
            viable.push(a);
        }
        if reached_leaf || include_produced { return Ok(viable); }
    }
    Ok(Vec::new())
}
