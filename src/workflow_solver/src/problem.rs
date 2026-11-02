//! Reading a problem, and deriving the maps the search steers by.
//!
//! The maps are pure functions of the problem, so the two implementations can be
//! made to agree here before either makes a decision.
//!
//! Which notion of *sameness* each map uses is not uniform and not obvious:
//!
//! - Transforms compare by **identity**. `Transform` defines `__hash__` and not
//!   `__eq__`, so `parent == child` is `is`, and two duplicate transforms are
//!   two transforms. Here that is the arena index.
//! - Dependencies compare by **structure**, since `Node.__eq__` compares
//!   signatures. Here that is the interned `DepId`.
//! - The distance walk memoizes on transforms by **identity**, so the arena
//!   index again. `sig` is printed from properties alone, so duplicate
//!   transforms share it and so do two transforms differing only in a lineage
//!   constraint -- memoizing on it drops the second one reached out of the
//!   distance table, and out of the search with it.
//!
//! The payload's node table serves both roles: every entry becomes a `DepId`
//! and *also* an `EpId`. The two are separate arenas on purpose -- see `model`.

use serde::Deserialize;

use crate::det::{self, Map, Set};
use crate::model::{DepId, Deps, EpId, EpSig, Endpoints, Transform, TransformId, Types};

#[derive(Debug, Deserialize)]
pub struct EncodedNode {
    pub props: Vec<u32>,
    pub parents: Vec<u32>,
}

#[derive(Debug, Deserialize)]
pub struct EncodedTransform {
    pub requires: Vec<u32>,
    pub produces: Vec<Vec<u32>>,
}

#[derive(Debug, Deserialize)]
pub struct EncodedProblem {
    pub wire_version: u32,
    pub seed: u64,
    pub max_iter: u32,
    pub max_refine: u32,
    pub n_properties: usize,
    pub nodes: Vec<EncodedNode>,
    pub transforms: Vec<EncodedTransform>,
    /// The synthesized `given` transform's index.
    pub given_index: TransformId,
    /// The caller's own `transforms` sequence, as indices. May repeat.
    pub caller_transforms: Vec<TransformId>,
    pub target_index: TransformId,
    /// Endpoint ids per given group, positionally matched to
    /// `transforms[given_index].produces`.
    pub given: Vec<Vec<u32>>,
}

/// Everything derived from the payload, and immutable from then on.
///
/// The endpoint arena is deliberately *not* in here: it grows throughout a solve,
/// and keeping it separate lets the search borrow the problem and mint endpoints
/// at the same time.
pub struct Problem {
    pub types: Types,
    pub deps: Deps,
    pub transforms: Vec<Transform>,
    pub given_index: TransformId,
    pub target_index: TransformId,
    pub caller_transforms: Vec<TransformId>,
    /// Given endpoints per group, in the order the encoder fixed.
    pub given: Vec<Vec<EpId>>,
    pub seed: u64,
    pub max_iter: u32,
    pub max_refine: u32,

    /// Given, then the caller's sequence, then target.
    pub iter_order: Vec<TransformId>,
    /// First appearance walking `iter_order`, requires before produces.
    pub dep_rank: Map<DepId, u32>,
    /// Each dependency's lineage parents in *rank* order, not intern order.
    /// One read needs it: the refiner sums a distance per lineage constraint,
    /// and floating-point addition is not associative.
    pub dep_parents_ranked: Vec<Vec<DepId>>,
    /// Ancestors of every given endpoint, transitively. Compared by equality,
    /// matching Python's `e.parents & inherent_parents` set intersection.
    pub inherent_parents: Set<EpSig>,
    pub given_endpoints: Set<EpSig>,

    pub product2consumer: Map<DepId, Vec<TransformId>>,
    pub demand2product: Map<DepId, Vec<DepId>>,
    pub demand2producer: Map<DepId, Vec<TransformId>>,
    pub distance: Map<TransformId, i64>,
    pub opportunity: Map<TransformId, i64>,
    pub max_distance: i64,
    pub relevant_transforms: Vec<TransformId>,
    pub free_transforms: Vec<TransformId>,
    /// No route from the givens to the target; the search never starts.
    pub no_path_possible: bool,
    /// Per transform: 1.0 when applying it can re-enable itself with nothing
    /// gained. See `derive`.
    pub self_feed: Vec<f64>,
}

impl Problem {
    pub fn load(enc: &EncodedProblem) -> Result<(Self, Endpoints), String> {
        let mut types = Types::new(enc.n_properties);
        let mut deps = Deps::default();
        let mut endpoints = Endpoints::default();
        // The encoder lists every node after its parents, so a parent's index is
        // always resolved. That the dependency index we get back is the one we
        // were handed is asserted rather than assumed: if it is not, every id in
        // the payload means something else, and the failure is a wrong plan
        // rather than a crash.
        let mut node_ep: Vec<EpId> = Vec::with_capacity(enc.nodes.len());
        for (i, n) in enc.nodes.iter().enumerate() {
            for &p in &n.props {
                if p as usize >= enc.n_properties {
                    return Err(format!("property {p} out of range"));
                }
            }
            let ty = types.intern(&n.props);
            let got = deps.intern(ty, &n.parents);
            if got as usize != i {
                return Err(format!(
                    "node {i} interned as {got}: the payload lists two nodes with the \
                     same properties and lineage, which the encoder should have merged"
                ));
            }
            let ep_parents: Vec<EpId> = n.parents.iter().map(|&p| node_ep[p as usize]).collect();
            node_ep.push(endpoints.new_endpoint(ty, &ep_parents));
        }

        // Structural identity from properties only, as `str(Transform)` prints
        // `requires` and `produces` and never a dependency's parents.
        let mut tr_sigs: Map<Vec<u32>, u32> = det::map();
        let mut transforms = Vec::with_capacity(enc.transforms.len());
        for t in &enc.transforms {
            let mut key: Vec<u32> = Vec::new();
            {
                let push = |key: &mut Vec<u32>, d: u32| {
                    key.extend_from_slice(&types.props(deps.ty(d)));
                    key.push(u32::MAX);
                };
                for &r in &t.requires { push(&mut key, r); }
                key.push(u32::MAX - 1);
                for g in &t.produces {
                    for &p in g { push(&mut key, p); }
                    key.push(u32::MAX - 2);
                }
            }
            let next = tr_sigs.len() as u32;
            let sig = *tr_sigs.entry(key).or_insert(next);
            transforms.push(Transform {
                requires: t.requires.clone(),
                produces: t.produces.clone(),
                sig,
            });
        }

        let mut iter_order = Vec::with_capacity(enc.caller_transforms.len() + 2);
        iter_order.push(enc.given_index);
        iter_order.extend_from_slice(&enc.caller_transforms);
        iter_order.push(enc.target_index);

        let mut dep_rank: Map<DepId, u32> = det::map();
        for &ti in &iter_order {
            let t = &transforms[ti as usize];
            for &d in &t.requires {
                let next = dep_rank.len() as u32;
                dep_rank.entry(d).or_insert(next);
            }
            for g in &t.produces {
                for &d in g {
                    let next = dep_rank.len() as u32;
                    dep_rank.entry(d).or_insert(next);
                }
            }
        }

        let mut dep_parents_ranked: Vec<Vec<DepId>> = Vec::with_capacity(deps.len());
        for d in 0..deps.len() {
            let mut ps = deps.parents(d as DepId).to_vec();
            // A parent with no rank never appeared in any transform, so it can
            // never be looked up in `used` either; it sorts last and stays put.
            ps.sort_by_key(|x| dep_rank.get(x).copied().unwrap_or(u32::MAX));
            dep_parents_ranked.push(ps);
        }

        let given: Vec<Vec<EpId>> = enc
            .given
            .iter()
            .map(|g| g.iter().map(|&n| node_ep[n as usize]).collect())
            .collect();
        let mut given_endpoints: Set<EpSig> = det::set();
        for g in &given { for &e in g { given_endpoints.insert(endpoints.sig(e)); } }
        let mut inherent_parents: Set<EpSig> = det::set();
        {
            let mut todo: Vec<EpId> = given.iter().flatten().copied().collect();
            let mut seen: Set<EpId> = det::set();
            while let Some(e) = todo.pop() {
                for &p in endpoints.parents(e) {
                    if seen.insert(p) {
                        inherent_parents.insert(endpoints.sig(p));
                        todo.push(p);
                    }
                }
            }
        }

        let mut p = Problem {
            types,
            deps,
            transforms,
            given_index: enc.given_index,
            target_index: enc.target_index,
            caller_transforms: enc.caller_transforms.clone(),
            given,
            seed: enc.seed,
            max_iter: enc.max_iter,
            max_refine: enc.max_refine,
            iter_order,
            dep_rank,
            dep_parents_ranked,
            inherent_parents,
            given_endpoints,
            product2consumer: det::map(),
            demand2product: det::map(),
            demand2producer: det::map(),
            distance: det::map(),
            opportunity: det::map(),
            max_distance: 0,
            relevant_transforms: Vec::new(),
            free_transforms: Vec::new(),
            no_path_possible: false,
            self_feed: Vec::new(),
        };
        p.derive();
        Ok((p, endpoints))
    }

    #[inline]
    pub fn dep_is_a(&self, x: DepId, y: DepId) -> bool {
        self.types.is_a(self.deps.ty(x), self.deps.ty(y))
    }

    /// `e.IsA(d)` for an endpoint against a dependency. The arena is passed in
    /// because it lives outside the problem and keeps growing.
    #[inline]
    pub fn ep_is_a(&self, eps: &Endpoints, e: EpId, d: DepId) -> bool {
        self.types.is_a(eps.ty(e), self.deps.ty(d))
    }

    fn derive(&mut self) {
        let n = self.transforms.len();

        // Which transforms can feed themselves forever.
        //
        // A transform whose product satisfies one of its own requirements
        // re-enters the frontier every time it is applied, so its copies grow
        // without bound while the chain that reaches the target sits at one copy.
        // `product2consumer` cannot see this: it skips `parent == child` because
        // Python's does.
        //
        // The test is type *equality*, not `is_a`. A product that is a strict
        // superset of the requirement also re-enables the transform, but it adds
        // a property each time, so the state signature moves and the search
        // terminates on its own -- that is an enrichment step, and the shipped
        // metagenomics workflow has exactly one. A product of the same type adds
        // nothing: the loop is the whole of what it does. Across the ratchet
        // corpus the equality test is 0 on all 17 real payloads and 1-5 on every
        // generated one, so a policy that reads it cannot move a real plan.
        self.self_feed = vec![0.0; n];
        for t in 0..n {
            let looped = self.transforms[t].produces.iter().flatten().any(|&prod| {
                self.transforms[t]
                    .requires
                    .iter()
                    .any(|&req| self.dep_is_a(prod, req) && self.dep_is_a(req, prod))
            });
            if looped { self.self_feed[t] = 1.0; }
        }
        // Pairwise, as Python does it: an index built to avoid the quadratic
        // loop would still have to reproduce this answer for the pathological
        // cases.
        for parent in 0..n {
            for child in 0..n {
                if parent == child { continue; } // identity, as in Python
                for gi in 0..self.transforms[parent].produces.len() {
                    for pi in 0..self.transforms[parent].produces[gi].len() {
                        let prod = self.transforms[parent].produces[gi][pi];
                        let consumed = (0..self.transforms[child].requires.len())
                            .any(|k| self.dep_is_a(prod, self.transforms[child].requires[k]));
                        if !consumed { continue; }
                        let e = self.product2consumer.entry(prod).or_default();
                        if !e.contains(&(child as TransformId)) { e.push(child as TransformId); }
                    }
                }
            }
        }

        for child in 0..n {
            for parent in 0..n {
                if parent == child { continue; }
                for ci in 0..self.transforms[child].requires.len() {
                    let c = self.transforms[child].requires[ci];
                    let mut found = false;
                    for gi in 0..self.transforms[parent].produces.len() {
                        for pi in 0..self.transforms[parent].produces[gi].len() {
                            let prod = self.transforms[parent].produces[gi][pi];
                            if !self.dep_is_a(prod, c) { continue; }
                            let e = self.demand2product.entry(c).or_default();
                            if !e.contains(&prod) { e.push(prod); }
                            found = true;
                        }
                    }
                    if found {
                        let e = self.demand2producer.entry(c).or_default();
                        if !e.contains(&(parent as TransformId)) {
                            e.push(parent as TransformId);
                        }
                    }
                }
            }
        }

        // Frozen into rank order once, here: sorting at the point of use puts a
        // sort inside the distance walk, the hottest loop in the search.
        let dep_rank = &self.dep_rank;
        for v in self.demand2product.values_mut() { v.sort_unstable_by_key(|d| dep_rank[d]); }
        for v in self.demand2producer.values_mut() { v.sort_unstable(); }
        for v in self.product2consumer.values_mut() { v.sort_unstable(); }

        // Distance to target by a single-pass backward BFS, memoized on the
        // transform rather than the path it was reached by, so a transform
        // expands once and the walk is O(V+E). Walking every simple path instead
        // is combinatorial once cycles overlap, and does not terminate at all on
        // a graph where every action has an inverse edge.
        //
        // The memo is keyed by arena index, not by `sig`: `sig` is shared by
        // duplicate transforms and by two transforms differing only in a lineage
        // constraint, which have different producer edges. Memoizing on it leaves
        // the second one reached with no distance entry, and membership here is
        // what `relevant_transforms` and the no-path bail read -- so that
        // transform would silently leave the search.
        //
        // `distance` is shortest-path, `opportunity` accumulates once per
        // incoming edge, and both feed the mcts guiding score only.
        let mut todo: std::collections::VecDeque<(TransformId, i64)> =
            std::collections::VecDeque::from([(self.target_index, -1)]);
        while let Some((step, consumer_dist)) = todo.pop_front() {
            let dist = consumer_dist + 1;
            *self.opportunity.entry(step).or_insert(1) += dist;
            if self.distance.contains_key(&step) { continue; }
            self.distance.insert(step, dist);
            for pi in 0..self.transforms[step as usize].requires.len() {
                let req = self.transforms[step as usize].requires[pi];
                if let Some(producers) = self.demand2producer.get(&req) {
                    for &producer in producers {
                        todo.push_back((producer, dist));
                    }
                }
            }
        }

        // The caller's own sequence, filtered -- not the arena, and not sorted.
        // Duplicates stay duplicated: Python's membership test is identity and
        // both copies pass it.
        self.relevant_transforms = self
            .caller_transforms
            .iter()
            .copied()
            .filter(|t| self.distance.contains_key(t))
            .collect();

        if !self.distance.contains_key(&self.given_index) {
            self.no_path_possible = true;
            return;
        }
        self.max_distance = self.distance.values().copied().max().unwrap_or(0);

        let mut rts: Set<TransformId> = self.relevant_transforms.iter().copied().collect();
        rts.insert(self.given_index);
        rts.insert(self.target_index);
        for v in self.product2consumer.values_mut() { v.retain(|t| rts.contains(t)); }
        for v in self.demand2producer.values_mut() { v.retain(|t| rts.contains(t)); }
        let mut rtsp: Set<DepId> = det::set();
        for &t in &rts {
            for g in &self.transforms[t as usize].produces { rtsp.extend(g.iter().copied()); }
        }
        for v in self.demand2product.values_mut() { v.retain(|d| rtsp.contains(d)); }

        self.free_transforms = self
            .relevant_transforms
            .iter()
            .copied()
            .filter(|&t| self.transforms[t as usize].is_free())
            .collect();
    }
}
