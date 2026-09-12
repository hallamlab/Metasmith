//! The refiner: single-edge swaps against a found plan, looking for a better one.
//!
//! It changes the plan it was given on almost none of the shipped templates.
//!
//! **CAUTION** Do not measure a candidate's admissibility without propagating the
//! swap downstream. A candidate that reuses the pre-swap products leaves every
//! *downstream* lineage check reading the original plan's parents, where it is
//! vacuously true. Two separate measurements in this scope reported improvements
//! of +209 and +207 on the two fabfos templates from exactly that gap, and agreed
//! with each other to three decimals because they shared it. `relineage` is what
//! closes it, and `validate` is only sound because it runs.
//!
//! **The AND in `validate` is ordered, and the order is the optimisation.** Its
//! terms are independent and side-effect-free, and the lineage term is both the
//! cheapest and the one that rejects nearly everything, so it runs first.
//!
//! **One defect is reproduced here rather than fixed, knowingly.** The expansion
//! removes the step it is swapping by *signature*, which drops both members of a
//! colliding pair. This is a port, and a port that fixes things cannot be checked
//! against what it replaced.
//!
//! **The cascade is what a wide plan pays for, and the budget is the mitigation.**
//! `relineage` rebuilds every step downstream of a swap, so a candidate costs
//! O(steps) arena entries where it used to cost one, and nothing is released
//! until the solve ends. On the real workflows that is free: the unpinned
//! `metagenomics_from_paired_reads` arm is 0.83 s at `max_refine` 8 either way,
//! and 1.31 s against 1.60 s at 256, for a plan two steps shorter. On a wide
//! cyclic generated instance it is not: `sink-6807` enumerates 33,062 candidates
//! and 169,332 rebuilds, which is 4.2 s at a budget of 8 and roughly 70 GB at
//! 256. `REFINER_BUDGET` is why that is survivable, so read a budget above it as
//! a memory decision rather than a quality one.
//!
//! `score` runs once per expanded state and is nearly the whole cost of a solve,
//! so every table it needs comes out of `scratch.rs` rather than being allocated
//! and hashed per state. That module carries the argument for why swapping a
//! hash map for a flat array cannot move a plan.
use crate::det::{self, Map, Set};
use crate::model::{EpId, EpSig, Endpoints, TransformId};
use crate::problem::Problem;
use crate::policy::{Phase, Policy};
use crate::rectify::{get_order, order_steps, rectify};
use crate::rng::{DecisionStream, argmax_index};
use crate::scratch::{Scratch, SigMap, SigSet};
use crate::search::{ApplId, ApplSig, Arena, Group, StateSig, generate_applications};
use crate::smath::entropy;

pub struct RefinerState {
    pub steps: Vec<ApplId>,
    pub scores: [f64; 2],
    pub valid: bool,
    pub iteration: i64,
    /// The transform this candidate swapped in, or `u32::MAX` for the plan the
    /// refiner was handed. A stateful policy keys its statistics on it, mirroring
    /// `state._swapped_in` on the Python side.
    pub swapped_in: TransformId,
}

pub struct RefinerResult {
    pub steps: Vec<ApplId>,
    pub iterations: i64,
    pub found_on: i64,
}

/// `_has_ancestor`: the specification's relation, and the one the proof is
/// about -- the reflexive transitive closure over an endpoint's *declared*
/// parents.
///
/// It used to walk the step graph instead, mapping each product to the inputs
/// of the step that emitted it. That relation gives a given endpoint no parents
/// at all, because a branched given application carries `used == {}`, so every
/// anchor bound to a given failed.
///
/// **The two spaces are both needed and neither is the other.** The walk moves
/// over `EpId`, because `parents` is an identity relation; membership and the
/// hit test are over `EpSig`, because Python compares endpoints with `==`, which
/// is signature equality, and its `seen` is a `set[Endpoint]`.
///
/// Reading declared parents is sound only because `refine` rebuilds a swapped
/// step's descendants. On an unpropagated candidate those parents describe
/// bindings the candidate no longer has.
fn has_ancestor(
    eps: &Endpoints, seen: &mut SigSet, todo: &mut Vec<EpId>, n_sigs: usize,
    e: EpId, a: EpSig,
) -> bool {
    todo.clear();
    todo.push(e);
    seen.clear(n_sigs);
    seen.insert(eps.sig(e));
    while let Some(x) = todo.pop() {
        if eps.sig(x) == a { return true; }
        for &parent in eps.parents(x) {
            if seen.insert(eps.sig(parent)) { todo.push(parent); }
        }
    }
    false
}

/// One backward walk from `src`, filling `depths`.
///
/// `stop_at` is the destination this walk was built to answer. Stopping the
/// moment it is popped is exact: `depths` is checked on *pop*, so the first pop
/// of a node is the depth that gets recorded and the walk never revisits it --
/// the value `stop_at` receives here is the one a walk to exhaustion would
/// leave. What the shortened walk gives up is the ability to answer a *second*
/// destination, because a miss can then mean "not reachable" or "not yet
/// explored"; `Ok(false)` says the map is partial and the caller must re-walk
/// with `stop_at: None` before it may read a miss as an answer.
///
/// Most of what a walk to exhaustion would pop lies past the destination that
/// was asked for, and a second question of an already-walked source is rare.
///
/// One difference this does make, on inputs nothing in the corpus or the
/// templates reaches: a node with no producer raises here, and a walk that
/// stops early may never pop the node that would have raised. That error is
/// already the engine's own -- Python raises `KeyError` in the same place --
/// so it can only differ where the reference does not answer at all.
fn depth_walk(
    depths: &mut SigMap<i32>, todo: &mut Vec<(EpSig, i32)>,
    pf: &SigMap<(u32, u32)>, pf_flat: &[EpSig], n_sigs: usize,
    src: EpSig, stop_at: Option<EpSig>,
) -> Result<bool, String> {
    depths.clear(n_sigs);
    todo.clear();
    todo.push((src, 0i32));
    while let Some((n, dd)) = todo.pop() {
        if depths.contains_key(n) { continue; }
        depths.insert(n, dd);
        if stop_at == Some(n) { return Ok(false); }
        // A node no step in this state produces is a given or an inherent
        // parent, and the walk stops there. This used to be a hard error, and it
        // is the twin of the unguarded `_product2producer` lookup in Python:
        // harmless while almost no candidate survived long enough to walk this
        // far, and fatal the moment the lineage repair let them.
        let (start, len) = match pf.get(n) { Some(v) => v, None => continue };
        for i in start..start + len {
            todo.push((pf_flat[i as usize], dd + 1));
        }
    }
    Ok(true)
}

/// `state.steps` with `step` swapped for `appl`, every descendant rebuilt.
/// `_relineage`.
///
/// A product's parents are the inputs of the step that emitted it, so rebinding
/// one step invalidates the declared lineage of everything downstream of it.
/// Rebuilding is not an optimisation -- `derived` is an equality, and a stale
/// product fails it. Worse, it fails it invisibly: a candidate that keeps the
/// pre-swap endpoints leaves every downstream lineage check reading the original
/// plan's parents, where it is vacuously true.
///
/// **To a fixpoint, not in one pass.** `ordered` is a depth map over the graph
/// *before* the swap, and the swap moves it, so a descendant can sit ahead of
/// its own producer. A single pass then leaves that descendant holding an
/// endpoint nothing in the candidate emits. Each pass strictly consumes remap
/// entries, so the bound is a guard rather than the mechanism.
///
/// **CAUTION** Under a cyclic candidate `ordered` is not a topological order at
/// all -- `get_order` gives up and dumps the stragglers at `max_depth` -- so a
/// descendant can be visited before its producer and keep its old inputs. That
/// is deliberate. `validate` rejects the cycle, and the cascade must not be what
/// decides it.
///
/// Nothing in `ordered` is mutated. Those applications belong to the parent
/// state and to every sibling candidate of the same expansion, and writing
/// through one corrupts both with nothing to show for it -- hence a new arena
/// entry per rebuilt step rather than an edit in place.
fn relineage(
    p: &Problem, ar: &mut Arena, ordered: &[ApplId], step: ApplId, appl: ApplId,
) -> Vec<ApplId> {
    // Keyed by signature, because Python's `remap` is a `dict[Endpoint, ...]`
    // and endpoint hashing is signature hashing. Never iterated.
    let mut remap: Map<EpSig, EpId> = det::map();
    let (old_groups, new_groups) = (ar.appl(step).produced.clone(), ar.appl(appl).produced.clone());
    for (og, ng) in old_groups.iter().zip(new_groups.iter()) {
        for &(dep, old_e) in og {
            if let Some(&(_, new_e)) = ng.iter().find(|(d, _)| *d == dep) {
                remap.insert(ar.eps.sig(old_e), new_e);
            }
        }
    }

    // By signature, not identity: a colliding pair drops both, which is what
    // this did before the cascade existed, and it is a port.
    let dropped = ar.appl(step).sig;
    let mut current: Vec<ApplId> =
        ordered.iter().copied().filter(|&s| ar.appl(s).sig != dropped).collect();

    for _ in 0..current.len() + 1 {
        if remap.is_empty() { break; }
        let mut moved = false;
        for i in 0..current.len() {
            let s = current[i];
            let mut new_used = ar.appl(s).used.clone();
            if !new_used.values().any(|e| remap.contains_key(&ar.eps.sig(e))) { continue; }
            for slot in new_used.0.iter_mut() {
                if let Some(&ne) = remap.get(&ar.eps.sig(slot.1)) { slot.1 = ne; }
            }
            // The lineage a product inherits, as `search.rs` mints it: every
            // input, plus every input's own parents, one flattened hop.
            let inputs: Vec<EpId> = new_used.values().collect();
            let mut lin: Vec<EpId> = Vec::new();
            for &e in &inputs { lin.extend_from_slice(ar.eps.parents(e)); }
            lin.extend_from_slice(&inputs);
            let lin = ar.ep_set(lin);

            let (timeline, transform, score, iteration) = {
                let a = ar.appl(s);
                (a.timeline, a.transform, a.score, a.iteration)
            };
            let groups = ar.appl(s).produced.clone();
            let rebuilt = ar.new_appl(p, timeline, transform, new_used);
            let mut produced: Vec<Group> = Vec::with_capacity(groups.len());
            for g in &groups {
                let mut ng: Group = Vec::with_capacity(g.len());
                for &(d, old_e) in g {
                    let ne = ar.eps.new_endpoint(p.deps.ty(d), &lin);
                    ng.push((d, ne));
                    remap.insert(ar.eps.sig(old_e), ne);
                }
                produced.push(ng);
            }
            ar.appls[rebuilt as usize].produced = produced;
            ar.appls[rebuilt as usize].score = score;
            ar.appls[rebuilt as usize].iteration = iteration;
            current[i] = rebuilt;
            moved = true;
        }
        if !moved { break; }
    }
    current.push(appl);
    current
}

pub struct Refiner<'a> {
    pub p: &'a Problem,
    pub given_appl: ApplId,
}

impl<'a> Refiner<'a> {
    /// The lineage term on its own: every binding descends from whatever its
    /// dependency's lineage constraint was bound to.
    fn lineage_ok(
        &self, ar: &Arena, steps: &[ApplId], sc: &mut Scratch, n_sigs: usize,
    ) -> Result<bool, String> {
        for &s in std::iter::once(&self.given_appl).chain(steps.iter()) {
            let used = &ar.appl(s).used;
            for &(d, e) in &used.0 {
                for &parent in &self.p.dep_parents_ranked[d as usize] {
                    // Python indexes `step.used[pproto]` and raises, and it means
                    // a malformed transform rather than an invalid candidate.
                    // Reporting it as "not rejected" was a hole that only ever
                    // pointed one way.
                    let constraint = used.get(parent).ok_or_else(|| {
                        format!("lineage constraint {parent} of requirement {d} is unbound")
                    })?;
                    if !has_ancestor(
                        &ar.eps, &mut sc.anc_seen, &mut sc.anc_todo, n_sigs,
                        e, ar.eps.sig(constraint))
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// The full check: every step is schedulable from the givens, and lineage
    /// holds.
    ///
    /// Schedulability fails on exactly the two things "invalid" has to mean --
    /// a cycle, whose members can never all be waited for, and a step with an
    /// input nothing produces -- and it subsumes the old separate test that the
    /// target's inputs were produced, since the target is one of `steps`.
    ///
    /// A forward walk over consumers cannot replace it: such a walk reaches a
    /// step as soon as **one** of its inputs is available and never asks about
    /// the others, so a cycle hanging off the side is invisible and `rectify`
    /// rewrites the state into a plan with unproduced inputs and no trace of one.
    ///
    /// Endpoints are held by `EpSig` (structure, matching Python's
    /// `set[Endpoint]` and what `get_order` uses, so a `true` here is the
    /// promise that ordering finds a total order) and steps by `ApplId`
    /// (identity, because two distinct applications may share a signature and
    /// both have to be runnable). The verdict does not depend on the order
    /// within a layer, so this adds no new site to the iteration-order
    /// contract.
    fn is_valid(
        &self, ar: &Arena, steps: &[ApplId], sc: &mut Scratch, n_sigs: usize,
    ) -> bool {
        sc.have.clear(n_sigs);
        for e in ar.appl(self.given_appl).products() { sc.have.insert(ar.eps.sig(e)); }
        sc.pending.clear();
        sc.pending.extend_from_slice(steps);
        while !sc.pending.is_empty() {
            sc.ready.clear();
            sc.rest.clear();
            for i in 0..sc.pending.len() {
                let s = sc.pending[i];
                if ar.appl(s).used.values().all(|e| sc.have.contains(ar.eps.sig(e))) {
                    sc.ready.push(s);
                } else {
                    sc.rest.push(s);
                }
            }
            if sc.ready.is_empty() { return false; } // looped, or an input nothing makes
            for i in 0..sc.ready.len() {
                let s = sc.ready[i];
                for e in ar.appl(s).products() { sc.have.insert(ar.eps.sig(e)); }
            }
            std::mem::swap(&mut sc.pending, &mut sc.rest);
        }
        // The lineage loop was repeated here, after `validate` had already run
        // it and returned early on a rejection. Both copies decided the same
        // question, so changing one moved nothing.
        true
    }

    fn validate(
        &self, ar: &Arena, steps: &[ApplId], sc: &mut Scratch, n_sigs: usize,
    ) -> Result<bool, String> {
        // `produced_from`, as one flat buffer of every step's inputs plus a
        // range per product. Unconditionally and before the early returns: the
        // lineage relation no longer reads it, but `score`'s depth walk does,
        // and that walk runs whatever this decides.
        sc.pf.clear(n_sigs);
        sc.pf_flat.clear();
        for &s in steps {
            let start = sc.pf_flat.len() as u32;
            for e in ar.appl(s).used.values() { sc.pf_flat.push(ar.eps.sig(e)); }
            let len = sc.pf_flat.len() as u32 - start;
            for e in ar.appl(s).products() { sc.pf.insert(ar.eps.sig(e), (start, len)); }
        }
        if !steps.iter().any(|&s| ar.appl(s).is_terminal()) {
            return Ok(false);
        }
        // Cheap term first: the two are independent and side-effect-free, and
        // this one rejects nearly everything.
        if !self.lineage_ok(ar, steps, sc, n_sigs)? { return Ok(false); }
        Ok(self.is_valid(ar, steps, sc, n_sigs))
    }

    pub fn score(
        &self, ar: &Arena, state: &mut RefinerState, sc: &mut Scratch,
    ) -> Result<(), String> {
        // The signature universe grows as the search interns endpoints, but not
        // during a scoring: `ar` is shared, so every table below is sized once
        // here and the sizes hold for the whole call.
        let n_sigs = ar.eps.n_sigs();
        sc.begin(n_sigs);

        state.valid = self.validate(ar, &state.steps, sc, n_sigs)?;
        let steps = &state.steps;

        for &s in steps {
            let used = &ar.appl(s).used;
            for &(d, _) in &used.0 {
                for &parent in &self.p.dep_parents_ranked[d as usize] {
                    let c = used.get(parent).ok_or_else(|| {
                        format!("lineage constraint {parent} of requirement {d} is unbound")
                    })?;
                    sc.lin_used.insert(ar.eps.sig(c));
                }
            }
        }
        // An ordered count, because `entropy` sums in the caller's order.
        for &s in steps {
            for e in ar.appl(s).used.values() {
                let es = ar.eps.sig(e);
                if !sc.lin_used.contains(es) { continue; }
                match sc.lin_usage.iter_mut().find(|(k, _)| *k == es) {
                    Some(slot) => slot.1 += 1,
                    None => sc.lin_usage.push((es, 1)),
                }
            }
        }
        sc.counts.extend(sc.lin_usage.iter().map(|(_, c)| *c));
        let e_score = entropy(&sc.counts);

        // One walk per distinct *source*: the destination is a plain equality
        // test during traversal and `seen` guarantees one visit per node, so a
        // depth map answers every destination asked of it -- once it is
        // complete. The two quirks are load-bearing and both feed the score --
        // a LIFO stack with an up-front `seen` check records depth at first pop
        // rather than the true maximum, and the `> 0` test below makes a
        // distance of zero indistinguishable from not-found.
        //
        // The per-source depth tables come out of a pool indexed by arrival
        // order rather than being allocated per source, which is what makes
        // caching them across the loop worth doing.
        let n_steps = steps.len() as f64;
        let mut n_sources = 0usize;
        for &s in steps {
            for &d in &self.p.transforms[ar.appl(s).transform as usize].requires {
                for &lin in &self.p.dep_parents_ranked[d as usize] {
                    let used = &ar.appl(s).used;
                    let (Some(e), Some(pe)) = (used.get(d), used.get(lin)) else {
                        return Err(format!("requirement {d} or its constraint {lin} is unbound"));
                    };
                    let (es, pes) = (ar.eps.sig(e), ar.eps.sig(pe));
                    // `produced_from`, which `validate` has already built, is
                    // this walk's adjacency: for a produced signature it holds
                    // the producing step's input signatures, contiguously.
                    // The two maps are filled by one loop over `steps` with
                    // last-write-wins, so they name the same producer for every
                    // signature.
                    let slot = match sc.depth_slot.get(es) {
                        Some(i) => i as usize,
                        None => {
                            let i = n_sources;
                            n_sources += 1;
                            if sc.depth_pool.len() <= i {
                                sc.depth_pool.push(SigMap::default());
                                sc.depth_full.push(false);
                            }
                            sc.depth_full[i] = depth_walk(
                                &mut sc.depth_pool[i], &mut sc.depth_todo,
                                &sc.pf, &sc.pf_flat, n_sigs, es, Some(pes))?;
                            sc.depth_slot.insert(es, i as u32);
                            i
                        }
                    };
                    // A miss against a partial map is not an answer -- see
                    // `depth_walk`. Completing it costs a whole walk and is what
                    // the shortened ones are paying for; it is rare.
                    if !sc.depth_full[slot] && !sc.depth_pool[slot].contains_key(pes) {
                        sc.depth_full[slot] = depth_walk(
                            &mut sc.depth_pool[slot], &mut sc.depth_todo,
                            &sc.pf, &sc.pf_flat, n_sigs, es, None)?;
                    }
                    let max_d = sc.depth_pool[slot].get(pes).unwrap_or(-1);
                    sc.lin_distances.push(if max_d > 0 { max_d as f64/n_steps } else { 1.0 });
                }
            }
        }
        let lin_score = if sc.lin_distances.is_empty() {
            0.0
        } else {
            let mut acc = 0.0f64;
            for d in &sc.lin_distances { acc += d; }
            -acc/sc.lin_distances.len() as f64
        };
        let score = e_score*1000.0 + lin_score;
        state.scores = [score, score*(state.valid as i64 as f64)];
        Ok(())
    }
}

/// `refine_mcts`.
///
/// States live in one arena and the frontier holds indices, because Python's do
/// too: `valids` and `frontier` hold *references* to the same objects, and
/// `state._iteration = i` on a selected state is seen by the copy already sitting
/// in `valids`. Snapshotting on the way in would report a different
/// `found_on`.
pub fn refine(
    p: &Problem, ar: &mut Arena, rng: &mut DecisionStream, given_appl: ApplId,
    initial: &[ApplId], max_iters: u32,
) -> Result<RefinerResult, String> {
    let r = Refiner { p, given_appl };
    let mut states: Vec<RefinerState> = vec![RefinerState {
        steps: initial.to_vec(), scores: [0.0, 0.0], valid: true, iteration: -1,
        swapped_in: u32::MAX,
    }];
    let mut sc = Scratch::default();
    r.score(ar, &mut states[0], &mut sc)?;

    let sig0 = {
        let sigs: Vec<ApplSig> = states[0].steps.iter().map(|&s| ar.appl(s).sig).collect();
        ar.state_sig(&sigs)
    };
    // The initial state is in `valids` from the start *and* goes back in if it is
    // selected and still valid. Python does the same; the duplicate changes
    // nothing, since the winner is chosen by score and ties go to the first.
    let mut valids: Vec<usize> = vec![0];
    let mut frontier: Vec<usize> = vec![0];
    let mut seen: Set<StateSig> = det::set();
    seen.insert(sig0);

    let mut policy = Policy::from_env(Phase::Refine)?;
    if policy.wants_structure() { policy.set_structure(&p.self_feed); }
    // Mirrors Python's `incumbent`: the best valid score seen so far. The
    // refiner's reward is whether this expansion beat it.
    let mut incumbent = f64::NEG_INFINITY;

    let mut i: i64 = 0;
    while !frontier.is_empty() && (i as u32) < max_iters {
        i += 1;
        let idx = policy.select(
            rng,
            frontier.len(),
            // Not `scores`: channel 1 is `score * valid` over a score that is
            // never positive, so an invalid state's 0.0 outranks every valid one.
            // The retired rule ranked on that; the prior must not inherit it.
            |j| {
                let st = &states[frontier[j]];
                [st.scores[0], if st.valid { 1.0 } else { 0.0 }]
            },
            |j| states[frontier[j]].swapped_in,
        );
        let n = frontier.len() - 1;
        frontier.swap(idx, n);
        let k = frontier.pop().expect("checked non-empty");
        states[k].iteration = i;
        if states[k].valid { valids.push(k); }
        let swapped = states[k].swapped_in;
        let mut improved = false;

        // `expand_node`. Neither the current signatures nor the production map
        // depends on which step is being swapped, so both are built once.
        let steps = states[k].steps.clone();
        let current: Set<ApplSig> = steps.iter().map(|&s| ar.appl(s).sig).collect();
        let mut production: Map<u32, Vec<u32>> = det::map();
        for &s in &steps {
            for g in &ar.appl(s).produced {
                for &(d, e) in g { production.entry(d).or_default().push(e); }
            }
        }
        // One topological order for the whole expansion. Every candidate here
        // differs from `steps` by one step, so the order over the rest is the
        // same for all of them and computing it per candidate is quadratic work
        // for one answer.
        let ordered = {
            let order = get_order(ar, &steps);
            order_steps(ar, &order, &steps)
        };
        for si in 0..steps.len() {
            let step = steps[si];
            let (timeline, transform, iteration) = {
                let a = ar.appl(step);
                (a.timeline, a.transform, a.iteration)
            };
            let mock = ar.appl(step).produced.clone();
            let children = generate_applications(
                p, ar, timeline, &production, &current, transform, Some(&mock))?;
            for a in children {
                ar.appls[a as usize].iteration = iteration;
                let child_steps = relineage(p, ar, &ordered, step, a);
                // After the cascade, not before: a downstream signature moves
                // when its inputs are rebound, so a key built from the old ones
                // collides two candidates that differ downstream and silently
                // drops one.
                let sigs: Vec<ApplSig> =
                    child_steps.iter().map(|&s| ar.appl(s).sig).collect();
                let sig = ar.state_sig(&sigs);
                if !seen.insert(sig) { continue; }
                let mut child = RefinerState {
                    steps: child_steps, scores: [0.0, 0.0], valid: false, iteration: -1,
                    swapped_in: ar.appl(a).transform,
                };
                r.score(ar, &mut child, &mut sc)?;
                if child.valid && child.scores[0] > incumbent {
                    incumbent = child.scores[0];
                    improved = true;
                }
                let ck = states.len();
                states.push(child);
                frontier.push(ck);
            }
        }
        {
            // Through `reward_for` like the mcts site rather than as a raw 0/1,
            // or a policy configured not to estimate a value still accumulates
            // one here and the two callers disagree about what one config means.
            let reward = policy.reward_for(improved, 0.0, 0.0);
            policy.observe(swapped, reward);
        }
    }

    // First maximum wins, where numpy's introselect picked whichever index its
    // partition happened to leave in that slot.
    let scores: Vec<f64> = valids.iter().map(|&k| states[k].scores[1]).collect();
    let win = valids[argmax_index(&scores)];
    let steps = rectify(p, ar, given_appl, &states[win].steps.clone(), true, true)?;
    Ok(RefinerResult { steps, iterations: i, found_on: states[win].iteration })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A three-node chain with a branch, as `produced_from`: signature 0 is
    /// made from 1 and 2, 1 from 3, 2 from 3, 3 from nothing reachable.
    fn fixture() -> (SigMap<(u32, u32)>, Vec<EpSig>) {
        let mut pf: SigMap<(u32, u32)> = SigMap::default();
        pf.clear(5);
        let flat: Vec<EpSig> = vec![1, 2, /*0*/ 3, /*1*/ 3, /*2*/ 4 /*3*/];
        pf.insert(0, (0, 2));
        pf.insert(1, (2, 1));
        pf.insert(2, (3, 1));
        pf.insert(3, (4, 1));
        pf.insert(4, (0, 0));
        (pf, flat)
    }

    /// The whole claim the shortened walk rests on: stopping at the destination
    /// leaves that destination -- and every node recorded before it -- holding
    /// exactly the depth a walk to exhaustion would leave. If this ever stops
    /// being true the refiner silently scores against a different graph.
    #[test]
    fn a_shortened_walk_agrees_with_the_full_one_wherever_it_answers() {
        let (pf, flat) = fixture();
        let mut todo = Vec::new();
        let mut full: SigMap<i32> = SigMap::default();
        let done = depth_walk(&mut full, &mut todo, &pf, &flat, 5, 0, None).unwrap();
        assert!(done, "an exhaustive walk reports itself complete");

        for dest in 0u32..5 {
            let mut part: SigMap<i32> = SigMap::default();
            let done = depth_walk(&mut part, &mut todo, &pf, &flat, 5, 0, Some(dest)).unwrap();
            for k in 0u32..5 {
                if let Some(v) = part.get(k) {
                    assert_eq!(Some(v), full.get(k), "signature {k} differs from the full walk");
                }
            }
            if !done {
                assert_eq!(part.get(dest), full.get(dest), "the destination it stopped for");
            } else {
                // Ran to exhaustion, so it is the full map and a miss is an answer.
                assert_eq!(part.get(dest), full.get(dest));
            }
        }
    }

    /// A destination that cannot be reached forces the walk to exhaustion, so
    /// the map it leaves may be read for a miss.
    #[test]
    fn an_unreachable_destination_completes_the_map() {
        let (pf, flat) = fixture();
        let mut todo = Vec::new();
        let mut m: SigMap<i32> = SigMap::default();
        let done = depth_walk(&mut m, &mut todo, &pf, &flat, 5, 1, Some(0)).unwrap();
        assert!(done, "0 is not reachable backwards from 1, so the walk exhausted");
        assert_eq!(m.get(0), None);
    }
}
