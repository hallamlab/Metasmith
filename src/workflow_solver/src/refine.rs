//! The refiner: single-edge swaps against a found plan, looking for a better one.
//!
//! It dominates the solve on the shipped templates and changes the plan it was
//! given on none of them: on a lineage-dense workflow every single-edge swap
//! breaks a lineage constraint, so single-swap refinement structurally cannot
//! improve one. It is not inert in general -- two branching tests do produce a
//! plan-changing refinement.
//!
//! **The AND in `validate` is ordered, and the order is the optimisation.** Its
//! terms are independent and side-effect-free, and the lineage term is both the
//! cheapest and the one that rejects nearly everything, so it runs first. A
//! `KeyError` from the prefilter means "cannot answer here", not "invalid", and
//! falls through to the full check.
//!
//! **One defect is reproduced here rather than fixed, knowingly.** `expand_node`
//! removes the step it is swapping by *signature*, which drops both members of a
//! colliding pair. This is a port, and a port that fixes things cannot be checked
//! against what it replaced.
//!
//! `score` runs once per expanded state and is nearly the whole cost of a solve,
//! so every table it needs comes out of `scratch.rs` rather than being allocated
//! and hashed per state. That module carries the argument for why swapping a
//! hash map for a flat array cannot move a plan.

use crate::det::{self, Map, Set};
use crate::model::EpSig;
use crate::problem::Problem;
use crate::rectify::rectify;
use crate::rng::{DecisionStream, argmax_index};
use crate::scratch::{Scratch, SigMap, SigSet};
use crate::search::{ApplId, ApplSig, Arena, StateSig, generate_applications};
use crate::smath::entropy;

/// Both phases weight the same three moves: two exploit arms and one explore
/// arm. Shared so the refiner and the mcts phase cannot drift apart.
pub const SELECTION_WEIGHTS: [i64; 3] = [75, 20, 5];
pub const SELECTION_TOP_K: usize = 1;

pub struct RefinerState {
    pub steps: Vec<ApplId>,
    pub scores: [f64; 2],
    pub valid: bool,
    pub iteration: i64,
}

pub struct RefinerResult {
    pub steps: Vec<ApplId>,
    pub iterations: i64,
    pub found_on: i64,
}

/// `_has_ancestor`, with the missing-producer case named instead of raised.
///
/// `None` is Python's `KeyError`: `produced_from` is indexed unguarded, and a
/// state the loop walk would have rejected first can reach here without one.
///
/// The tables come in as separate borrows rather than as a `&mut Scratch`
/// because this reads `produced_from` while writing its own `seen` -- two
/// disjoint fields of the same scratch, which the borrow checker will allow
/// only if it can see them apart.
fn has_ancestor(
    pf: &SigMap<(u32, u32)>, pf_flat: &[EpSig],
    seen: &mut SigSet, todo: &mut Vec<EpSig>, n_sigs: usize,
    e: EpSig, a: EpSig,
) -> Option<bool> {
    todo.clear();
    todo.push(e);
    seen.clear(n_sigs);
    seen.insert(e);
    while let Some(x) = todo.pop() {
        if x == a { return Some(true); }
        let (start, len) = pf.get(x)?;
        for i in start..start + len {
            let parent = pf_flat[i as usize];
            if seen.insert(parent) { todo.push(parent); }
        }
    }
    Some(false)
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
        let (start, len) = pf.get(n).ok_or_else(|| {
            format!("endpoint {n} is used but produced by no step")
        })?;
        for i in start..start + len {
            todo.push((pf_flat[i as usize], dd + 1));
        }
    }
    Ok(true)
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
    ) -> Option<bool> {
        for &s in std::iter::once(&self.given_appl).chain(steps.iter()) {
            let used = &ar.appl(s).used;
            for &(d, e) in &used.0 {
                for &parent in &self.p.dep_parents_ranked[d as usize] {
                    let constraint = used.get(parent)?;
                    let ok = has_ancestor(
                        &sc.pf, &sc.pf_flat, &mut sc.anc_seen, &mut sc.anc_todo, n_sigs,
                        ar.eps.sig(e), ar.eps.sig(constraint))?;
                    if !ok { return Some(false); }
                }
            }
        }
        Some(true)
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
    ) -> Option<bool> {
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
            if sc.ready.is_empty() { return Some(false); } // looped, or an input nothing makes
            for i in 0..sc.ready.len() {
                let s = sc.ready[i];
                for e in ar.appl(s).products() { sc.have.insert(ar.eps.sig(e)); }
            }
            std::mem::swap(&mut sc.pending, &mut sc.rest);
        }
        self.lineage_ok(ar, steps, sc, n_sigs)
    }

    fn validate(
        &self, ar: &Arena, steps: &[ApplId], sc: &mut Scratch, n_sigs: usize,
    ) -> Option<bool> {
        // `produced_from`, as one flat buffer of every step's inputs plus a
        // range per product.
        sc.pf.clear(n_sigs);
        sc.pf_flat.clear();
        for &s in steps {
            let start = sc.pf_flat.len() as u32;
            for e in ar.appl(s).used.values() { sc.pf_flat.push(ar.eps.sig(e)); }
            let len = sc.pf_flat.len() as u32 - start;
            for e in ar.appl(s).products() { sc.pf.insert(ar.eps.sig(e), (start, len)); }
        }
        if !steps.iter().any(|&s| ar.appl(s).is_terminal()) {
            return Some(false);
        }
        // Cheap term first; a `KeyError` here answers nothing, so fall through.
        let rejected = matches!(self.lineage_ok(ar, steps, sc, n_sigs), Some(false));
        if rejected { return Some(false); }
        self.is_valid(ar, steps, sc, n_sigs)
    }

    pub fn score(
        &self, ar: &Arena, state: &mut RefinerState, sc: &mut Scratch,
    ) -> Result<(), String> {
        // The signature universe grows as the search interns endpoints, but not
        // during a scoring: `ar` is shared, so every table below is sized once
        // here and the sizes hold for the whole call.
        let n_sigs = ar.eps.n_sigs();
        sc.begin(n_sigs);

        // One deliberate difference from Python, in *failure* rather than in
        // answer: the same lookup inside `_is_valid` is unguarded there, so a
        // state using an endpoint no step produces crashes the Python solve.
        // Here it is simply invalid.
        state.valid = self.validate(ar, &state.steps, sc, n_sigs).unwrap_or(false);
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

    let mut i: i64 = 0;
    while !frontier.is_empty() && (i as u32) < max_iters {
        i += 1;
        let idx = {
            let arm = rng.weighted_index(&SELECTION_WEIGHTS);
            if arm < SELECTION_WEIGHTS.len() - 1 {
                let scores: Vec<f64> = frontier.iter().map(|&k| states[k].scores[arm]).collect();
                rng.pick_top_k(&scores, SELECTION_TOP_K)
            } else {
                rng.bounded_int(frontier.len() as u64) as usize
            }
        };
        let n = frontier.len() - 1;
        frontier.swap(idx, n);
        let k = frontier.pop().expect("checked non-empty");
        states[k].iteration = i;
        if states[k].valid { valids.push(k); }

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
        for si in 0..steps.len() {
            let step = steps[si];
            let (step_sig, timeline, transform, iteration) = {
                let a = ar.appl(step);
                (a.sig, a.timeline, a.transform, a.iteration)
            };
            // NOTE: this drops *both* members of a colliding pair, which is a
            // latent defect. Preserved verbatim: this is a port.
            let base: Vec<ApplId> =
                steps.iter().copied().filter(|&s| ar.appl(s).sig != step_sig).collect();
            let base_sigs: Vec<ApplSig> = base.iter().map(|&s| ar.appl(s).sig).collect();
            let mock = ar.appl(step).produced.clone();
            let children = generate_applications(
                p, ar, timeline, &production, &current, transform, Some(&mock))?;
            for a in children {
                ar.appls[a as usize].iteration = iteration;
                let mut sigs = base_sigs.clone();
                sigs.push(ar.appl(a).sig);
                let sig = ar.state_sig(&sigs);
                if !seen.insert(sig) { continue; }
                let mut child_steps = base.clone();
                child_steps.push(a);
                let mut child = RefinerState {
                    steps: child_steps, scores: [0.0, 0.0], valid: false, iteration: -1,
                };
                r.score(ar, &mut child, &mut sc)?;
                let ck = states.len();
                states.push(child);
                frontier.push(ck);
            }
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
