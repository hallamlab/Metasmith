//! The refiner: single-edge swaps against a found plan, looking for a better one.
//!
//! It is 70% of the solve on the shipped templates and it never changes the plan
//! it was given on any of them -- on `metagenomics_from_paired_reads` it spends
//! 23 of 33 seconds proving that. It is not inert in general; two branching tests
//! do produce a plan-changing refinement. But on lineage-dense workflows every
//! single-edge swap breaks a lineage constraint, so single-swap refinement
//! structurally cannot improve them.
//!
//! Two defects are reproduced here rather than fixed, and knowingly:
//!
//! - **`_is_valid`'s loop branch is reachable but incomplete.** It asks whether
//!   any walk from the given application repeats an application *signature*,
//!   which equals cycle detection only if signature and object are in bijection
//!   -- and they are not. Property testing over 40,000 random graphs found 122
//!   disagreements in 30,000 once signatures could collide, every one of them a
//!   cycle the signature test *missed*. It fires 2,259 times in one solve of
//!   `sink-9391`, so it is emphatically live code. Porting it faithfully
//!   inherits the hole; dropping it as unreachable would be simply wrong.
//! - **The AND is ordered, and the order is the optimisation.** The three terms
//!   are independent and side-effect-free, and the loop walk dominates the solve
//!   while the lineage term is nearly free -- on the metagenomics template *all*
//!   19,683 validations fail on lineage, after paying for the walk. So lineage
//!   runs first. A `KeyError` from the prefilter means "cannot answer here", not
//!   "invalid", and falls through to the full check.

use crate::det::{self, Map, Set};
use crate::model::EpSig;
use crate::problem::Problem;
use crate::rectify::rectify;
use crate::rng::{DecisionStream, argmax_index};
use crate::search::{ApplId, ApplSig, Arena, Bindings, StateSig, generate_applications};
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
fn has_ancestor(
    ar: &Arena, produced_from: &Map<EpSig, Vec<EpSig>>, e: EpSig, a: EpSig,
) -> Option<bool> {
    let _ = ar;
    let mut todo = vec![e];
    let mut seen: Set<EpSig> = det::set();
    seen.insert(e);
    while let Some(x) = todo.pop() {
        if x == a { return Some(true); }
        for &parent in produced_from.get(&x)? {
            if seen.insert(parent) { todo.push(parent); }
        }
    }
    Some(false)
}

pub struct Refiner<'a> {
    pub p: &'a Problem,
    pub given_appl: ApplId,
}

impl<'a> Refiner<'a> {
    /// The lineage term on its own: every binding descends from whatever its
    /// dependency's lineage constraint was bound to.
    fn lineage_ok(
        &self, ar: &Arena, steps: &[ApplId], produced_from: &Map<EpSig, Vec<EpSig>>,
    ) -> Option<bool> {
        for &s in std::iter::once(&self.given_appl).chain(steps.iter()) {
            let used = ar.appl(s).used.clone();
            for &(d, e) in &used.0 {
                for &parent in &self.p.dep_parents_ranked[d as usize] {
                    let constraint = used.get(parent)?;
                    let ok = has_ancestor(
                        ar, produced_from, ar.eps.sig(e), ar.eps.sig(constraint))?;
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
    /// This replaced a forward walk over *consumers* that rejected a repeated
    /// `ApplSig` along a path. That walk reached a step as soon as **one** of
    /// its inputs was available and never asked about the others, so a cycle
    /// hanging off the side of it was invisible; `rectify` then rewrote those
    /// states into plans with unproduced inputs and no trace of a cycle. Both
    /// implementations carried it, and both are fixed together.
    ///
    /// Endpoints are held by `EpSig` (structure, matching Python's
    /// `set[Endpoint]` and what `get_order` uses, so a `true` here is the
    /// promise that ordering finds a total order) and steps by `ApplId`
    /// (identity, because two distinct applications may share a signature and
    /// both have to be runnable). The verdict does not depend on the order
    /// within a layer, so this adds no new site to the iteration-order
    /// contract.
    fn is_valid(
        &self, ar: &Arena, steps: &[ApplId],
        produced_from: &Map<EpSig, Vec<EpSig>>,
    ) -> Option<bool> {
        let mut have: Set<EpSig> = det::set();
        for e in ar.appl(self.given_appl).products() { have.insert(ar.eps.sig(e)); }
        let mut pending: Vec<ApplId> = steps.to_vec();
        while !pending.is_empty() {
            let mut ready: Vec<ApplId> = Vec::new();
            let mut rest: Vec<ApplId> = Vec::new();
            for &s in &pending {
                if ar.appl(s).used.values().all(|e| have.contains(&ar.eps.sig(e))) {
                    ready.push(s);
                } else {
                    rest.push(s);
                }
            }
            if ready.is_empty() { return Some(false); } // looped, or an input nothing makes
            for &s in &ready {
                for e in ar.appl(s).products() { have.insert(ar.eps.sig(e)); }
            }
            pending = rest;
        }
        self.lineage_ok(ar, steps, produced_from)
    }

    fn validate(&self, ar: &Arena, steps: &[ApplId]) -> Option<bool> {
        let mut produced_from: Map<EpSig, Vec<EpSig>> = det::map();
        for &s in steps {
            let from: Vec<EpSig> =
                ar.appl(s).used.values().map(|e| ar.eps.sig(e)).collect();
            for e in ar.appl(s).products() { produced_from.insert(ar.eps.sig(e), from.clone()); }
        }
        if !steps.iter().any(|&s| ar.appl(s).is_terminal()) {
            return Some(false);
        }
        // Cheap term first; a `KeyError` here answers nothing, so fall through.
        let rejected = matches!(self.lineage_ok(ar, steps, &produced_from), Some(false));
        if rejected { return Some(false); }
        self.is_valid(ar, steps, &produced_from)
    }

    pub fn score(&self, ar: &Arena, state: &mut RefinerState) -> Result<(), String> {
        // One deliberate difference from Python, and it is a difference in
        // *failure*, not in answer. The prefilter's `KeyError` is caught there
        // and falls through, but the same lookup inside `_is_valid` is not, so a
        // state using an endpoint no step produces crashes the Python solve.
        // Here it is simply invalid. Reproducing a crash has no value, and no
        // state in the corpus or the four templates reaches it.
        state.valid = self.validate(ar, &state.steps).unwrap_or(false);
        let steps = &state.steps;

        let mut used_as_lineage: Set<EpSig> = det::set();
        for &s in steps {
            let used = &ar.appl(s).used;
            for &(d, _) in &used.0 {
                for &parent in &self.p.dep_parents_ranked[d as usize] {
                    let c = used.get(parent).ok_or_else(|| {
                        format!("lineage constraint {parent} of requirement {d} is unbound")
                    })?;
                    used_as_lineage.insert(ar.eps.sig(c));
                }
            }
        }
        // An ordered count, because `entropy` sums in the caller's order.
        let mut lineage_usage: Vec<(EpSig, i64)> = Vec::new();
        for &s in steps {
            for e in ar.appl(s).used.values() {
                let es = ar.eps.sig(e);
                if !used_as_lineage.contains(&es) { continue; }
                match lineage_usage.iter_mut().find(|(k, _)| *k == es) {
                    Some(slot) => slot.1 += 1,
                    None => lineage_usage.push((es, 1)),
                }
            }
        }
        let counts: Vec<i64> = lineage_usage.iter().map(|(_, c)| *c).collect();
        let e_score = entropy(&counts);

        let mut product2producer: Map<EpSig, ApplId> = det::map();
        for &s in steps {
            for e in ar.appl(s).products() { product2producer.insert(ar.eps.sig(e), s); }
        }
        // One walk per distinct *source*: the destination is a plain equality
        // test during traversal and `seen` guarantees one visit per node, so a
        // depth map answers every destination asked of it. The two quirks are
        // load-bearing and both feed the score -- a LIFO stack with an up-front
        // `seen` check records depth at first pop rather than the true maximum,
        // and the `> 0` test below makes a distance of zero indistinguishable
        // from not-found.
        let mut depth_maps: Map<EpSig, Map<EpSig, i64>> = det::map();
        let n_steps = steps.len() as f64;
        let mut lin_distances: Vec<f64> = Vec::new();
        for &s in steps {
            let requires = self.p.transforms[ar.appl(s).transform as usize].requires.clone();
            for d in requires {
                for &lin in &self.p.dep_parents_ranked[d as usize] {
                    let used = &ar.appl(s).used;
                    let (Some(e), Some(pe)) = (used.get(d), used.get(lin)) else {
                        return Err(format!("requirement {d} or its constraint {lin} is unbound"));
                    };
                    let (es, pes) = (ar.eps.sig(e), ar.eps.sig(pe));
                    if !depth_maps.contains_key(&es) {
                        let mut depths: Map<EpSig, i64> = det::map();
                        let mut todo = vec![(es, 0i64)];
                        while let Some((n, dd)) = todo.pop() {
                            if depths.contains_key(&n) { continue; }
                            depths.insert(n, dd);
                            let prod = *product2producer.get(&n).ok_or_else(|| {
                                format!("endpoint {n} is used but produced by no step")
                            })?;
                            for pe in ar.appl(prod).used.values() {
                                todo.push((ar.eps.sig(pe), dd + 1));
                            }
                        }
                        depth_maps.insert(es, depths);
                    }
                    let max_d = depth_maps[&es].get(&pes).copied().unwrap_or(-1);
                    lin_distances.push(if max_d > 0 { max_d as f64/n_steps } else { 1.0 });
                }
            }
        }
        let lin_score = if lin_distances.is_empty() {
            0.0
        } else {
            let mut acc = 0.0f64;
            for d in &lin_distances { acc += d; }
            -acc/lin_distances.len() as f64
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
    r.score(ar, &mut states[0])?;

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
        // depends on which step is being swapped, and about 90% of candidates are
        // discarded as duplicates, so both are built once.
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
                r.score(ar, &mut child)?;
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
