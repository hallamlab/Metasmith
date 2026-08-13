//! The search proper: growing a plan one application at a time, and merging the
//! timelines that a multi-sample problem branches into.
//!
//! The solver is not searching the workflow graph. Each node it visits *is* a
//! workflow graph, and the search is over the space of them. A `SolverState` is
//! one such graph in progress; the frontier holds candidate applications rather
//! than states, and an application is applied to every live timeline it belongs
//! to.
//!
//! `merge_states` is where multi-sample problems come back together. Two
//! timelines that solved separately are reconciled by substituting one's
//! endpoints for the other's wherever the substitution keeps every downstream
//! consumer satisfied. It is the least obvious code in the solver and it is
//! transcribed here rather than rethought.

use crate::det::{self, Map, Set};
use crate::model::{DepId, EpId, EpSig, TransformId};
use crate::problem::Problem;
use crate::rectify::{get_order, order_steps, prune_steps, rectify};
use crate::refine::{SELECTION_TOP_K, SELECTION_WEIGHTS, refine};
use crate::rng::DecisionStream;
use crate::search::{ApplId, ApplSig, Arena, Bindings, Group, generate_applications};

pub struct SolverState {
    pub k: i64,
    pub steps: Vec<ApplId>,
    pub production: Map<DepId, Vec<EpId>>,
    pub candidates: Set<TransformId>,
}

impl Clone for SolverState {
    fn clone(&self) -> Self {
        Self {
            k: self.k,
            steps: self.steps.clone(),
            production: self.production.clone(),
            candidates: self.candidates.clone(),
        }
    }
}

pub struct MctsResult {
    pub complete: bool,
    pub steps: Vec<ApplId>,
    /// `merged_endpoints`: what a surviving endpoint now stands in for after two
    /// timelines were reconciled. Keyed by equality, holding identities --
    /// `plan.py` looks these up against the problem's given endpoints, so the
    /// objects have to be rebuildable, not just nameable.
    pub merged: Vec<(EpSig, EpId, Vec<EpId>)>,
    pub iterations: i64,
    pub refiner_iterations: Vec<(i64, i64)>,
}

/// The timeline tree. A branching application mints one child id per sample
/// group, and an application queued against a timeline applies to every
/// descendant of it that is still live.
struct Timelines {
    last: i64,
    children: Map<i64, Vec<i64>>,
}

impl Timelines {
    fn new() -> Self { Self { last: -1, children: det::map() } }
    fn mint(&mut self, source: i64) -> i64 {
        self.last += 1;
        self.children.entry(source).or_default().push(self.last);
        self.last
    }
    fn descendants(&self, k: i64) -> Set<i64> {
        let mut todo = vec![k];
        let mut seen: Set<i64> = det::set();
        while let Some(s) = todo.pop() {
            if !seen.insert(s) { continue; }
            if let Some(cs) = self.children.get(&s) { todo.extend(cs.iter().copied()); }
        }
        seen
    }
}

pub struct Search<'a> {
    pub p: &'a Problem,
    pub given_appl: ApplId,
}

impl<'a> Search<'a> {
    fn score_appl(&self, ar: &mut Arena, a: ApplId) {
        let tr = ar.appl(a).transform;
        let dist = self.p.distance[&tr] as f64;
        let opportunity = self.p.opportunity[&tr] as f64;
        ar.appls[a as usize].score = [
            1.0 - dist/self.p.max_distance as f64,
            1.0 - 1.0/(1.0 + opportunity/10.0),
        ];
    }

    /// One application, applied to one timeline, giving the states that result.
    ///
    /// Multi-group applications carry two different intents behind one data
    /// shape. The synthesized `given` transform's groups are *alternatives* --
    /// one sample family each -- so they branch into separate timelines. Every
    /// other transform's groups are co-produced by a single invocation and stay
    /// in one timeline, or a downstream step requiring two of its slots could
    /// never apply. The DSL is symmetric, so the two are told apart by identity
    /// against the given transform.
    fn expand(
        &self, ar: &mut Arena, tl: &mut Timelines, state: &SolverState, a: ApplId,
    ) -> Vec<SolverState> {
        let appl = ar.appl(a);
        let branching = appl.transform == self.p.given_index && appl.produced.len() > 1;
        let mut out = Vec::new();
        if branching {
            let groups = appl.produced.clone();
            let (used, transform, score) = (appl.used.clone(), appl.transform, appl.score);
            let ks: Vec<i64> = groups.iter().map(|_| tl.mint(state.k)).collect();
            for (group, k) in groups.into_iter().zip(ks) {
                let mut candidates = state.candidates.clone();
                let mut production = state.production.clone();
                for &(d, _) in &group {
                    if let Some(linked) = self.p.product2consumer.get(&d) {
                        candidates.extend(linked.iter().copied());
                    }
                }
                for &(d, e) in &group { production.entry(d).or_default().push(e); }
                let variant = ar.new_appl(self.p, k, transform, used.clone());
                ar.appls[variant as usize].produced = vec![group];
                ar.appls[variant as usize].score = score;
                let mut steps = state.steps.clone();
                steps.push(variant);
                out.push(SolverState { k, steps, production, candidates });
            }
        } else {
            let mut candidates = state.candidates.clone();
            let mut production = state.production.clone();
            for g in appl.produced.clone() {
                for (d, e) in g {
                    if let Some(linked) = self.p.product2consumer.get(&d) {
                        candidates.extend(linked.iter().copied());
                    }
                    production.entry(d).or_default().push(e);
                }
            }
            let mut steps = state.steps.clone();
            steps.push(a);
            out.push(SolverState { k: state.k, steps, production, candidates });
        }
        out
    }

    /// The transforms to draw children from: the ones this state's production
    /// has unlocked, in rank order, then the ones that need no inputs at all.
    ///
    /// A transform in both lists is yielded twice, exactly as Python does -- and
    /// that is not harmless, which is why `children_of` exists rather than one
    /// function returning every child at once.
    fn candidate_transforms(&self, state: &SolverState) -> Vec<TransformId> {
        let mut candidates: Vec<TransformId> = state.candidates.iter().copied().collect();
        candidates.sort_unstable(); // the arena index *is* the rank
        candidates.extend(self.p.free_transforms.iter().copied());
        candidates
    }

    /// One transform's children, against the blacklist *as it stands now*.
    ///
    /// The caller has to fold each batch's signatures into the blacklist before
    /// asking for the next. `generate_child_nodes` is a generator on the Python
    /// side and its caller adds every child's signature to `frontier_signatures`
    /// as it consumes them, so a transform reached later in the same expansion
    /// sees the earlier transforms' children already blacklisted. Generating
    /// every batch against one frozen blacklist looks equivalent and is not: it
    /// puts an extra application on the frontier, which changes what the explore
    /// arm draws, which changes the plan.
    fn children_of(
        &self, ar: &mut Arena, state: &SolverState, blacklist: &Set<ApplSig>, tr: TransformId,
    ) -> Result<Vec<ApplId>, String> {
        generate_applications(self.p, ar, state.k, &state.production, blacklist, tr, None)
    }
}

/// `merge_states`: fold `alt` into `source`, substituting endpoints wherever the
/// substitution leaves every downstream consumer satisfied.
fn merge_states(
    p: &Problem, ar: &mut Arena, given_appl: ApplId, source: &mut SolverState,
    alt: &SolverState, merged_endpoints: &mut Vec<(EpSig, EpId, Vec<EpId>)>,
) -> Result<(), String> {
    let mut e2consumer: Map<EpSig, Vec<ApplId>> = det::map();
    for &s in &alt.steps {
        for e in ar.appl(s).used.values() {
            e2consumer.entry(ar.eps.sig(e)).or_default().push(s);
        }
    }

    let source_timelines: Set<i64> = source.steps.iter().map(|&s| ar.appl(s).timeline).collect();
    let alt_timelines: Set<i64> = alt.steps.iter().map(|&s| ar.appl(s).timeline).collect();
    let mut source_tr2appl: Map<TransformId, Vec<ApplId>> = det::map();
    for &s in &source.steps {
        if alt_timelines.contains(&ar.appl(s).timeline) { continue; }
        source_tr2appl.entry(ar.appl(s).transform).or_default().push(s);
    }
    let mut to_check: Vec<ApplId> = alt
        .steps
        .iter()
        .copied()
        .filter(|&s| !source_timelines.contains(&ar.appl(s).timeline))
        .collect();
    to_check.reverse(); // target -> given

    // Can `alt_step` be served by a step already in `source`? Only if every
    // consumer of the endpoints that would be swapped still accepts what it gets
    // instead.
    let substitute = |ar: &Arena, alt_step: ApplId| -> Option<ApplId> {
        let candidates = source_tr2appl.get(&ar.appl(alt_step).transform)?;
        'candidate: for &src_step in candidates {
            let mut subs: Vec<(EpId, EpId)> = Vec::new();
            for &(d, se) in &ar.appl(src_step).used.0 {
                // Python raises `KeyError` here. It cannot fire: the two steps
                // apply the same transform, so they bind the same requirements.
                let ae = ar.appl(alt_step).used.get(d)?;
                subs.push((ae, se));
            }
            for (sg, ag) in ar.appl(src_step).produced.iter().zip(ar.appl(alt_step).produced.iter())
            {
                for &(d, se) in sg {
                    // In the current interface an output group cannot share a
                    // dependency with another group of the same transform, so
                    // this branch is taken for a whole group or for none of it.
                    let Some(ae) = ag.iter().find(|(k, _)| *k == d).map(|(_, v)| *v) else {
                        let _ = se;
                        continue;
                    };
                    subs.push((ae, se));
                }
            }
            for (ae, se) in subs {
                let Some(consumers) = e2consumer.get(&ar.eps.sig(ae)) else { continue };
                for &c in consumers {
                    if ar.appl(c).sig == ar.appl(alt_step).sig { continue; }
                    for &(d, e) in &ar.appl(c).used.0 {
                        if ar.eps.sig(e) != ar.eps.sig(ae) { continue; }
                        if !p.ep_is_a(&ar.eps, se, d) { continue 'candidate; }
                    }
                }
            }
            return Some(src_step);
        }
        None
    };

    let mut to_merge: Vec<(ApplId, ApplId)> = Vec::new();
    let mut to_add_from_alt: Vec<ApplId> = Vec::new();
    for &step in &to_check {
        match substitute(ar, step) {
            Some(src) => to_merge.push((step, src)),
            None => to_add_from_alt.push(step),
        }
    }
    to_add_from_alt.reverse(); // given -> target
    to_merge.reverse();

    let common: Vec<ApplId> = alt
        .steps
        .iter()
        .copied()
        .filter(|&s| source_timelines.contains(&ar.appl(s).timeline))
        .collect();
    let merged_sigs: Set<ApplSig> = to_merge.iter().map(|&(_, s)| ar.appl(s).sig).collect();
    let from_src: Vec<ApplId> = source
        .steps
        .iter()
        .copied()
        .filter(|&s| {
            !alt_timelines.contains(&ar.appl(s).timeline) && !merged_sigs.contains(&ar.appl(s).sig)
        })
        .collect();

    let mut merged_steps: Vec<ApplId> = Vec::new();
    let mut swapped: Map<EpSig, EpId> = det::map();
    for &(alt_step, src_step) in &to_merge {
        for i in 0..ar.appl(alt_step).used.0.len() {
            let (ad, ae) = ar.appl(alt_step).used.0[i];
            let Some(se) = ar.appl(src_step).used.get(ad) else { continue };
            let se = swapped.get(&ar.eps.sig(se)).copied().unwrap_or(se);
            ar.appls[src_step as usize].used.set(ad, se);
            swapped.insert(ar.eps.sig(ae), se);
            let ses = ar.eps.sig(se);
            let aes = ar.eps.sig(ae);
            // `merged_endpoints.get(se, {se}) | {ae}` -- a *set*, so an endpoint
            // merged with an equal one gives a class of size one, and `plan.py`
            // skips classes smaller than two. Pushing both would quietly turn a
            // no-op merge into a real one.
            match merged_endpoints.iter_mut().find(|(k, _, _)| *k == ses) {
                Some((_, _, v)) => {
                    if !v.iter().any(|&x| ar.eps.sig(x) == aes) { v.push(ae); }
                }
                None if ses == aes => merged_endpoints.push((ses, se, vec![se])),
                None => merged_endpoints.push((ses, se, vec![se, ae])),
            }
        }
        // Groups the source step does not already have are carried over whole.
        let mut groups = ar.appl(src_step).produced.clone();
        for mp in ar.appl(alt_step).produced.clone() {
            let mk: Set<DepId> = mp.iter().map(|(d, _)| *d).collect();
            let dup = groups.iter().any(|g| {
                let gk: Set<DepId> = g.iter().map(|(d, _)| *d).collect();
                gk == mk
            });
            if dup { continue; }
            groups.push(mp);
        }
        ar.appls[src_step as usize].produced = groups;
        ar.resign(p, src_step);
        merged_steps.push(src_step);
    }
    for &s in &merged_steps {
        let mut groups = ar.appl(s).produced.clone();
        for g in groups.iter_mut() {
            for slot in g.iter_mut() {
                if let Some(&ne) = swapped.get(&ar.eps.sig(slot.1)) { slot.1 = ne; }
            }
        }
        ar.appls[s as usize].produced = groups;
    }
    for &s in &to_add_from_alt {
        let mut groups = ar.appl(s).produced.clone();
        for g in groups.iter_mut() {
            for slot in g.iter_mut() {
                if let Some(&ne) = swapped.get(&ar.eps.sig(slot.1)) { slot.1 = ne; }
            }
        }
        ar.appls[s as usize].produced = groups;
    }

    source.k = alt.k;
    let mut steps = common;
    steps.extend(from_src);
    steps.extend(to_add_from_alt);
    steps.extend(merged_steps);
    let order = get_order(ar, &steps);
    let steps = order_steps(ar, &order, &steps);
    source.steps = rectify(p, ar, given_appl, &steps, false, false)?;
    Ok(())
}

pub fn mcts(
    p: &Problem, ar: &mut Arena, rng: &mut DecisionStream, given_appl: ApplId,
) -> Result<MctsResult, String> {
    let s = Search { p, given_appl };
    let mut tl = Timelines::new();
    let start = SolverState {
        k: tl.mint(-1), steps: Vec::new(), production: det::map(), candidates: det::set(),
    };
    let mut timelines = vec![start];
    s.score_appl(ar, given_appl);
    let mut frontier: Vec<ApplId> = vec![given_appl];
    let mut frontier_sigs: Set<ApplSig> = det::set();
    frontier_sigs.insert(ar.appl(given_appl).sig);
    let mut solved: Option<SolverState> = None;
    let mut merged_endpoints: Vec<(EpSig, EpId, Vec<EpId>)> = Vec::new();
    let mut refiner_iterations: Vec<(i64, i64)> = Vec::new();
    let mut i: i64 = 0;

    while !frontier.is_empty() && (i as u32) < p.max_iter {
        i += 1;
        // The frontier, per iteration, when `MSM_SOLVER_TRACE` is set. Together
        // with `rng`'s decision trace this is how a differential failure gets
        // localised: the decisions say *when* the two sides parted, and this
        // says *what they were choosing between*. Finding the generator/batching
        // bug in `children_of` took one run of each. stderr, so it can never be
        // mistaken for the reply.
        if std::env::var_os("MSM_SOLVER_TRACE").is_some() {
            let f: Vec<String> = frontier
                .iter()
                .map(|&a| format!("t{}@{}", ar.appl(a).transform, ar.appl(a).timeline))
                .collect();
            eprintln!("IT {i} frontier=[{}]", f.join(","));
        }
        let idx = {
            let arm = rng.weighted_index(&SELECTION_WEIGHTS);
            if arm < SELECTION_WEIGHTS.len() - 1 {
                let scores: Vec<f64> = frontier.iter().map(|&a| ar.appl(a).score[arm]).collect();
                rng.pick_top_k(&scores, SELECTION_TOP_K)
            } else {
                rng.bounded_int(frontier.len() as u64) as usize
            }
        };
        let n = frontier.len() - 1;
        frontier.swap(idx, n);
        let node = frontier.pop().expect("checked non-empty");
        ar.appls[node as usize].iteration = i;

        let live = tl.descendants(ar.appl(node).timeline);
        let sources: Vec<SolverState> =
            timelines.iter().filter(|st| live.contains(&st.k)).cloned().collect();
        let carry: Vec<SolverState> =
            timelines.iter().filter(|st| !live.contains(&st.k)).cloned().collect();
        let mut next: Vec<SolverState> = Vec::new();
        for st in &sources { next.extend(s.expand(ar, &mut tl, st, node)); }

        let mut remain: Vec<SolverState> = Vec::new();
        for mut st in next {
            let last_tr = st.steps.last().map(|&a| ar.appl(a).transform);
            if last_tr != Some(p.target_index) {
                remain.push(st);
                continue;
            }
            // Pruned before refining so that "one producer per endpoint" still
            // holds; after merging it does not, and the refiner assumes it.
            st.steps = prune_steps(ar, &st.steps);
            let refined = refine(p, ar, rng, given_appl, &st.steps, p.max_refine)?;
            refiner_iterations.push((refined.found_on, refined.iterations));
            let order = get_order(ar, &refined.steps);
            st.steps = order_steps(ar, &order, &refined.steps);
            match solved.as_mut() {
                Some(dst) => merge_states(p, ar, given_appl, dst, &st, &mut merged_endpoints)?,
                None => solved = Some(st),
            }
        }

        if remain.is_empty() && carry.is_empty() {
            let Some(st) = solved else { break };
            return Ok(MctsResult {
                complete: true,
                steps: st.steps,
                merged: merged_endpoints,
                iterations: i,
                refiner_iterations,
            });
        }
        for st in remain.iter_mut() {
            let mut applied: Set<TransformId> = det::set();
            for tr in s.candidate_transforms(st) {
                for child in s.children_of(ar, st, &frontier_sigs, tr)? {
                    s.score_appl(ar, child);
                    ar.appls[child as usize].iteration = -i;
                    frontier_sigs.insert(ar.appl(child).sig);
                    applied.insert(ar.appl(child).transform);
                    frontier.push(child);
                }
            }
            st.candidates.retain(|t| !applied.contains(t)); // all possibilities per tr explored
        }
        timelines = carry;
        timelines.extend(remain);
    }

    // Falling out of the loop is not the same as failing. The early return above
    // fires only when every timeline resolves on the same pass; a search that
    // instead runs its frontier down still holds a merged solution for the
    // timelines that did solve, and on multi-sample problems that is the normal
    // exit. What distinguishes "no answer" is having no solved state at all.
    let complete = solved.is_some();
    let steps = match solved {
        Some(st) => st.steps,
        None => timelines
            .first()
            .map(|st| st.steps.clone())
            .ok_or("the search ended with no timeline at all")?,
    };
    Ok(MctsResult { complete, steps, merged: merged_endpoints, iterations: i, refiner_iterations })
}

/// Build the synthesized `given` application: one product group per given group,
/// each endpoint bound to the dependency the encoder paired it with.
pub fn build_given_appl(p: &Problem, ar: &mut Arena) -> Result<ApplId, String> {
    let a = ar.new_appl(p, 0, p.given_index, Bindings::default());
    let groups = &p.transforms[p.given_index as usize].produces;
    if groups.len() != p.given.len() {
        return Err(format!(
            "the given transform declares {} product groups but {} were supplied",
            groups.len(), p.given.len()
        ));
    }
    let produced: Vec<Group> = groups
        .iter()
        .zip(p.given.iter())
        .map(|(deps, eps)| deps.iter().copied().zip(eps.iter().copied()).collect())
        .collect();
    ar.appls[a as usize].produced = produced;
    Ok(a)
}
