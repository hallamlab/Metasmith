//! Pruning, ordering, and rewriting a plan's endpoints so its lineage is real.
//!
//! The search builds endpoints as it goes, with whatever lineage was known at
//! the time. `rectify` walks the finished plan from its roots and rebuilds every
//! produced endpoint carrying the full lineage of the step that made it. Only
//! then do the endpoint signatures -- and so the application signatures, and so
//! the plan's identity -- mean what they say.
//!
//! Two traps here, both of them faithful reproductions rather than choices.
//!
//! **`get_order` never keeps the first depth it assigned to an endpoint.** Its
//! guard is `if e in order: continue`, where `e` is an `Endpoint` and `order` is
//! keyed by *strings*. The test is therefore always false, and the last step to
//! produce an equal endpoint wins. Writing the obvious thing here -- first
//! writer wins, which is plainly what the line intends -- would change plan
//! ordering on any graph where two steps produce equal endpoints.
//!
//! **Endpoints are mutated in place.** `new_e.parents |= ...; RefreshHash()`
//! changes an endpoint everything else is already pointing at, and the change is
//! meant to be seen. This is why the arena holds endpoints by identity instead
//! of interning them.

use crate::det::{self, Map, Set};
use crate::model::{EpId, EpSig};
use crate::problem::Problem;
use crate::search::{ApplId, ApplSig, Arena, Group};

/// A key in `prune_steps`'s and `get_order`'s maps.
///
/// Python keys both by string and lets application signatures and endpoint keys
/// share one namespace -- they cannot collide there because an application
/// signature is always longer. Spelling the union out is the same thing, said
/// once instead of relied on.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Key {
    Step(ApplSig),
    Ep(EpSig),
}

/// Everything the last step transitively depends on, in reverse discovery
/// order. `prune_steps`, which assumes the last step is the target.
pub fn prune_steps(ar: &Arena, steps: &[ApplId]) -> Vec<ApplId> {
    let mut e2source: Map<EpSig, ApplId> = det::map();
    for &s in steps {
        for e in ar.appl(s).products() { e2source.insert(ar.eps.sig(e), s); }
    }

    #[derive(Clone, Copy)]
    enum Ref { A(ApplId), E(EpId) }

    let mut todo: std::collections::VecDeque<Ref> = std::collections::VecDeque::new();
    todo.push_back(Ref::A(*steps.last().expect("prune_steps needs a plan")));
    let mut seen: Set<Key> = det::set();
    let mut order: Vec<Ref> = Vec::new(); // insertion order, as `dict.values()`
    while let Some(node) = todo.pop_front() {
        let key = match node {
            Ref::A(a) => Key::Step(ar.appl(a).sig),
            Ref::E(e) => Key::Ep(ar.eps.sig(e)),
        };
        if !seen.insert(key) { continue; }
        order.push(node);
        match node {
            Ref::A(a) => {
                for e in ar.appl(a).used.values() { todo.push_back(Ref::E(e)); }
            }
            Ref::E(e) => {
                if let Some(&a) = e2source.get(&ar.eps.sig(e)) { todo.push_back(Ref::A(a)); }
            }
        }
    }
    let mut required: Vec<ApplId> =
        order.iter().filter_map(|r| match r { Ref::A(a) => Some(*a), _ => None }).collect();
    required.reverse();
    required
}

/// Depth of every step and endpoint, by peeling one layer of ready steps at a
/// time. `get_order`.
pub fn get_order(ar: &Arena, steps: &[ApplId]) -> Map<Key, i64> {
    let mut seen: Set<ApplSig> = det::set();
    let mut have: Set<EpSig> = det::set();
    let mut order: Map<Key, i64> = det::map();

    while seen.len() < steps.len() {
        // A list, not a set: `seen` already makes the signatures within one
        // layer unique, so taking them in `steps` order states the order instead
        // of inheriting a hash table's.
        let mut reachable: Vec<ApplId> = Vec::new();
        for &s in steps {
            let sig = ar.appl(s).sig;
            if seen.contains(&sig) { continue; }
            if ar.appl(s).used.values().any(|e| !have.contains(&ar.eps.sig(e))) { continue; }
            seen.insert(sig);
            reachable.push(s);
        }
        if reachable.is_empty() { break; } // cannot happen; here so it cannot loop
        for &s in &reachable {
            let a = ar.appl(s);
            let depth = if a.used.len() > 0 {
                a.used
                    .values()
                    .map(|e| order[&Key::Ep(ar.eps.sig(e))])
                    .max()
                    .expect("a step with inputs has at least one")
                    + 1
            } else {
                1
            };
            order.insert(Key::Step(a.sig), depth);
            // Unconditionally, because Python's guard tests an `Endpoint`
            // against a dict of strings and is therefore always false. The last
            // producer of an equal endpoint sets its depth.
            for e in a.products() { order.insert(Key::Ep(ar.eps.sig(e)), depth + 1); }
            for e in a.products() { have.insert(ar.eps.sig(e)); }
        }
    }
    let max_depth = order.values().copied().max().unwrap_or(-1) + 1;
    for &s in steps {
        order.entry(Key::Step(ar.appl(s).sig)).or_insert(max_depth);
    }
    order
}

/// `order_steps`: by depth, then by how many inputs a step binds. A stable sort,
/// because Python's is.
pub fn order_steps(ar: &Arena, order: &Map<Key, i64>, steps: &[ApplId]) -> Vec<ApplId> {
    let mut out = steps.to_vec();
    out.sort_by_key(|&s| {
        let a = ar.appl(s);
        order[&Key::Step(a.sig)]*10000 + a.used.len() as i64
    });
    out
}

/// First minimum, matching `argmin_index` on the integer depths.
fn argmin(values: &[i64]) -> usize {
    let mut best = 0;
    for i in 1..values.len() {
        if values[i] < values[best] { best = i; }
    }
    best
}

/// Rebuild a plan's endpoints with their true lineage. `rectify`.
pub fn rectify(
    p: &Problem, ar: &mut Arena, given_appl: ApplId, solution: &[ApplId],
    prune: bool, insert_given: bool,
) -> Result<Vec<ApplId>, String> {
    let source: Vec<ApplId> = if insert_given {
        std::iter::once(given_appl).chain(solution.iter().copied()).collect()
    } else {
        solution.to_vec()
    };
    // Copies, so the search's own applications are left alone. Python's copy is
    // shallow in exactly the same places: `used` gets a new dict (and is then
    // rewritten), `produced` a new list whose groups are replaced wholesale.
    let mut steps: Vec<ApplId> = source.iter().map(|&a| ar.clone_appl(a)).collect();

    let target_at = steps
        .iter()
        .position(|&s| ar.appl(s).is_terminal())
        .ok_or("no step produces nothing, so there is no target to place last")?;
    let last = steps.len() - 1;
    steps.swap(target_at, last);
    if prune { steps = prune_steps(ar, &steps); }

    // Keyed by *equality*, like Python's `dict[Endpoint, Endpoint]`. Its
    // companion `rev_emap` is written and never read on the Python side, and is
    // not carried here.
    let mut endpoint_map: Map<EpSig, EpId> = det::map();

    let node_order = get_order(ar, &steps);
    let mut todo = steps.clone();
    let mut order: Vec<i64> = todo.iter().map(|&s| node_order[&Key::Step(ar.appl(s).sig)]).collect();
    while !todo.is_empty() {
        let i = argmin(&order);
        let n = todo.len() - 1;
        todo.swap(i, n);
        order.swap(i, n);
        order.pop();
        let appl = todo.pop().expect("checked non-empty");
        fix_endpoints(p, ar, appl, &mut endpoint_map)?;
    }
    Ok(steps)
}

fn fix_endpoints(
    p: &Problem, ar: &mut Arena, a: ApplId, endpoint_map: &mut Map<EpSig, EpId>,
) -> Result<(), String> {
    let requires = p.transforms[ar.appl(a).transform as usize].requires.clone();
    let mut lineage: Vec<EpId> = Vec::new();
    for d in requires {
        let e = ar.appls[a as usize].used.get(d).ok_or_else(|| {
            format!("step of transform {} left requirement {d} unbound", ar.appl(a).transform)
        })?;
        let e = endpoint_map.get(&ar.eps.sig(e)).copied().unwrap_or(e);
        ar.appls[a as usize].used.set(d, e);
        lineage.push(e);
        lineage.extend_from_slice(ar.eps.parents(e));
    }
    let lineage = ar.ep_set(lineage);

    let groups: Vec<Group> = ar.appls[a as usize].produced.clone();
    let mut new_produced: Vec<Group> = Vec::with_capacity(groups.len());
    for g in &groups {
        let mut ng: Group = Vec::with_capacity(g.len());
        for &(d, e) in g {
            let old_sig = ar.eps.sig(e);
            // The original endpoint's own parents, kept only where they are
            // problem inputs -- everything else is about to be re-derived from
            // the step's actual inputs.
            let mut extra: Vec<EpId> = ar
                .eps
                .parents(e)
                .iter()
                .copied()
                .filter(|&x| p.inherent_parents.contains(&ar.eps.sig(x)))
                .collect();
            extra.extend_from_slice(&lineage);
            let extra = ar.ep_set(extra);
            let new_e = match endpoint_map.get(&old_sig).copied() {
                Some(mapped) => {
                    // In place: everything already pointing at `mapped` sees the
                    // wider lineage, and its signature changes underneath them.
                    ar.eps.extend_parents(mapped, &extra);
                    mapped
                }
                None => {
                    let ty = ar.eps.ty(e);
                    ar.eps.new_endpoint(ty, &extra)
                }
            };
            ng.push((d, new_e));
            endpoint_map.insert(old_sig, new_e);
        }
        new_produced.push(ng);
    }
    ar.appls[a as usize].produced = new_produced;
    ar.resign(p, a);
    Ok(())
}
