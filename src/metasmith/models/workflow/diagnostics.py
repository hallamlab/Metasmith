from __future__ import annotations

import json
from dataclasses import dataclass, field

from ..libraries import DataInstance, TransformInstance
from ..solver import Dependency, Endpoint, Transform
from ..solver import Solution as SolverResult


@dataclass
class PlanHint:
    kind: str
    target: str
    message: str
    chain: list[str] = field(default_factory=list)
    candidate_transforms: list[str] = field(default_factory=list)
    near_misses: list[str] = field(default_factory=list)


# By properties only: a lineage constraint that blocks a route is what the
# lineage_mismatch hint reports.
def _forward_closure(givens: set[Endpoint], transform2inst: dict[Transform, TransformInstance]) -> list:
    produced: list = list(givens)
    satisfied: set[str] = set()
    pending = list(transform2inst)
    while True:
        fired = []
        for model in pending:
            ok = True
            for req in model.requires:
                if req.key in satisfied:
                    continue
                if any(e.IsA(req) for e in produced):
                    satisfied.add(req.key)
                    continue
                ok = False
                break
            if ok:
                fired.append(model)
        if not fired:
            return produced
        for model in fired:
            for pgroup in model.produces:
                produced.extend(pgroup)
        pending = [m for m in pending if m not in fired]


def _diagnose_plan_failure(
    target_model: Transform,
    target_names: list[str],
    given_map: dict[Endpoint, list[DataInstance]],
    transform2inst: dict[Transform, TransformInstance],
    solver_result: SolverResult|None,
    type_lookups: list = None,
    max_hops: int = 6,
    max_hints_per_target: int = 4,
    near_miss_top_n: int = 3,
) -> list[PlanHint]:
    from collections import deque

    hints: list[PlanHint] = []

    all_givens: set[Endpoint] = set(given_map.keys())

    def _matches_any_given(d: Dependency) -> bool:
        return any(g.IsA(d) for g in all_givens)

    def _producers_of(demand: Dependency) -> list[Transform]:
        out: list[Transform] = []
        for model in transform2inst:
            hit = False
            for pgroup in model.produces:
                for p in pgroup:
                    if p.IsA(demand):
                        hit = True
                        break
                if hit:
                    break
            if hit:
                out.append(model)
        return out

    _name_cache: dict = {}
    for d, nm in zip(target_model.requires, target_names):
        _name_cache.setdefault(Endpoint(d.properties), nm)
    for ep, insts in given_map.items():
        for inst in insts:
            if inst.dtype_name:
                _name_cache.setdefault(ep, inst.dtype_name)
                break
    for lookup in (type_lookups or []):
        try:
            for ns, tlib in lookup.types.items():
                for type_name, ep in tlib.types.items():
                    _name_cache.setdefault(ep, f"{ns}::{type_name}")
        except Exception:
            pass

    def _name(d) -> str:
        cached = _name_cache.get(d)
        if cached:
            return cached
        for ep, nm in _name_cache.items():
            if ep.properties == d.properties:
                _name_cache[d] = nm
                return nm
        if not d.properties:
            return "<unspecified>"
        return "{" + ", ".join(sorted(d.properties)) + "}"

    def _model_name(model: Transform|None) -> str:
        if model is None:
            return "<target>"
        ti = transform2inst.get(model)
        if ti is not None and ti.name:
            return ti.name
        return "<unnamed>"

    def _jaccard(a: set[str], b: set[str]) -> float:
        u = a | b
        return len(a & b) / len(u) if u else 0.0

    def _property_keys(props: set[str]) -> set[str]:
        keys = set()
        for p in props:
            try:
                obj = json.loads(p)
                if isinstance(obj, dict):
                    keys.update(obj.keys())
                    continue
            except (json.JSONDecodeError, TypeError):
                pass
            keys.add(p)
        return keys

    def _shape_key(d) -> str:
        return "|".join(sorted(d.properties)) or "<unspecified>"

    def _similarity_to_givens(demand) -> float:
        if not all_givens:
            return 0.0
        best = max(
            (_jaccard(g.properties, demand.properties) for g in all_givens),
            default=0.0,
        )
        if best > 0:
            return best
        dkeys = _property_keys(demand.properties)
        if not dkeys:
            return 0.0
        return max(
            (_jaccard(_property_keys(g.properties), dkeys) * 0.5 for g in all_givens),
            default=0.0,
        )

    def _rank_near_misses_among_givens(demand: Dependency) -> list[str]:
        scored: list[tuple[float, str, str, list[str]]] = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):
                continue
            score = _jaccard(ep.properties, demand.properties)
            if score <= 0:
                continue
            overlap = sorted(ep.properties & demand.properties)
            for inst in insts:
                scored.append((score, inst.dtype_name or _name(ep), str(inst.path), overlap))
        scored.sort(key=lambda x: -x[0])
        out = [
            f"{name} @ {path} (overlap {score:.2f}: {overlap})"
            for score, name, path, overlap in scored[:near_miss_top_n]
        ]
        if out:
            return out
        demand_keys = _property_keys(demand.properties)
        if not demand_keys:
            return []
        key_scored: list[tuple[float, str, str, list[str]]] = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):
                continue
            ep_keys = _property_keys(ep.properties)
            key_score = _jaccard(ep_keys, demand_keys)
            if key_score <= 0:
                continue
            shared_keys = sorted(ep_keys & demand_keys)
            for inst in insts:
                key_scored.append((key_score, inst.dtype_name or _name(ep), str(inst.path), shared_keys))
        key_scored.sort(key=lambda x: -x[0])
        return [
            f"{name} @ {path} (shape-match {score:.2f}: shared keys {keys})"
            for score, name, path, keys in key_scored[:near_miss_top_n]
        ]

    def _rank_near_misses_among_products(demand: Dependency) -> list[str]:
        scored: list[tuple[float, str, list[str]]] = []
        for model in transform2inst:
            for pgroup in model.produces:
                for p in pgroup:
                    if p.IsA(demand):
                        continue
                    score = _jaccard(p.properties, demand.properties)
                    if score <= 0:
                        continue
                    overlap = sorted(p.properties & demand.properties)
                    scored.append((score, _model_name(model), overlap))
        scored.sort(key=lambda x: -x[0])
        seen: set[str] = set()
        out: list[str] = []
        for score, name, overlap in scored:
            if name in seen:
                continue
            seen.add(name)
            out.append(f"{name} produces a near-match (overlap {score:.2f}: {overlap})")
            if len(out) >= near_miss_top_n:
                break
        return out


    def _too_general_givens(demand) -> list[tuple[Endpoint, list[DataInstance]]]:
        out = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):
                continue
            if demand.properties > ep.properties:
                out.append((ep, insts))
        return out

    def _retypings(demand, given_ep: Endpoint, limit: int = 4) -> list[str]:
        found: list[tuple[int, str]] = []
        for ep, name in _name_cache.items():
            props = getattr(ep, "properties", None)
            if not props:
                continue
            if not (props >= demand.properties and props >= given_ep.properties):
                continue
            found.append((len(props - given_ep.properties), name))
        found.sort()
        seen: set[str] = set()
        out: list[str] = []
        for _, name in found:
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
            if len(out) >= limit:
                break
        return out

    def _unmet_parents(demand) -> list[str]:
        out = []
        for parent in getattr(demand, "parents", None) or ():
            if any(g.properties >= parent.properties for g in all_givens):
                continue
            name = _name(parent)
            if name not in out:
                out.append(name)
        return out

    reachable = _forward_closure(all_givens, transform2inst)

    def _reachable(d) -> bool:
        return any(e.IsA(d) for e in reachable)

    necessary: dict[str, frozenset[str]] = {}
    leaf_of: dict[str, Dependency] = {}
    required_by: dict[str, set[str]] = {}

    # The unproduced requirements every route to `d` needs: a union over one
    # producer's unmet requirements, intersected across producers. A route that
    # cycles back to a demand still being computed is no route.
    def _necessary(d, stack: frozenset[str]) -> frozenset[str]|None:
        if d.key in necessary:
            return necessary[d.key]
        if d.key in stack:
            return None
        producers = _producers_of(d)
        if not producers:
            leaf_of[d.key] = d
            necessary[d.key] = frozenset({d.key})
            return necessary[d.key]
        routes: list[frozenset[str]] = []
        for prod in producers:
            route: set[str] = set()
            for req in prod.requires:
                if _matches_any_given(req) or _reachable(req):
                    continue
                sub = _necessary(req, stack | {d.key})
                if sub is None:
                    route = None
                    break
                if req.key in leaf_of:
                    required_by.setdefault(req.key, set()).add(_model_name(prod))
                route |= sub
            if route is not None:
                routes.append(frozenset(route))
        out = frozenset.intersection(*routes) if routes else frozenset()
        necessary[d.key] = out
        return out

    unreachable_targets: list[str] = []
    leaf_targets: dict[str, list[str]] = {}
    for tr_req in target_model.requires:
        if _matches_any_given(tr_req) or _reachable(tr_req) or not _producers_of(tr_req):
            continue
        target_name = _name(tr_req)
        unreachable_targets.append(target_name)
        leaves = sorted(_necessary(tr_req, frozenset()) or ())
        if any(_too_general_givens(leaf_of[leaf]) for leaf in leaves):
            continue
        for leaf in leaves:
            leaf_targets.setdefault(leaf, []).append(target_name)

    for leaf, targets in sorted(leaf_targets.items(), key=lambda kv: (-len(kv[1]), _name(leaf_of[kv[0]]))):
        leaf_name = _name(leaf_of[leaf])
        users = sorted(required_by.get(leaf, ()))
        hints.append(PlanHint(
            kind='unproduced_requirement',
            target=targets[0] if len(targets) == 1 else f"{len(targets)} targets",
            message=(
                f"no transform in the loaded libraries produces {leaf_name} and no given "
                f"input matches it, and every route to {', '.join(targets)} needs it"
                + (f" (required by {', '.join(users)})" if users else "")
            ),
            chain=[f"{u} needs {leaf_name}" for u in users],
            candidate_transforms=users,
        ))

    for tr_req in target_model.requires:
        target_name = _name(tr_req)
        if unreachable_targets and target_name not in unreachable_targets and (
                _matches_any_given(tr_req) or _reachable(tr_req)):
            hints.append(PlanHint(
                kind='blocked_by_other_target',
                target=target_name,
                message=(
                    f"{target_name} can be produced from the given inputs. It was dropped "
                    f"because the plan fails on "
                    f"{', '.join(unreachable_targets)}"
                ),
            ))
            continue
        producers = _producers_of(tr_req)
        if not producers:
            hints.append(PlanHint(
                kind='unreachable_target',
                target=target_name,
                message=f"no transform in the loaded libraries produces a type that satisfies {target_name}",
                near_misses=_rank_near_misses_among_products(tr_req),
            ))
            continue

        queue: deque[tuple[Dependency, list[str], int]] = deque()
        for prod in producers:
            link = (
                f"{_model_name(prod)} produces {target_name} but needs "
                + ", ".join(_name(r) for r in prod.requires)
            )
            for sub in prod.requires:
                queue.append((sub, [f"target needs {target_name}", link], 1))

        visited: set[str] = set()
        dead_ends: dict[str, tuple[Dependency, list[str]]] = {}
        too_general: dict[Endpoint, list[tuple[Dependency, str]]] = {}
        steps_budget = max_hints_per_target * 16
        while queue and steps_budget > 0:
            steps_budget -= 1
            d, chain, hops = queue.popleft()
            if d.key in visited:
                continue
            visited.add(d.key)
            if _matches_any_given(d) or _reachable(d):
                continue
            for ep, _insts in _too_general_givens(d):
                wanted_by = chain[-1].split(" produces ")[0] if chain else "a transform"
                too_general.setdefault(ep, []).append((d, wanted_by))
            sub_producers = _producers_of(d)
            if not sub_producers:
                sk = _shape_key(d)
                if sk not in dead_ends:
                    dead_ends[sk] = (d, chain + [f"<no producer for {_name(d)}>"])
                continue
            if hops >= max_hops:
                sk = _shape_key(d)
                if sk not in dead_ends:
                    dead_ends[sk] = (d, chain + [f"<hop limit reached at {_name(d)}>"])
                continue
            for prod in sub_producers[:3]:
                link = (
                    f"{_model_name(prod)} produces {_name(d)} but needs "
                    + ", ".join(_name(r) for r in prod.requires)
                )
                for sub in prod.requires:
                    queue.append((sub, chain + [link], hops + 1))

        ranked = sorted(
            dead_ends.values(),
            key=lambda dc: (
                -_similarity_to_givens(dc[0]),
                len(dc[1]),
                _name(dc[0]),
            ),
        )
        for ep, wants in too_general.items():
            insts = given_map.get(ep, [])
            label = next((i.dtype_name for i in insts if i.dtype_name), _name(ep))
            where = ", ".join(str(i.path) for i in insts[:3]) or "(no path)"
            suggestions: list[str] = []
            parent_notes: list[str] = []
            seen_names: set[str] = set()
            for demand, wanted_by in wants[:max_hints_per_target]:
                for name in _retypings(demand, ep):
                    if name in seen_names or name == label:
                        continue
                    seen_names.add(name)
                    suggestions.append(f"{name} — what {wanted_by} asks for")
                for parent in _unmet_parents(demand):
                    note = (
                        f"{parent} — {wanted_by} needs its {_name(demand)} to descend "
                        f"from one, and nothing registered is one"
                    )
                    if note not in parent_notes:
                        parent_notes.append(note)
            suggestions += parent_notes
            if not suggestions:
                continue
            wanted_names = []
            said: set[str] = set()
            for demand, wanted_by in wants:
                nm = _name(demand)
                if nm in said:
                    continue
                said.add(nm)
                wanted_names.append(f"{nm} (for {wanted_by})")
                if len(wanted_names) >= 3:
                    break
            hints.append(PlanHint(
                kind='too_general',
                target=target_name,
                message=(
                    f"{label} @ {where} is more general than what the chain to "
                    f"{target_name} needs: {', '.join(wanted_names)}. A more specific "
                    f"type satisfies a general requirement, never the other way round, "
                    f"so this input is not offered to those steps at all"
                ),
                chain=[f"you registered {label}", f"the chain needs {wanted_names[0]}"],
                near_misses=suggestions[:max_hints_per_target + 2],
            ))

        for d, chain in ranked[:max_hints_per_target]:
            hints.append(PlanHint(
                kind='missing_input',
                target=target_name,
                message=(
                    f"to produce {target_name}, the chain dead-ends at {_name(d)}: "
                    f"no transform produces it and no given input matches"
                ),
                chain=chain,
                near_misses=_rank_near_misses_among_givens(d),
            ))

    def _collect_ancestors(ep: Endpoint) -> set[Endpoint]:
        seen: set[Endpoint] = set()
        todo = [ep]
        while todo:
            cur = todo.pop()
            for p in cur.parents:
                if p in seen:
                    continue
                seen.add(p)
                todo.append(p)
        return seen

    seen_lineage_keys: set[str] = set()
    checked: list[tuple[Dependency, Transform|None, str]] = []
    for tr_req in target_model.requires:
        checked.append((tr_req, None, _name(tr_req)))
    for model in transform2inst:
        ctx_name = _model_name(model)
        for req in model.requires:
            checked.append((req, model, ctx_name))

    for req, model, ctx in checked:
        if not req.parents:
            continue
        prop_match = [g for g in all_givens if g.properties >= req.properties]
        if not prop_match:
            continue
        for req_parent in req.parents:
            parent_match = [g for g in all_givens if g.properties >= req_parent.properties]
            for child in prop_match:
                if child in parent_match:
                    continue
                ancestors = _collect_ancestors(child)
                if any(pm in ancestors for pm in parent_match):
                    continue
                key = f"{req.key}|{req_parent.key}|{child.key}"
                if key in seen_lineage_keys:
                    continue
                seen_lineage_keys.add(key)
                req_name = _name(req)
                req_parent_name = _name(req_parent)
                child_insts = given_map.get(child, [])
                child_label = child_insts[0].dtype_name if child_insts and child_insts[0].dtype_name else _name(child)
                near_misses: list[str] = []
                parent_path_hint = None
                for parent_ep in parent_match:
                    insts = given_map.get(parent_ep, [])
                    if insts:
                        parent_path_hint = str(insts[0].path)
                        break
                if parent_path_hint is None:
                    parent_path_hint = f"<{req_parent_name} instance>"
                for inst in child_insts:
                    near_misses.append(
                        f"add parents=[{parent_path_hint}] when registering {inst.path} "
                        f"so it descends from a {req_parent_name}"
                    )
                hints.append(PlanHint(
                    kind='lineage_mismatch',
                    target=ctx,
                    message=(
                        f"{child_label} matches {req_name} by properties, "
                        f"but it is not registered as a descendant of any {req_parent_name}; "
                        f"{_model_name(model)} requires that parent in its data lineage"
                    ),
                    chain=[f"{_model_name(model)} needs {req_name} with parent {req_parent_name}"],
                    near_misses=near_misses,
                ))
    return hints
