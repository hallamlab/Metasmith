"""Why no plan was found -- the structured answer, not "no plan".

When the solver comes back empty, `WorkflowPlan.Generate` calls in here and
attaches the result to `WorkflowPlan.hints`. Every consumer is expected to
surface them; a bare "no plan" is not an acceptable failure.

Three passes, in order of how specific an answer they can give: an unreachable
target (nothing produces a match at all), a multi-hop reverse-BFS that reports
where a chain dead-ends, and a lineage mismatch -- a given that matches a
requirement on properties but does not descend from the parent the slot asked
for. The last one is the reason a plan can look obviously satisfiable and not
be: `parents=` puts lineage inside the type's identity.

Everything here takes what it needs as arguments and holds no state, which is
what lets it live outside `plan.py` rather than on `WorkflowPlan`.
"""

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
    """Build PlanHint objects explaining why no plan was found.

    Three passes:
        a. unreachable target: no transform produces a match.
        b. multi-hop reverse-BFS: chain dead-ends at a demand with no
           producer and no given match.
        c. lineage mismatch: a given matches a requirement by properties
           but does not descend from a required parent.
    """
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

    # build endpoint -> name cache from data instances, target_names, and any
    # supplied type lookups (typically the transform libs)
    _name_cache: dict = {}
    for d, nm in zip(target_model.requires, target_names):
        _name_cache.setdefault(Endpoint(d.properties), nm)
    for ep, insts in given_map.items():
        for inst in insts:
            if inst.dtype_name:
                _name_cache.setdefault(ep, inst.dtype_name)
                break
    for lookup in (type_lookups or []):
        # walk the inner DataTypeLibrary namespaces to capture every typed
        # Endpoint (lookup.Iterate() only yields stored instances, not type
        # definitions)
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
        # node hashes by properties+parents; try property-only match
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
        # canonical key that ignores parent identity — collapses two demands
        # with the same property bag (but different `parents={...}`) into one
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
        # 0.5 factor keeps shape-match scores strictly below value-match scores
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
        # fallback: rank by property-KEY overlap (helps when same shape but
        # different value, e.g. ext=bam vs ext=fq.gz)
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

    # ---- "you have the right thing, said too loosely" -----------------------
    #
    # `x.IsA(y)` is `y.properties <= x.properties`: more properties means more
    # specific, and a *supertype* never satisfies a subtype's requirement. That
    # asymmetry is correct and it is also the single most confusing failure the
    # planner produces -- registering reads and asking for an assembly dead-ends
    # somewhere five hops away at an ncbi accession, because every assembler
    # wants `long_reads` or `short_reads_pe` and plain `reads` is neither.
    #
    # So a demand nothing satisfies is worth reporting against the givens that
    # are *nearly* it in the one direction the type system cares about.

    def _too_general_givens(demand) -> list[tuple[Endpoint, list[DataInstance]]]:
        """Givens that are strictly more general than `demand`."""
        out = []
        for ep, insts in given_map.items():
            if ep.IsA(demand):       # already satisfies it; not this problem
                continue
            if demand.properties > ep.properties:
                out.append((ep, insts))
        return out

    def _retypings(demand, given_ep: Endpoint, limit: int = 4) -> list[str]:
        """Named types that would satisfy `demand` and still describe `given_ep`.

        A retyping is only a suggestion if it is a specialization of what the
        user already said they have -- otherwise it is a different file, not a
        better label for this one.
        """
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
        """Parents the slot declares that nothing registered could stand in for.

        A requirement's lineage is part of it: bbduk does not want three read
        files, it wants the reads belonging to *this* metadata. When the parent
        type is not registered at all, no amount of retyping the child will
        help -- and nothing else in the diagnosis says so.
        """
        out = []
        for parent in getattr(demand, "parents", None) or ():
            if any(g.properties >= parent.properties for g in all_givens):
                continue
            name = _name(parent)
            if name not in out:
                out.append(name)
        return out

    # ---- pass (a) + (b): per-target reverse-BFS ----
    for tr_req in target_model.requires:
        target_name = _name(tr_req)
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
        # given endpoint -> the demands it is a supertype of, in walk order
        too_general: dict[Endpoint, list[tuple[Dependency, str]]] = {}
        steps_budget = max_hints_per_target * 16
        while queue and steps_budget > 0:
            steps_budget -= 1
            d, chain, hops = queue.popleft()
            if d.key in visited:
                continue
            visited.add(d.key)
            if _matches_any_given(d):
                continue
            # Recorded for every demand on the way, not only for the dead ends:
            # the demand a too-general input was *meant* to answer usually has
            # producers of its own, so the walk goes straight past it and the
            # dead end it eventually reports is several hops off the point.
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
        # Ahead of the dead ends on purpose: when one of these fires it is
        # almost always the actual answer, and the dead end is a symptom of it.
        for ep, wants in too_general.items():
            insts = given_map.get(ep, [])
            label = next((i.dtype_name for i in insts if i.dtype_name), _name(ep))
            where = ", ".join(str(i.path) for i in insts[:3]) or "(no path)"
            # retypings first, then what the lineage still wants: one is a
            # correction to a row that exists, the other is a row that does not
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
            # by type, not by asker: three transforms wanting `long_reads` is
            # one thing to fix, and saying it three times reads as three
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

    # ---- pass (c): lineage mismatch ----
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
