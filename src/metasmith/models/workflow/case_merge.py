"""Plan several cases: solve each alone, then merge the plans step by step."""
from __future__ import annotations

from pathlib import Path

from ...logging import Log
from ..libraries import (
    DataInstance, TransformInstance, TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..solver import Application, Dependency, Endpoint, Transform, solve_by_mcts, Solution as SolverResult
from .plan import (
    Case, CollectSolverInputs, _MixedCase, _absorb_givens, _dedupe_instances, _target_names,
)
from .steps import WorkflowStep, WorkflowTarget


def _products(a: Application) -> list[Endpoint]:
    return [e for g in a.produced for e in g.values()]


def _route(given: set[Endpoint], plan: list[Application]) -> list[Application]|None:
    # The steps of `plan` that `given` alone drives to the target. A step whose
    # every product is already at hand is skipped, so a given intermediate is
    # used and never remade.
    have = set(given)
    fired: list[Application] = []
    pending = [a for a in plan if a.used]
    progressed = True
    while progressed:
        progressed = False
        for a in list(pending):
            if not all(e in have for e in a.used.values()):
                continue
            pending.remove(a)
            products = _products(a)
            if products and all(e in have for e in products):
                continue
            fired.append(a)
            progressed = True
            if not products:
                return fired
            have.update(products)
    return None


class _Streams:
    # Endpoints that post to one runtime stream. The representative of a class
    # is its earliest member, so stream names follow case order.
    def __init__(self):
        self.parent: dict[Endpoint, Endpoint] = {}
        self.members: dict[Endpoint, list[Endpoint]] = {}
        self.seq: dict[Endpoint, int] = {}

    def find(self, e: Endpoint) -> Endpoint:
        if e not in self.parent:
            self.parent[e] = e
            self.members[e] = [e]
            self.seq[e] = len(self.seq)
        while self.parent[e] != e:
            e = self.parent[e]
        return e

    def union(self, a: Endpoint, b: Endpoint) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        keep, drop = (ra, rb) if self.seq[ra] < self.seq[rb] else (rb, ra)
        self.parent[drop] = keep
        self.members[keep] = sorted(self.members[keep] + self.members.pop(drop), key=self.seq.__getitem__)

    def of(self, e: Endpoint) -> list[Endpoint]:
        return self.members[self.find(e)]

    def save(self):
        return dict(self.parent), {k: list(v) for k, v in self.members.items()}

    def restore(self, saved) -> None:
        self.parent, self.members = saved


class _Node:
    # One step of the merged plan, serving one or more cases. Compared by
    # identity: two nodes may bind equal endpoints and must stay distinct.
    def __init__(self, transform: Transform, seq: int):
        self.transform = transform
        self.seq = seq
        self.cases: list[int] = []
        self.apps: dict[int, Application] = {}
        self.slots: dict[Dependency, Endpoint] = {}
        self.outputs: dict[Dependency, Endpoint] = {}

    def save(self):
        return list(self.cases), dict(self.apps), dict(self.slots), dict(self.outputs)

    def restore(self, saved) -> None:
        self.cases, self.apps, self.slots, self.outputs = saved


def _ancestors(d: Dependency) -> set[Dependency]:
    out: set[Dependency] = set()
    todo = list(d.parents)
    while todo:
        p = todo.pop()
        if p in out:
            continue
        out.add(p)
        todo.extend(p.parents)
    return out


def _unrelated(d: Dependency, by: Dependency) -> bool:
    if d == by:
        return False
    da, ba = _ancestors(d), _ancestors(by)
    return by not in da and d not in ba and not (da & ba)


class _Merge:
    def __init__(self, given_eps, routes, targets, given_map, inst_cases, group_by):
        self.given_eps: list[set[Endpoint]] = given_eps
        self.routes: list[list[Application]] = routes
        self.targets: list[Application] = targets
        self.given_map: dict[Endpoint, list[DataInstance]] = given_map
        self.inst_cases: dict[str, set[int]] = inst_cases
        self.group_by: dict[Transform, Dependency] = group_by
        self.streams = _Streams()
        self.nodes: list[_Node] = []
        self.pending: list[list[Application]] = [list(r) for r in routes]

    def run(self) -> dict[int, list[str]]:
        for c, route in enumerate(self.routes):
            for app in route:
                self.pending[c].remove(app)
                self._place(c, app)
        return self.problems()

    def _place(self, c: int, app: Application) -> None:
        same = [n for n in self.nodes if n.transform is app.transform and c not in n.cases and set(n.slots) == set(app.used)]
        exact = [n for n in same if all(self.streams.find(app.used[d]) == self.streams.find(n.slots[d]) for d in n.slots)]
        lanes = sorted((n for n in same if n not in exact), key=lambda n: (not self._kind_matches(n, app), n.seq))
        for n in exact + lanes:
            saved = self.streams.save(), [(m, m.save()) for m in self.nodes]
            self._join(n, c, app)
            if not self.problems() and not self._pools_case_givens(n):
                return
            self.streams.restore(saved[0])
            for m, s in saved[1]:
                m.restore(s)
        n = _Node(app.transform, len(self.nodes))
        self.nodes.append(n)
        self._join(n, c, app)

    def _kind_matches(self, n: _Node, app: Application) -> bool:
        return all(
            any(m.properties == app.used[d].properties for m in self.streams.of(n.slots[d]))
            for d in n.slots
        )

    def _join(self, n: _Node, c: int, app: Application) -> None:
        n.cases.append(c)
        n.apps[c] = app
        for d, e in app.used.items():
            if d in n.slots:
                self.streams.union(n.slots[d], e)
            else:
                n.slots[d] = e
                self.streams.find(e)
        for g in app.produced:
            for d, e in g.items():
                if d in n.outputs:
                    self.streams.union(n.outputs[d], e)
                else:
                    n.outputs[d] = e
                    self.streams.find(e)

    def _pools_case_givens(self, n: _Node) -> bool:
        # A slot unrelated to the grouping input pairs every item with every
        # member, so cases that give it different instances must not share it.
        by = self.group_by.get(n.transform)
        for d, e in n.slots.items():
            if by is None or not _unrelated(d, by):
                continue
            seen = set()
            for k in n.cases:
                insts = frozenset(
                    x.instance_id for m in self.streams.of(e) for x in self.given_map.get(m, [])
                    if k in self.inst_cases.get(x.instance_id, {k})
                )
                if insts:
                    seen.add(insts)
            if len(seen) > 1:
                return True
        return False

    def problems(self) -> dict[int, list[str]]:
        # Every slot a case reads, placed or still pending, has exactly one
        # source for that case: one of its givens or one step that serves it.
        find = self.streams.find
        out: dict[int, list[str]] = {}
        for k in range(len(self.routes)):
            served = [n for n in self.nodes if k in n.cases]
            sources: dict[Endpoint, int] = {}
            for e in self.given_eps[k]:
                r = find(e)
                sources[r] = sources.get(r, 0) + 1
            outs = [list(n.outputs.values()) for n in served] + [_products(a) for a in self.pending[k]]
            for es in outs:
                for r in {find(e) for e in es}:
                    sources[r] = sources.get(r, 0) + 1
            reads = (
                [(n.transform, d, e) for n in served for d, e in n.slots.items()]
                + [(a.transform, d, e) for a in self.pending[k] + [self.targets[k]] for d, e in a.used.items()]
            )
            problems = [
                f"reads [{d.key}] from {sources.get(find(e), 0)} sources"
                for _, d, e in reads if sources.get(find(e), 0) != 1
            ]
            if problems:
                out[k] = problems
        if self._order() is None:
            out.setdefault(0, []).append("the merged steps form a cycle")
        return out

    def _order(self) -> list[_Node]|None:
        find = self.streams.find
        made = {id(n): {find(e) for e in n.outputs.values()} for n in self.nodes}
        preds = {
            id(n): {id(m) for m in self.nodes if m is not n and made[id(m)] & {find(e) for e in n.slots.values()}}
            for n in self.nodes
        }
        order: list[_Node] = []
        done: set[int] = set()
        while len(order) < len(self.nodes):
            ready = [n for n in self.nodes if id(n) not in done and preds[id(n)] <= done]
            if not ready:
                return None
            nxt = min(ready, key=lambda n: n.seq)
            order.append(nxt)
            done.add(id(nxt))
        return order


def PlanCases(
    plan_cls, cases: list[Case],
    transforms: list[TransformInstanceLibrary|TransformInstanceLibraryView],
    max_iter: int, max_refine: int|None, seed: int,
):
    group_cases = [c for c, case in enumerate(cases) for _ in case.given]
    given = [sample for case in cases for sample in case.given]
    try:
        given_map, case_eps, transform2inst, inst2trlib, index = CollectSolverInputs(
            given, transforms, group_cases=group_cases,
        )
    except _MixedCase as e:
        raise ValueError(f"case [{cases[e.case].name}] mixes samples of different shapes, such as [{e.label}]") from None
    names = [_target_names(case, transforms) for case in cases]
    Log.Info(f"solving plan for [{len(given)}] samples as [{len(cases)}] cases")

    results: list[SolverResult] = []
    for case, eps in zip(cases, case_eps):
        Log.Info(f"solving case [{case.name}] for [{len(case.given)}] sample(s)")
        results.append(solve_by_mcts(
            given=[eps], target=case.target, transforms=transform2inst.keys(),
            max_iter=max_iter, max_refine=max_refine, seed=seed,
        ))
    solved = [h for h, r in enumerate(results) if r.complete and r.dependency_plan]

    given_eps: list[set[Endpoint]] = []
    for c, eps in enumerate(case_eps):
        have = set(eps)
        r = results[c]
        if r.dependency_plan and not r.dependency_plan[0].used:
            have |= set(_products(r.dependency_plan[0]))
        given_eps.append(have)

    routes: list[list[Application]|None] = []
    for c, case in enumerate(cases):
        # A case with no route of its own may still follow the plan of a case
        # with the same target, skipping the steps whose products it was given.
        candidates = ([c] if c in solved else []) + [
            h for h in solved if h != c and cases[h].target is case.target
        ]
        routes.append(next(
            (r for r in (_route(given_eps[c], results[h].dependency_plan) for h in candidates) if r is not None),
            None,
        ))

    common = dict(given_map=given_map, transform2inst=transform2inst, transforms=transforms)

    def refuse(failed: dict[int, list[str]]):
        first = min(failed)
        return plan_cls._Refuse(
            result=results[first], failed=failed, cases=index,
            solver_inputs=(case_eps, list(transform2inst.keys()), cases[first].target),
            target_names=names[first], target_model=cases[first].target, **common,
        )

    failed = {c: ["no step reaches the target"] for c, r in enumerate(routes) if r is None}
    if failed:
        return refuse(failed)

    for h in solved:
        r = results[h]
        if r.dependency_plan and not r.dependency_plan[0].used:
            _absorb_givens(given_map, r.dependency_plan[0], r.merged_endpoints)

    merge = _Merge(
        given_eps=given_eps,
        routes=[[a for a in r if _products(a)] for r in routes],  # type: ignore # none are None
        targets=[r[-1] for r in routes],  # type: ignore
        given_map=given_map,
        inst_cases=index.inst_cases,
        group_by={t: transform2inst[t].group_by for t in transform2inst},
    )
    failed = merge.run()
    if failed:
        return refuse(failed)
    order = merge._order()
    assert order is not None

    streams = merge.streams
    inst_cases = index.inst_cases
    instance_map: dict[Endpoint, list[DataInstance]] = {k: _dedupe_instances(v) for k, v in given_map.items()}
    out_inst: dict[tuple[int, Dependency], DataInstance] = {}
    step_of: dict[int, WorkflowStep] = {}
    paths_taken: set[Path] = set()
    steps: list[WorkflowStep] = []
    used_endpoints: set[Endpoint] = set()

    def serves(inst: DataInstance, n: _Node) -> bool:
        return bool(inst_cases.get(inst.instance_id, set(n.cases)) & set(n.cases))

    for i, n in enumerate(order):
        tr = transform2inst[n.transform]
        lib = inst2trlib[tr]
        dep_map: dict[Dependency, list[DataInstance]] = {}
        for d, e in n.slots.items():
            members = streams.of(e)
            used_endpoints.update(members)
            dep_map[d] = _dedupe_instances([
                x for m in members for x in instance_map.get(m, []) if serves(x, n)
            ])
        for d, e in n.outputs.items():
            path = Path(e.key + e.GetPreferredFileExtension())
            k = 1
            while path in paths_taken:
                k += 1
                path = Path(f"{e.key}_{k}{e.GetPreferredFileExtension()}")
            paths_taken.add(path)
            inst = DataInstance(path=path, dtype=e, dtype_name=lib.GetName(Endpoint(d.properties)), parent_lib=lib)
            inst_cases[inst.instance_id] = set(n.cases)
            instance_map[e] = instance_map.get(e, []) + [inst]
            out_inst[(id(n), d)] = inst
            dep_map[d] = [inst]
        step = WorkflowStep(
            order=i+1, dependency_map=dep_map, transform=tr, transform_library=lib,
            cases=[cases[c].name for c in sorted(n.cases)],
        )
        step_of[id(n)] = step
        steps.append(step)

    targets: list[WorkflowTarget] = []
    dropped_targets: list[str] = []
    for c, case in enumerate(cases):
        tgt = merge.targets[c]
        for d, nm in zip(case.target.requires, names[c]):
            e = tgt.used.get(d)
            hit = None if e is None else next((
                (n, od) for n in order if c in n.cases for od, oe in n.outputs.items()
                if streams.find(oe) == streams.find(e)
            ), None)
            if hit is None:
                Log.Warn(f"case [{case.name}]: target [{nm}] was requested but not included in plan")
                if nm not in dropped_targets:
                    dropped_targets.append(nm)
                continue
            n, od = hit
            targets.append(WorkflowTarget(
                name=nm, instance=out_inst[(id(n), od)], producing_step=step_of[id(n)], case=case.name,
            ))

    pool = set(given_map)
    frontier = set(used_endpoints)
    while frontier:
        frontier = {p for e in frontier for p in e.parents if p in pool and p not in used_endpoints}  # type: ignore
        used_endpoints |= frontier
    plan_given: list[DataInstance] = []
    seen: set[str] = set()
    for e, lst in given_map.items():
        if e not in used_endpoints:
            continue
        for inst in lst:
            if inst.instance_id not in seen:
                seen.add(inst.instance_id)
                plan_given.append(inst)

    given_steps = [r.dependency_plan[0] for r in results if r.dependency_plan and not r.dependency_plan[0].used]
    merged_eps: dict[Endpoint, set[Endpoint]] = {}
    for r in results:
        for e, me in r.merged_endpoints.items():
            merged_eps.setdefault(e, set()).update(me)
    combined = SolverResult(
        complete=True,
        dependency_plan=given_steps[:1] + [n.apps[n.cases[0]] for n in order] + list(merge.targets),
        merged_endpoints=merged_eps,
        _iterations=sum(r._iterations for r in results),
        _refiner_iterations=[x for r in results for x in r._refiner_iterations],
        _relavent_transforms=list({id(t): t for r in results for t in r._relavent_transforms}.values()),
    )
    return plan_cls(
        given=plan_given,
        targets=targets,
        steps=steps,
        _solver_result=combined,
        dropped_targets=dropped_targets,
        cases=[c.name for c in cases],
        streams={
            m.key: rep.key
            for rep, members in streams.members.items() if len(members) > 1 for m in members
        },
        given_cases={
            inst.instance_id: [cases[c].name for c in sorted(inst_cases.get(inst.instance_id, range(len(cases))))]
            for inst in plan_given
        },
    )
