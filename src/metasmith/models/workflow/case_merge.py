"""Plan several cases: solve each alone, then merge the plans step by step."""
from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from ...hashing import KeyGenerator
from ...logging import Log
from ..libraries import (
    DataInstance, TransformInstance, TransformInstanceLibrary, TransformInstanceLibraryView,
)
from ..solver import Application, Dependency, Endpoint, Transform, solve_by_mcts, Solution as SolverResult
from .diagnostics import PlanHint
from .plan import (
    Case, CollectSolverInputs, _MixedCase, _absorb_givens, _dedupe_instances, _target_names, _try_name,
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


def _outcome(e: Endpoint, g: int) -> Endpoint:
    x = Endpoint(set(e.properties), set(e.parents))  # type: ignore[arg-type]
    x._sig = f"{e.Signature()}#{g}"
    x.hash, x.key = KeyGenerator.FromStr(x._sig)
    return x


def _by_outcome(route: list[Application]) -> list[Application]:
    # The products of a fork are equal across its groups, and the merge joins
    # equal endpoints. Each group's products get an identity of their own, so
    # a group's stream carries only what a task wrote for that group.
    remap: dict[Endpoint, Endpoint] = {}
    out = []
    for a in route:
        produced = a.produced
        if len(a.transform.produces) > 1:
            g = a.group or 0
            produced = [{d: remap.setdefault(e, _outcome(e, g)) for d, e in p.items()} for p in a.produced]
        used = {d: remap.get(e, e) for d, e in a.used.items()}
        out.append(replace(a, used=used, produced=produced, _sig=None, _hash=None))
    return out


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
        # Keyed by product and group: each group of a fork posts its own
        # stream, even for a product that several groups share.
        self.outputs: dict[tuple[Dependency, int], Endpoint] = {}

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


_MAX_CASES = 64


@dataclass
class _Job:
    # One case on the stack: a declared case, with the outcome of every fork it
    # has pinned. `head` is the plan of the case that pushed it, replayed first.
    root: int
    forks: dict[Transform, int]
    head: list[Transform]
    origin: tuple[Transform, int]|None = None
    result: SolverResult|None = None


def _group_names(fork: Transform, g: int, transform2inst, inst2trlib) -> list[str]:
    lib = inst2trlib[transform2inst[fork]]
    return [_try_name(lib, Endpoint(d.properties)) or "+".join(sorted(d.properties)) for d in fork.produces[g]]


def _describe(job: _Job, transform2inst) -> str:
    if not job.forks:
        return ""
    return " where " + ", ".join(f"[{transform2inst[t].name}] makes group [{g}]" for t, g in job.forks.items())


class _Merge:
    def __init__(self, given_map, inst_cases, group_by):
        self.given_eps: list[set[Endpoint]] = []
        self.routes: list[list[Application]] = []
        self.targets: list[Application] = []
        self.given_map: dict[Endpoint, list[DataInstance]] = given_map
        self.inst_cases: dict[str, set[int]] = inst_cases
        self.group_by: dict[Transform, Dependency] = group_by
        self.streams = _Streams()
        self.nodes: list[_Node] = []
        self.pending: list[list[Application]] = []

    def add(self, given_eps: set[Endpoint], route: list[Application], target: Application) -> dict[int, list[str]]:
        c = len(self.routes)
        self.given_eps.append(given_eps)
        self.routes.append(route)
        self.targets.append(target)
        self.pending.append(list(route))
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
            if not self.problems() and not self._pools_case_givens(n) and not self._joins_outcomes():
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
        g = (app.group or 0) if len(app.transform.produces) > 1 else 0
        for d, e in (x for p in app.produced for x in p.items()):
            if (d, g) in n.outputs:
                self.streams.union(n.outputs[(d, g)], e)
            else:
                n.outputs[(d, g)] = e
                self.streams.find(e)

    def _joins_outcomes(self) -> bool:
        # Two outcomes of one fork stay two streams. A step that would read
        # both through one slot is placed twice instead, once per outcome, so
        # a step that serves one outcome never sees the other's products.
        find = self.streams.find
        for n in self.nodes:
            if len(n.transform.produces) < 2:
                continue
            group_of: dict[Endpoint, int] = {}
            for (_, g), e in n.outputs.items():
                if group_of.setdefault(find(e), g) != g:
                    return True
        return False

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
            outs = [_products(n.apps[k]) for n in served] + [_products(a) for a in self.pending[k]]
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

    common = dict(given_map=given_map, transform2inst=transform2inst, transforms=transforms)
    inst_cases: dict[str, set[int]] = {}
    jobs: list[_Job] = []
    results: list[SolverResult] = []
    merge = _Merge(
        given_map=given_map, inst_cases=inst_cases,
        group_by={t: transform2inst[t].group_by for t in transform2inst},
    )

    def refuse(failures: list[tuple[_Job, SolverResult, list[str]]]):
        failed: dict[int, list[str]] = {}
        hints = []
        for job, result, problems in failures:
            # A declared case whose search stopped below a fork names the
            # outcome it was solving, as a pushed case does.
            origin = job.origin or next((
                (a.transform, a.group or 0) for a in result.dependency_plan
                if a.used and len(a.transform.produces) > 1
            ), None)
            if origin is not None:
                fork, g = origin
                what = _group_names(fork, g, transform2inst, inst2trlib)
                msg = f"[{transform2inst[fork].name}] can make {what}, and no plan carries that to the target"
                problems = [f"{msg}: {p}" for p in problems]
                hints.append(PlanHint(
                    kind="fork_branch", target=",".join(names[job.root]), message=msg,
                    candidate_transforms=[transform2inst[fork].name],
                ))
            failed.setdefault(job.root, []).extend(problems)
        job, result, _ = min(failures, key=lambda f: f[0].root)
        r = job.root
        plan = plan_cls._Refuse(
            result=result, failed=failed, cases=index,
            solver_inputs=(case_eps, list(transform2inst.keys()), cases[r].target),
            target_names=names[r], target_model=cases[r].target, **common,
        )
        plan.hints = hints + plan.hints
        return plan

    def fallback(job: _Job, have: set[Endpoint]) -> list[Application]|None:
        # A declared case with no route of its own may still follow the plan of
        # a declared case with the same target, skipping the steps whose
        # products it was given. A fork case never does: the substitute would
        # not carry its outcome.
        if job.forks:
            return None
        for h, other in enumerate(jobs):
            if other.forks or cases[other.root].target is not cases[job.root].target:
                continue
            r = results[h]
            route = _route(have, r.dependency_plan) if r.complete and r.dependency_plan else None
            if route is not None:
                return route
        return None

    seen: set[tuple[int, frozenset]] = set()
    stack = [_Job(root=r, forks={}, head=[]) for r in reversed(range(len(cases)))]
    deferred: list[_Job] = []
    unrouted: list[tuple[_Job, SolverResult, list[str]]] = []
    while stack or deferred:
        if not stack:
            stack, deferred = list(reversed(deferred)), []
        job = stack.pop()
        retry = job.result is not None
        key = (job.root, frozenset((t, g) for t, g in job.forks.items() if g != 0))
        if key in seen and not retry:
            continue
        seen.add(key)
        if len(seen) > _MAX_CASES:
            raise ValueError(f"forks in case [{cases[job.root].name}] make more than [{_MAX_CASES}] cases")
        if job.result is not None:
            result = job.result
        else:
            # Only a pushed case is guided. A declared case solves alone, so its
            # plan does not depend on the order cases are declared in.
            aggregate = [n.transform for n in merge._order() or []] if job.origin else []
            guide = list({id(t): t for t in job.head + aggregate}.values())
            Log.Info(f"solving case [{cases[job.root].name}]{_describe(job, transform2inst)} for [{len(cases[job.root].given)}] sample(s)")
            result = solve_by_mcts(
                given=[case_eps[job.root]], target=cases[job.root].target, transforms=transform2inst.keys(),
                max_iter=max_iter, max_refine=max_refine, seed=seed,
                fork_groups=list(job.forks.items()), guide=guide,
            )
            job.result = result
        have = set(case_eps[job.root])
        if result.dependency_plan and not result.dependency_plan[0].used:
            have |= set(_products(result.dependency_plan[0]))
        route = _route(have, result.dependency_plan) if result.complete and result.dependency_plan else None
        if route is None:
            route = fallback(job, have)
        if route is None:
            if not retry and not job.forks:
                deferred.append(job)
                continue
            unrouted.append((job, result, ["no step reaches the target"]))
            if job.forks:
                break
            continue

        c = len(jobs)
        jobs.append(job)
        results.append(result)
        for iid, roots in index.inst_cases.items():
            if job.root in roots:
                inst_cases.setdefault(iid, set()).add(c)
        if result.complete and result.dependency_plan and not result.dependency_plan[0].used:
            _absorb_givens(given_map, result.dependency_plan[0], result.merged_endpoints)
        placed = _by_outcome(route)
        failed = merge.add(have, [a for a in placed if _products(a)], placed[-1])
        if failed:
            return refuse([(jobs[k], results[k], ps) for k, ps in failed.items()])

        head = [a.transform for a in route if a.used and _products(a)]
        for a in reversed(route):
            if not a.used or len(a.transform.produces) < 2 or a.transform in job.forks:
                continue
            chosen = a.group or 0
            for g in reversed(range(len(a.transform.produces))):
                if g != chosen:
                    stack.append(_Job(
                        root=job.root, forks={**job.forks, a.transform: g}, head=head, origin=(a.transform, g),
                    ))

    if unrouted:
        return refuse(unrouted)
    order = merge._order()
    assert order is not None

    streams = merge.streams
    roots_of = [j.root for j in jobs]

    def root_names(cs) -> list[str]:
        return [cases[r].name for r in sorted({roots_of[c] for c in cs})]

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
        fork = len(n.transform.produces) > 1
        groups: dict[str, list[int]] = {}
        for (d, g), e in sorted(n.outputs.items(), key=lambda x: x[0][1]):
            path = Path(e.key + e.GetPreferredFileExtension())
            k = 1
            while path in paths_taken:
                k += 1
                path = Path(f"{e.key}_{k}{e.GetPreferredFileExtension()}")
            paths_taken.add(path)
            inst = DataInstance(path=path, dtype=e, dtype_name=lib.GetName(Endpoint(d.properties)), parent_lib=lib)
            inst_cases[inst.instance_id] = set(n.cases)
            instance_map[e] = instance_map.get(e, []) + [inst]
            out_inst[(id(n), d, g)] = inst
            dep_map[d] = dep_map.get(d, []) + [inst]
            if fork:
                groups.setdefault(d.key, []).append(g)
        step = WorkflowStep(
            order=i+1, dependency_map=dep_map, transform=tr, transform_library=lib,
            cases=root_names(n.cases), groups=groups,
        )
        step_of[id(n)] = step
        steps.append(step)

    targets: list[WorkflowTarget] = []
    placed: set[tuple[str, str, str]] = set()
    dropped_targets: list[str] = []
    for c in sorted(range(len(jobs)), key=roots_of.__getitem__):
        case = cases[roots_of[c]]
        tgt = merge.targets[c]
        for d, nm in zip(case.target.requires, names[roots_of[c]]):
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
            inst = out_inst[(id(n), *od)]
            if (nm, case.name, inst.instance_id) in placed:
                continue
            placed.add((nm, case.name, inst.instance_id))
            targets.append(WorkflowTarget(name=nm, instance=inst, producing_step=step_of[id(n)], case=case.name))

    pool = set(given_map)
    frontier = set(used_endpoints)
    while frontier:
        frontier = {p for e in frontier for p in e.parents if p in pool and p not in used_endpoints}  # type: ignore
        used_endpoints |= frontier
    plan_given: list[DataInstance] = []
    seen_given: set[str] = set()
    for e, lst in given_map.items():
        if e not in used_endpoints:
            continue
        for inst in lst:
            if inst.instance_id not in seen_given:
                seen_given.add(inst.instance_id)
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
            inst.instance_id: root_names(inst_cases.get(inst.instance_id, range(len(jobs))))
            for inst in plan_given
        },
    )
