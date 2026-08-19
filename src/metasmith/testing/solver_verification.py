from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Any, Iterable

from ..models.solver import (
    Application,
    Dependency,
    Endpoint,
    Solution,
    Transform,
    solve_by_mcts,
)

__all__ = [
    "SolverProblem",
    "GeneratorDials",
    "plan_fingerprint",
    "plan_shape",
    "check_plan",
    "generate_problem",
    "problem_of_plan",
    "forward_closure_solvable",
    "exhaustive_solvable",
]


def _h(*parts: object) -> str:
    m = hashlib.blake2b(digest_size=12)
    for p in parts:
        m.update(repr(p).encode("utf-8"))
        m.update(b"\x1f")
    return m.hexdigest()


def _steps_of(plan: Solution | Iterable[Application]) -> list[Application]:
    if isinstance(plan, Solution):
        return list(plan.dependency_plan)
    return list(plan)


def plan_fingerprint(plan: Solution | Iterable[Application], *, rounds: int = 24) -> str:
    return _fingerprint_detail(plan, rounds=rounds)[0]


def plan_shape(plan: Solution | Iterable[Application]) -> dict[str, Any]:
    steps = _steps_of(plan)
    endpoints: set[int] = set()
    for s in steps:
        endpoints.update(id(e) for e in s.used.values())
        endpoints.update(id(e) for g in s.produced for e in g.values())
    fp, colours = _fingerprint_detail(plan)
    return {
        "fingerprint": fp,
        "steps": len(steps),
        "endpoints": len(endpoints),
        "transforms": sorted({s.transform.key for s in steps}),
        "orbits": len(set(colours.values())),
    }


def _fingerprint_detail(
    plan: Solution | Iterable[Application], *, rounds: int = 24
) -> tuple[str, dict[tuple[str, int], str]]:
    steps = _steps_of(plan)
    if not steps:
        return _h("EMPTY"), {}

    step_nodes: dict[int, Application] = {id(s): s for s in steps}
    ep_nodes: dict[int, Endpoint] = {}
    step_in: dict[int, list[tuple[str, int]]] = {}
    step_out: dict[int, list[tuple[int, str, int]]] = {}
    ep_prod: dict[int, list[tuple[int, str, int]]] = {}
    ep_cons: dict[int, list[tuple[str, int]]] = {}

    def _ep(e: Endpoint) -> int:
        k = id(e)
        if k not in ep_nodes:
            ep_nodes[k] = e
            ep_prod[k] = []
            ep_cons[k] = []
        return k

    for s in steps:
        sk = id(s)
        step_in[sk] = []
        step_out[sk] = []
        for d, e in s.used.items():
            ek = _ep(e)
            step_in[sk].append((d.key, ek))
            ep_cons[ek].append((d.key, sk))
        for gi, group in enumerate(s.produced):
            for d, e in group.items():
                ek = _ep(e)
                step_out[sk].append((gi, d.key, ek))
                ep_prod[ek].append((gi, d.key, sk))

    colour: dict[tuple[str, int], str] = {}
    for k, s in step_nodes.items():
        colour[("s", k)] = _h("T", s.transform.key)
    for k, e in ep_nodes.items():
        colour[("e", k)] = _h("E", tuple(sorted(e.properties)))

    previous = -1
    for _ in range(rounds):
        nxt: dict[tuple[str, int], str] = {}
        for k in step_nodes:
            ins = sorted(_h("i", dk, colour[("e", ek)]) for dk, ek in step_in[k])
            outs = sorted(
                _h("o", gi, dk, colour[("e", ek)]) for gi, dk, ek in step_out[k]
            )
            nxt[("s", k)] = _h(colour[("s", k)], tuple(ins), tuple(outs))
        for k in ep_nodes:
            prods = sorted(
                _h("p", gi, dk, colour[("s", sk)]) for gi, dk, sk in ep_prod[k]
            )
            cons = sorted(_h("c", dk, colour[("s", sk)]) for dk, sk in ep_cons[k])
            nxt[("e", k)] = _h(colour[("e", k)], tuple(prods), tuple(cons))
        colour = nxt
        distinct = len(set(colour.values()))
        if distinct == previous:
            break
        previous = distinct

    return _h(tuple(sorted(colour.values()))), colour


def _conforms(dep: Dependency, ep: Endpoint) -> bool:
    return set(dep.properties).issubset(set(ep.properties))


class _Ancestry:
    def __init__(self, steps: list[Application]) -> None:
        self.producers: dict[int, list[Application]] = {}
        self.by_id: dict[int, Endpoint] = {}
        for s in steps:
            for group in s.produced:
                for e in group.values():
                    self.by_id[id(e)] = e
                    self.producers.setdefault(id(e), []).append(s)
            for e in s.used.values():
                self.by_id.setdefault(id(e), e)

    def parents_of(self, e: Endpoint) -> list[Endpoint]:
        out: list[Endpoint] = []
        for step in self.producers.get(id(e), []):
            out.extend(step.used.values())
        out.extend(e.parents)  # type: ignore[arg-type]
        return out

    def is_ancestor(self, e: Endpoint, a: Endpoint) -> bool:
        todo = [e]
        seen = {id(e)}
        while todo:
            n = todo.pop()
            for p in self.parents_of(n):
                if p is a or p == a:
                    return True
                if id(p) in seen:
                    continue
                seen.add(id(p))
                todo.append(p)
        return False


@dataclass
class PlanCheck:
    violations: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations

    def __bool__(self) -> bool:
        return self.ok

    def __str__(self) -> str:
        if self.ok:
            return "plan ok" + (f" ({len(self.notes)} notes)" if self.notes else "")
        return "; ".join(self.violations)


def check_plan(problem: SolverProblem, solution: Solution) -> PlanCheck:
    res = PlanCheck()
    steps = list(solution.dependency_plan)
    if not steps:
        res.violations.append("plan has no steps")
        return res

    known = {id(t) for t in problem.transforms} | {id(problem.target)}
    synthetic = [s for s in steps if id(s.transform) not in known]
    if len(synthetic) > 1:
        res.violations.append(
            f"{len(synthetic)} steps apply a transform absent from the problem"
        )
    for s in synthetic:
        if s.transform.requires:
            res.violations.append(
                "a step applies an unknown transform that has requirements "
                "(only the synthetic `given` step may be foreign)"
            )

    anc = _Ancestry(steps)
    produced_ids = set(anc.producers)
    produced_eq = {e for k, e in anc.by_id.items() if k in produced_ids}

    for i, s in enumerate(steps):
        for d, e in s.used.items():
            if id(e) not in produced_ids:
                if e in produced_eq:
                    res.notes.append(
                        f"step {i} ({s.transform.key}) consumes an endpoint that is "
                        "only equal to, not identical with, a produced one"
                    )
                else:
                    res.violations.append(
                        f"step {i} ({s.transform.key}) consumes an endpoint "
                        f"{sorted(e.properties)} that no step produces"
                    )
            if not _conforms(d, e):
                res.violations.append(
                    f"step {i} ({s.transform.key}) binds {sorted(e.properties)} to a "
                    f"slot requiring {sorted(d.properties)}"
                )
        is_given_step = id(s.transform) not in known
        for gi, group in enumerate(s.produced):
            for d, e in group.items():
                if _conforms(d, e):
                    continue
                if is_given_step:
                    if any(e.properties == g.properties for grp in problem.given for g in grp):
                        res.notes.append(
                            f"step {i} emits {sorted(e.properties)} from a slot "
                            f"declaring {sorted(d.properties)} -- merged timelines"
                        )
                        continue
                    res.violations.append(
                        f"step {i} presents {sorted(e.properties)} as given, but "
                        "no given endpoint has those properties"
                    )
                    continue
                res.violations.append(
                    f"step {i} ({s.transform.key}) emits {sorted(e.properties)} "
                    f"from a product slot declaring {sorted(d.properties)}"
                )

    step_index = {id(s): i for i, s in enumerate(steps)}
    edges: dict[int, set[int]] = {id(s): set() for s in steps}
    for s in steps:
        for e in s.used.values():
            for producer in anc.producers.get(id(e), []):
                if producer is s:
                    continue
                edges[id(producer)].add(id(s))
                if step_index[id(producer)] > step_index[id(s)]:
                    res.violations.append(
                        f"step {step_index[id(s)]} ({s.transform.key}) consumes output "
                        f"of later step {step_index[id(producer)]} "
                        f"({producer.transform.key})"
                    )

    WHITE, GREY, BLACK = 0, 1, 2
    colour = {k: WHITE for k in edges}
    for root in list(edges):
        if colour[root] != WHITE:
            continue
        stack: list[tuple[int, bool]] = [(root, False)]
        while stack:
            node, done = stack.pop()
            if done:
                colour[node] = BLACK
                continue
            if colour[node] != WHITE:
                continue
            colour[node] = GREY
            stack.append((node, True))
            for nxt in edges[node]:
                if colour[nxt] == GREY:
                    res.violations.append(
                        f"step graph has a cycle through step {step_index[nxt]}"
                    )
                    colour[nxt] = BLACK
                elif colour[nxt] == WHITE:
                    stack.append((nxt, False))

    target_steps = [s for s in steps if s.transform is problem.target]
    if len(target_steps) != 1:
        res.violations.append(
            f"expected exactly 1 application of the target transform, found "
            f"{len(target_steps)}"
        )
    for t in target_steps:
        for d in problem.target.requires:
            if d not in t.used:
                res.violations.append(
                    f"target requirement {sorted(d.properties)} is unbound"
                )

    for i, s in enumerate(steps):
        for d, e in s.used.items():
            for proto in d.parents:
                if proto not in s.used:
                    res.violations.append(
                        f"step {i} ({s.transform.key}) declares a lineage constraint "
                        f"on an unbound slot {sorted(proto.properties)}"
                    )
                    continue
                constraint = s.used[proto]  # type: ignore[index]
                if constraint is e or constraint == e:
                    continue
                if not anc.is_ancestor(e, constraint):
                    res.violations.append(
                        f"step {i} ({s.transform.key}): {sorted(e.properties)} is not "
                        f"descended from its declared lineage constraint "
                        f"{sorted(constraint.properties)}"
                    )
    return res


@dataclass
class GeneratorDials:
    n_types: int = 6
    n_given: int = 1
    n_given_groups: int = 1
    n_extra_transforms: int = 3
    cycle_density: float = 0.0
    lineage_density: float = 0.0
    n_duplicate_transforms: int = 0
    product_group_density: float = 0.0
    target_lineage: float = 0.0
    max_requirements: int = 2


@dataclass
class SolverProblem:
    given: list[set[Endpoint]]
    transforms: list[Transform]
    target: Transform
    name: str = "problem"
    dials: GeneratorDials | None = None

    def solve(self, **kwargs) -> Solution:
        return solve_by_mcts(
            given=[set(g) for g in self.given],
            transforms=list(self.transforms),
            target=self.target,
            **kwargs,
        )


def problem_of_plan(plan, *, name: str = "template") -> SolverProblem | None:
    inputs = getattr(plan, "_solver_inputs", None)
    if inputs is None:
        return None
    given, transforms, target = inputs
    return SolverProblem(
        given=[set(g) for g in given],
        transforms=list(transforms),
        target=target,
        name=name,
    )


def _type_props(i: int) -> set[str]:
    return {f"t{i}"}


def generate_problem(
    seed: int, dials: GeneratorDials | None = None, *, name: str | None = None
) -> SolverProblem:
    d = dials or GeneratorDials()
    rng = random.Random(seed)
    transforms: list[Transform] = []

    for j in range(d.n_given - 1, d.n_types - 1):
        tr = Transform()
        tr.AddRequirement(properties=_type_props(j))
        tr.AddProduct(properties=_type_props(j + 1))
        transforms.append(tr)
    spine = len(transforms)

    for _ in range(d.n_extra_transforms):
        tr = Transform()
        n_req = rng.randint(1, max(1, d.max_requirements))
        pool = list(range(0, d.n_types - 1))
        n_req = min(n_req, len(pool))
        picks = sorted(rng.sample(pool, n_req))
        reqs: list[Dependency] = [tr.AddRequirement(properties=_type_props(i)) for i in picks]
        if len(reqs) >= 2 and rng.random() < d.lineage_density:
            tr = Transform()
            first = tr.AddRequirement(properties=_type_props(picks[0]))
            for i in picks[1:]:
                tr.AddRequirement(properties=_type_props(i), parents={first})
        if rng.random() < d.cycle_density:
            tr.AddProduct(properties=_type_props(picks[0]))
        else:
            hi = rng.randint(min(picks[-1] + 1, d.n_types - 1), d.n_types - 1)
            tr.AddProduct(properties=_type_props(hi))
        if rng.random() < d.product_group_density:
            tr.NewProductGroup()
            tr.AddProduct(properties=_type_props(rng.randint(1, d.n_types - 1)))
        transforms.append(tr)

    for _ in range(d.n_duplicate_transforms):
        if not transforms:
            break
        src = transforms[rng.randrange(min(spine, len(transforms)) or len(transforms))]
        clone = Transform()
        proto2clone: dict[Dependency, Dependency] = {}
        for r in src.requires:
            parents = {proto2clone[p] for p in r.parents if p in proto2clone}  # type: ignore[index]
            proto2clone[r] = clone.AddRequirement(
                properties=set(r.properties), parents=parents or None
            )
        for gi, group in enumerate(src.produces):
            if gi > 0:
                clone.NewProductGroup()
            for p in group:
                clone.AddProduct(properties=set(p.properties))
        transforms.append(clone)

    given: list[set[Endpoint]] = []
    for _ in range(max(1, d.n_given_groups)):
        given.append({Endpoint(properties=_type_props(i)) for i in range(d.n_given)})

    target = Transform()
    top = d.n_types - 1
    if rng.random() < d.target_lineage and top >= 2:
        anchor = target.AddRequirement(properties=_type_props(rng.randint(1, top - 1)))
        target.AddRequirement(properties=_type_props(top), parents={anchor})
    else:
        target.AddRequirement(properties=_type_props(top))

    return SolverProblem(
        given=given,
        transforms=transforms,
        target=target,
        name=name or f"gen-{seed}",
        dials=d,
    )


def forward_closure_solvable(problem: SolverProblem) -> bool:
    have: list[set[str]] = [set(e.properties) for group in problem.given for e in group]

    def _met(dep: Dependency) -> bool:
        return any(set(dep.properties).issubset(h) for h in have)

    changed = True
    while changed:
        changed = False
        for tr in problem.transforms:
            if not all(_met(r) for r in tr.requires):
                continue
            for group in tr.produces:
                for p in group:
                    ps = set(p.properties)
                    if not any(ps == h for h in have):
                        have.append(ps)
                        changed = True
    return all(_met(r) for r in problem.target.requires)


@dataclass(frozen=True)
class _Ep:
    props: frozenset[str]
    parents: frozenset["_Ep"]

    def ancestors(self) -> frozenset["_Ep"]:
        out: set[_Ep] = set()
        todo = list(self.parents)
        while todo:
            p = todo.pop()
            if p in out:
                continue
            out.add(p)
            todo.extend(p.parents)
        return frozenset(out)


def exhaustive_solvable(
    problem: SolverProblem,
    *,
    max_applications: int = 6,
    node_cap: int = 200_000,
) -> bool | None:
    if len(problem.transforms) > 8:
        return None

    start: set[_Ep] = set()
    for group in problem.given:
        for e in group:
            start.add(_Ep(frozenset(e.properties), frozenset()))

    def _bindings(tr: Transform, have: frozenset[_Ep]):
        reqs = tr.requires
        if not reqs:
            yield {}
            return
        partial: list[tuple[int, dict[Dependency, _Ep]]] = [(0, {})]
        while partial:
            i, used = partial.pop()
            dep = reqs[i]
            for ep in have:
                if not set(dep.properties).issubset(ep.props):
                    continue
                ok = True
                for proto in dep.parents:
                    anchor = used.get(proto)  # type: ignore[arg-type]
                    if anchor is None:
                        ok = False
                        break
                    if anchor is not ep and anchor not in ep.ancestors():
                        ok = False
                        break
                if not ok:
                    continue
                nxt = dict(used)
                nxt[dep] = ep
                if i + 1 == len(reqs):
                    yield nxt
                else:
                    partial.append((i + 1, nxt))

    def _solved(have: frozenset[_Ep]) -> bool:
        for _ in _bindings(problem.target, have):
            return True
        return False

    seen: set[frozenset[_Ep]] = set()
    todo: list[tuple[frozenset[_Ep], int]] = [(frozenset(start), 0)]
    visited = 0
    exhausted = True
    while todo:
        have, depth = todo.pop()
        if have in seen:
            continue
        seen.add(have)
        visited += 1
        if visited > node_cap:
            exhausted = False
            break
        if _solved(have):
            return True
        if depth >= max_applications:
            exhausted = False
            continue
        for tr in problem.transforms:
            for used in _bindings(tr, have):
                lineage: set[_Ep] = set(used.values())
                for ep in used.values():
                    lineage.update(ep.parents)
                frozen = frozenset(lineage)
                grown = set(have)
                for group in tr.produces:
                    for p in group:
                        grown.add(_Ep(frozenset(p.properties), frozen))
                nxt = frozenset(grown)
                if nxt != have and nxt not in seen:
                    todo.append((nxt, depth + 1))
    return False if exhausted else None
