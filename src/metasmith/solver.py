from typing import Iterable
from pathlib import Path
from dataclasses import dataclass

from .models.libraries import DataInstance, DataType, TransformInstanceLibrary, TransformInstance
from .models.solver import *

# concretely describes a solver.Application
@dataclass
class WorkflowStep:
    key: str
    transform_key: str
    uses: list[tuple[DataInstance, Endpoint]]
    produces: list[tuple[DataInstance, Endpoint]]
    transform: TransformInstance | Path

# concretely describes a solver.Result
@dataclass
class WorkflowPlan:
    given: list[tuple[DataInstance, Endpoint]]
    targets: list[tuple[DataInstance, Endpoint]]
    steps: list[WorkflowStep]

    def __len__(self):
        return len(self.steps)

class WorkflowSolver:
    def __init__(
            self,
            lib: TransformInstanceLibrary,
        ) -> None:
        self._transform_lib = lib

    def Solve(self, given: Iterable[DataInstance], target: Iterable[DataType], horizon: int=64, seed: int|None=None):
        def _solve(given: Iterable[Endpoint], target: Transform, transforms: Iterable[Transform], _debug=False):
            @dataclass
            class State:
                have: dict[Endpoint, Dependency]
                needed: set[Dependency]
                target: Dependency|Transform
                lineage_requirements: dict[Node, Endpoint]
                seen_signatures: set[str]
                depth: int

            def _get_producers_of(target: Dependency):
                for tr in transforms:
                    for p in tr.produces:
                        if p.IsA(target):
                            yield tr
                            break

            if _debug:
                log_path = Path("./cache/debug_log.txt")
                log_path.parent.mkdir(parents=True, exist_ok=True)
                log = open("./cache/debug_log.txt", "w")
                debug_print = lambda *args: log.write(" ".join(str(a) for a in args)+"\n") if args[0] != "END" else log.close()
            else:
                debug_print = lambda *args: None

            _apply_cache: dict[str, Application] = {}
            def _apply(target: Transform, inputs: Iterable[tuple[Endpoint, Node]]):
                sig  = "".join(e.key+d.key for e, d in inputs)
                if sig in _apply_cache:
                    return _apply_cache[sig]
                appl = target.Apply(inputs)
                _apply_cache[sig] = appl
                return appl

            def _satisfies_lineage(tproto: Dependency, candidate: Endpoint):
                for tp_proto in tproto.parents:
                    if all(not p.IsA(tp_proto) for p, _ in candidate.Iterparents()):
                        return False
                return True

            HORIZON=horizon
            def _solve_dep(s: State) -> list[DependencyResult]:
                if s.depth >= HORIZON:
                    if _debug: debug_print(f" <-  HORIZON", s.depth)
                    return []
                target: Dependency = s.target
                assert isinstance(target, Dependency), f"{s.target}, not dep"
                if _debug: debug_print(f" ->", s.target, s.lineage_requirements)
                if _debug: debug_print(f"   ", s.have.keys())

                candidates:list[DependencyResult] = []
                for e, eproto in s.have.items():
                    if not e.IsA(target): continue
                    acceptable = True
                    for rproto, r in s.lineage_requirements.items():
                        if e == r: continue
                        if eproto.IsA(rproto): # e is protype, but explicitly breaks lineage
                            acceptable=False; break

                        for p, pproto in e.Iterparents():
                            if rproto.IsA(pproto):
                                if p != r:
                                    acceptable=False; break

                    if not acceptable:
                        continue
                    else:
                        if _debug: debug_print(f"    ^candidate", e, eproto, e.parents)
                        if _debug: debug_print(f"    ^reqs.    ", s.lineage_requirements)
                        candidates.append(DependencyResult([], e))
                    # elif quality == 2:
                    #     if DEBUG: debug_print(f" <-", s.target, e, "DIRECT")
                    #     return [DepResult(0, [], e)]

                def _add_result(res: Result):
                    ep: Endpoint|None = None
                    for e in res.application.produced:
                        if e.IsA(target):
                            ep = e; break
                    assert isinstance(ep, Endpoint)
                    if not _satisfies_lineage(target, ep): return
                    candidates.append(DependencyResult(
                        res.dependency_plan+[res.application],
                        ep,
                    ))

                for tr in _get_producers_of(target):
                    # if target in tr.deletes: continue
                    results = _solve_tr(State(s.have, s.needed, tr, s.lineage_requirements, s.seen_signatures, s.depth))
                    for res in results:
                        _add_result(res)

                if _debug: debug_print(f" <-", s.target, f"{len(candidates)} sol.", candidates[0].endpoint if len(candidates)>0 else None)
                return candidates

            _transform_cache: dict[str, list[Result]] = {}
            def _solve_tr(s: State) -> list[Result]:
                assert isinstance(s.target, Transform), f"{s.target} not tr"
                target: Transform = s.target
                if _debug: debug_print(f">>>{s.depth:02}", s.target, s.lineage_requirements)
                for h in s.have:
                    if _debug: debug_print(f"      ", h)

                # memoization
                sig = "".join(e.key for e in s.have)
                sig += f":{s.target.key}"
                sig += ":"+"".join(e.key for e in s.lineage_requirements.values())
                if sig in _transform_cache:
                    if _debug: debug_print(f"<<<{s.depth:02} CACHED: {len(_transform_cache[sig])} solutions")
                    return _transform_cache[sig]
                if sig in s.seen_signatures:
                    if _debug: debug_print(f"<<<{s.depth:02} FAIL: is loop")
                    return []

                plans: list[list[DependencyResult]] = []
                for i, req in enumerate(s.target.requires):
                    req_p = {}
                    for proto, e in s.lineage_requirements.items():
                        if req.IsA(proto): continue
                        req_p[proto] = e

                    results = _solve_dep(State(s.have, s.needed|{req}, req, req_p, s.seen_signatures|{sig}, s.depth+1))
                    
                    if len(results) == 0:
                        if _debug: debug_print(f"<<< FAIL", s.target, req)
                        return []
                    else:
                        plans.append(results)

                def _gather_valid_inputs():
                    valids: list[list[DependencyResult]] = []
                    ii = 0
                    def _gather(req_i: int, req: Dependency, res: DependencyResult, deps: dict, used: set[Endpoint], inputs: list[DependencyResult]):
                        nonlocal ii; ii += 1         
                        if _debug: debug_print(f"          ", deps)
                        if _debug: debug_print(f"    ___", req, req.parents)
                        if _debug: debug_print(f"        __", res.endpoint, list(res.endpoint.Iterparents()))
                        if res.endpoint in used:
                            if _debug: debug_print(f"    ___ FAIL: duplicate input", res.endpoint)
                            return
                        # used.add(res.endpoint)

                        if not _satisfies_lineage(req, res.endpoint):
                            if _debug: debug_print(f"    ___ FAIL: unsatisfied lineage", req)
                            return

                        for rproto in req.parents:
                            r = deps[rproto]
                            # if all(not p.IsA(rproto) for p, pproto in res.endpoint.Iterparents()):
                            #     if DEBUG: debug_print(f"    ___ FAIL: unsatisfied lineage", rproto)
                            #     _fail=True; break
                            res_parents = list(res.endpoint.Iterparents())
                            res_parents.reverse()
                            for p, pproto in res_parents:
                                if not p.IsA(rproto): continue
                                if p!=r:
                                    if _debug: debug_print(f"    ___ FAIL: lineage mismatch", p, r)
                                    return
                                else:
                                    break # in the case of asm -> bin, the closest ancestor takes priority
                        # deps[req] = res.endpoint

                        if req_i >= len(target.requires)-1:
                            valids.append(inputs+[res])
                        else:
                            req_i += 1
                            for i, next_res in enumerate(plans[req_i]):
                                _gather(req_i, target.requires[req_i], next_res, deps|{req:res.endpoint}, used|{res.endpoint}, inputs+[res])
                    req_i = 0
                    for i, next_res in enumerate(plans[req_i]):
                        _gather(0, target.requires[req_i], next_res, {}, set(), [])
                    total = 1
                    for s in plans:
                        total *= len(s)
                    if _debug: debug_print(f"    ## {ii} visited, {total} combos")
                    return valids

                if _debug: debug_print(f"<<<{s.depth:02}", s.target, s.lineage_requirements)
                if _debug: debug_print(f"     ", [len(x) for x in plans])
                solutions: list[Result] = []
                # for inputs in _iter_satisfies():
                for inputs in _gather_valid_inputs():
                    my_appl = _apply(s.target, [(res.endpoint, req) for req, res in zip(s.target.requires, inputs)])
                    consolidated_plan: list[Application] = []
                    produced_sigs: set[str] = {p.Signature() for p in my_appl.produced}
                    # if DEBUG: debug_print(f"   __", my_appl)
                    for res in inputs:
                        for appl in res.plan:
                            if all(p.Signature() in produced_sigs for p in appl.produced): continue
                            consolidated_plan.append(appl)
                            produced_sigs = produced_sigs.union(p.Signature() for p in appl.produced)
                    solutions.append(Result(
                        my_appl,
                        consolidated_plan,
                    ))
                    # if DEBUG: debug_print(f"    *", my_appl)
                    # if DEBUG: debug_print(f"     ", [res.endpoint for res in inputs])
                    # if DEBUG: debug_print(f"    .", target.requires)
                    # for appl in consolidated_plan:
                    #     if DEBUG: debug_print(f"    __", appl)
                if _debug: debug_print(f"     ", f"{len(solutions)} sol.", solutions[0].application.produced if len(solutions)>0 else None)
                solutions = sorted(solutions, key=lambda s: len(s))
                _transform_cache[sig] = solutions
                return solutions

            input_tr = Transform(target._ns)
            given_dict = {g:input_tr.AddProduct(g.properties) for g in given}
            res = _solve_tr(State(given_dict, set(), target, {}, set(), 0))
            if _debug: debug_print("END")
            return res

        _namespace = Namespace(seed=seed)
        _transform_map: dict[Transform, TransformInstance] = {}
        _prototype_instances: dict[Dependency, DataInstance] = {}
        def _parse_transform(tr: TransformInstance):
            model = Transform(_namespace)
            _transform_map[model] = tr
            for x in tr.input_signature:
                model.AddRequirement(x.AsProperties())
            for x in tr.output_signature:
                dep = model.AddProduct(x.type.AsProperties())
                _prototype_instances[dep] = x
            return model
        _transforms = [_parse_transform(t) for p, t in self._transform_lib]

        data_instances = {Endpoint(_namespace, x.type.AsProperties()):x for x in given}
        given_instances = {k for k in data_instances}
        output_map: dict[Dependency, DataType] = {}
        target_tr = Transform(_namespace)

        _target_deps: dict[str, Dependency] = {}
        def _add_target(t: DataType, top=False):
            if t in _target_deps: return
            for a in t.ancestors:
                _add_target(a)
            parents = {_target_deps[a.name] for a in t.ancestors}
            dep = target_tr.AddRequirement(t.AsProperties(), parents=parents)
            _target_deps[t.name] = dep
            if top:
                output_map[dep] = t
        for t in target:
            _add_target(t, top=True)
        solutions = _solve(data_instances, target_tr, _transforms)
        if len(solutions) == 0: return

        solution: Result = solutions[0] # just pick first solution
        steps = []
        for appl in solution.dependency_plan:
            tr = _transform_map[appl.transform]
            appl.used = dict(sorted(appl.used.items(), key=lambda x: x[0].key))
            appl.produced = dict(sorted(appl.produced.items(), key=lambda x: x[0].key))
            used = [(data_instances[x], x) for x in appl.used]
            produced = []
            for e, dep in appl.produced.items():
                inst = _prototype_instances[dep]
                data_instances[e] = inst    
                produced.append((inst, e))
            
            key =  _namespace._kg.FromStr(appl.transform.key+''.join(x.key for x in appl.used), 5)
            steps.append(WorkflowStep(key, appl.transform.key, used, produced, tr))
        return WorkflowPlan(
            [(x, e) for e, x in data_instances.items() if e in given_instances],
            [(data_instances[e], e) for e in solution.application.used],
            steps,
        )