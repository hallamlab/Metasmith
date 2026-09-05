"""Why does the refiner reject every candidate, and how many are really admissible?

    python research/metasmith/witness_sweep/profiling/relation_probe.py templates 256 7,42,99
    python research/metasmith/witness_sweep/profiling/relation_probe.py arm unpinned 1

`validate_node` decides ancestry over the step graph. A branched given application
carries `used == {}`, so the step graph gives a given endpoint no parents, and every
requirement whose lineage anchor binds to a given fails. This counts the rejections by
cause, and re-judges the same candidates by the relation the specification uses -- the
reflexive transitive closure over `Endpoint.parents`, which `solver_spec.py` implements
and `solver_witness` decides.

The instrumentation is applied to a *copy* of `src/`, never to the tree. The copy lands
under `$CLAUDE_JOB_DIR/tmp` when that is set, else a temporary directory.

**CAUTION** Neither relation is usable alone, and they fail in opposite directions.
The step graph is blind to given siblings, so it rejects every candidate on six of the
eleven templates. Declared parents read the stale values `mock_produced` leaves on an
intermediate candidate, so they refuse sound improvements: on
`isolate_assembly_from_long_reads` at seeds 7 and 99, three of the four candidates the
step graph accepts are refused by declared parents, and staleness is the cause of all
three. The witness accepts the plans those candidates lead to. Mint truthful parents
under the swap first, then change the relation. Either alone regresses.

**CAUTION** The lineage loop is written twice -- `_lineage_ok` runs it and `_is_valid`
runs it again after its scheduling check. This patches both. A fix that moves only one
changes nothing.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]

PATCHES = [
    # module-level counters
    ("from __future__ import annotations",
     "from __future__ import annotations\n\nPROBE = {}\n"
     "def _probe(k, n=1): PROBE[k] = PROBE.get(k, 0) + n\n"),
    # split the scheduling half of _is_valid out, and add the declared relation
    ("""            def _is_valid(target_appl: Application):
                have: set[Endpoint] = {
                    e for pgroup in given_appl.produced for e in pgroup.values()
                }""",
     """            def _declared_has_ancestor(e, a):
                if e == a: return True
                todo, seen = list(e.parents), {e}
                while todo:
                    x = todo.pop()
                    if x == a: return True
                    if x in seen: continue
                    seen.add(x); todo.extend(x.parents)
                return False

            def _declared_lineage_ok():
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            if not _declared_has_ancestor(e, step.used[pproto]): return False
                return True

            def _structural_ok(target_appl: Application):
                have: set[Endpoint] = {
                    e for pgroup in given_appl.produced for e in pgroup.values()
                }
                pending: list[Application] = list(state.steps)
                while len(pending) > 0:
                    ready = [s for s in pending if all(e in have for e in s.used.values())]
                    if len(ready) == 0: return False
                    for s in ready:
                        have |= {e for pgroup in s.produced for e in pgroup.values()}
                    scheduled = {id(s) for s in ready}
                    pending = [s for s in pending if id(s) not in scheduled]
                return True

            def _is_valid(target_appl: Application):
                have: set[Endpoint] = {
                    e for pgroup in given_appl.produced for e in pgroup.values()
                }"""),
    # attribute each rejection to a cause
    ("""            def _lineage_ok():
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            lineage_constraint_e = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, lineage_constraint_e): return False
                return True
""",
     """            def _lineage_ok():
                ok = True
                for step in _iter_steps():
                    for p, e in step.used.items():
                        for pproto in p.parents:
                            a = step.used[pproto] # type: ignore
                            if not _has_ancestor(e, a):
                                if ok:
                                    if a not in produced_from: _probe("reject_anchor_missing")
                                    elif len(produced_from[a]) == 0: _probe("reject_anchor_is_given")
                                    else: _probe("reject_anchor_other")
                                ok = False
                return ok
"""),
    ("        _slot_cache: dict[Transform, list[tuple[Dependency, Dependency]]] = {}",
     "        _declared_flag = [False]\n"
     "        _slot_cache: dict[Transform, list[tuple[Dependency, Dependency]]] = {}"),
    # count each candidate under both relations
    ("""            target_appl = _get_target()
            if target_appl is None:
                state.valid = False
            else:
                try:
                    rejected = not _lineage_ok()
                except KeyError:
                    rejected = False
                state.valid = False if rejected else _is_valid(target_appl)
""",
     """            target_appl = _get_target()
            _probe("candidates")
            if target_appl is None:
                state.valid = False
                _probe("no_target")
            else:
                try:
                    rejected = not _lineage_ok()
                except KeyError:
                    rejected = False
                    _probe("keyerror")
                state.valid = False if rejected else _is_valid(target_appl)
                if state.valid: _probe("valid_current")
                try: d_ok = _declared_lineage_ok()
                except Exception: d_ok = False; _probe("declared_error")
                st_ok = _structural_ok(target_appl)
                if d_ok and st_ok: _probe("valid_declared")
                _declared_flag[0] = bool(d_ok and st_ok)
                if state.valid and not d_ok:
                    # accepted by the step graph, refused by declared parents.
                    _probe("stepgraph_ok_declared_no")
                    fail = None
                    for step in _iter_steps():
                        for p, e in step.used.items():
                            for pproto in p.parents:
                                a = step.used[pproto] # type: ignore
                                if fail is None and not _declared_has_ancestor(e, a):
                                    fail = (e, a)
                    e, _a = fail
                    src = None
                    for st in state.steps:
                        for g in st.produced:
                            if e in g.values(): src = st
                    if src is None: _probe("stale_na_given")
                    elif not set(src.used.values()).issubset(set(e.parents)):
                        _probe("declared_no_because_stale")
                    else: _probe("declared_no_other")
"""),
    # the best score an admissible candidate reaches, against the incumbent
    ("""            score = e_score*1000+lin_score
            vscore = score*state.valid
            state.scores = [score, vscore]""",
     """            score = e_score*1000+lin_score
            vscore = score*state.valid
            state.scores = [score, vscore]
            if _declared_flag[0]:
                PROBE["best_declared"] = max(PROBE.get("best_declared", float("-inf")), score)"""),
    ("        score_node(initial_state)\n",
     "        score_node(initial_state)\n"
     '        PROBE["incumbent"] = initial_state.scores[0]\n'
     '        PROBE["best_declared"] = float("-inf")\n'),
]

COLUMNS = ("candidates", "valid_current", "valid_declared", "reject_anchor_is_given",
           "reject_anchor_other", "reject_anchor_missing", "keyerror",
           "stepgraph_ok_declared_no", "declared_no_because_stale", "declared_no_other")


def build_tree() -> Path:
    base = Path(os.environ.get("CLAUDE_JOB_DIR", "")) / "tmp" if os.environ.get("CLAUDE_JOB_DIR") \
        else Path(tempfile.mkdtemp(prefix="relation_probe_"))
    base.mkdir(parents=True, exist_ok=True)
    dst = base / "relation_probe_src"
    if dst.exists(): shutil.rmtree(dst)
    shutil.copytree(ROOT / "src", dst)
    target = dst / "metasmith" / "models" / "solver.py"
    s = target.read_text()
    for old, new in PATCHES:
        assert old in s, f"patch anchor not found:\n{old[:120]}"
        s = s.replace(old, new, 1)
    target.write_text(s)
    return dst


DRIVER = r'''
import sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
ROOT = Path(sys.argv[2])
import metasmith.models.solver as S
assert "relation_probe_src" in S.__file__, S.__file__
from metasmith.models.solver_backend import Backend, UsePythonSolver
COLUMNS = %(cols)r
mode = sys.argv[3]

def row(name, P, extra=""):
    inc, bd = P.get("incumbent", float("nan")), P.get("best_declared", float("-inf"))
    gain = f"{bd-inc:9.3f}" if bd != float("-inf") else "     none"
    print(f"{name:40s} " + " ".join(f"{P.get(k,0):9,d}" for k in COLUMNS) + f" {gain}{extra}")

print(f"{'case':40s} " + " ".join(f"{k[:9]:>9s}" for k in COLUMNS) + f" {'gain':>9s}")
if mode == "templates":
    from metasmith.agents import Template
    refine = int(sys.argv[4])
    seeds = [int(x) for x in sys.argv[5].split(",")] if len(sys.argv) > 5 else [42]
    tpl = {t.name: t for t in Template.Discover(ROOT / "src" / "metasmith_libraries")}
    for name in sorted(tpl):
        for seed in seeds:
            with UsePythonSolver():
                S.PROBE.clear()
                tpl[name].spec.Solve(max_refine=refine, seed=seed)
            row(f"{name}@{seed}", S.PROBE)
else:
    sys.path.insert(0, str(ROOT / "src" / "metasmith_libraries"))
    sys.path.insert(0, str(ROOT / "research/metasmith/witness_sweep"))
    import metasmith.agents.spec, metasmith.testing.solver_spec
    import metasmith.testing.solver_verification, metasmith.testing.witness_check
    import duplicate_work as DW
    assert "relation_probe_src" in S.__file__, S.__file__
    arm, refine = sys.argv[4], int(sys.argv[5])
    with UsePythonSolver():
        S.PROBE.clear()
        t0 = time.perf_counter()
        DW.solve(arm, max_refine=refine)
        dt = time.perf_counter() - t0
    row(f"{arm} (max_refine={refine})", S.PROBE, f"  {dt:.1f}s")
'''


def main() -> int:
    argv = sys.argv[1:] or ["templates", "8"]
    tree = build_tree()
    driver = tree.parent / "relation_probe_driver.py"
    driver.write_text(DRIVER % {"cols": COLUMNS})
    cmd = [sys.executable, str(driver), str(tree), str(ROOT)] + argv
    return subprocess.call(cmd)


if __name__ == "__main__":
    raise SystemExit(main())
