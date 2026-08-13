"""The solver's perf corpus, and the determinism the corpus depends on.

Two things live here, both needing the sibling ``metasmith-libraries`` checkout:

* a cross-process determinism pin — the property that makes every fingerprint
  comparison downstream mean anything;
* the benchmark runner's own smoke test, so a change to the reporting cannot
  silently stop reporting.

The wall-time numbers themselves are not asserted. Timing thresholds on a
developer laptop are a flake generator; the A/B comparison against a recorded
baseline (``python -m metasmith.testing.solver_bench --baseline``) is where a
performance claim gets settled.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

_CHILD = textwrap.dedent(
    """
    import sys
    from pathlib import Path
    from metasmith.agents import Template
    from metasmith.testing.solver_verification import plan_fingerprint

    root, name = Path(sys.argv[1]), sys.argv[2]
    template = [t for t in Template.Discover(root) if t.name == name][0]
    task = template.spec.Solve()
    print("FINGERPRINT=" + plan_fingerprint(task.plan._solver_result))
    """
)


def _solve_in_child(root: Path, name: str, hash_seed: str) -> str:
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(Path.home()),
        "PYTHONHASHSEED": hash_seed,
        "PYTHONPATH": str(Path(__file__).resolve().parents[3] / "src"),
    }
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD, str(root), name],
        capture_output=True,
        text=True,
        env=env,
        timeout=600,
    )
    assert proc.returncode == 0, proc.stderr[-4000:]
    line = [x for x in proc.stdout.splitlines() if x.startswith("FINGERPRINT=")]
    assert line, proc.stdout[-4000:] + proc.stderr[-4000:]
    return line[-1].split("=", 1)[1]


def test_one_template_solves_to_one_plan_whatever_the_hash_seed(
    metasmith_libraries_root: Path,
):
    """Two processes, two string-hash seeds, one plan.

    `Path.__hash__` is the string hash, and python randomizes it per process.
    A library view that walked its mask in set order therefore handed the
    planner its transforms in a different order every run, and where two
    transforms are interchangeable the planner chose a different one — the same
    template produced two different plans, with two different tools, from
    identical inputs. Nothing else in the suite can see that: it needs two
    processes and a topological comparison of the results.
    """
    name = "pangenome_heatmap_from_assembly"
    if not (metasmith_libraries_root / "templates" / name).exists():
        pytest.skip(f"{name} is not in this libraries checkout")
    a = _solve_in_child(metasmith_libraries_root, name, "0")
    b = _solve_in_child(metasmith_libraries_root, name, "1")
    assert a == b, (
        "the same template solved to two different plans in two processes -- "
        "something on the planning path iterates a set of strings or Paths"
    )


def test_every_shipped_template_solves_to_a_runnable_plan(
    metasmith_libraries_root: Path,
):
    """The checker, pointed at the plans people actually run.

    The generated corpus is where `check_plan` earns its keep, but a harness
    that has never judged a shipped template is a harness with an untested
    claim at its centre. This is also the containment check on the cyclic-graph
    unsoundness in `tests/solver/test_known_unsound.py`: those transform graphs
    are generated, and this is what says the shipped ones are not that shape.

    Slow on purpose -- `metagenomics_from_paired_reads` is ~35s of real search.
    """
    from metasmith.testing.solver_bench import run_templates

    cases = run_templates(metasmith_libraries_root)
    assert cases, f"no templates discovered under {metasmith_libraries_root}"
    unsound = {k: v["violations"] for k, v in cases.items() if not v["ok"]}
    assert not unsound, unsound


def test_the_benchmark_runner_reports_every_case_it_ran():
    """Cheap guard on the reporting, not on the solver."""
    from metasmith.testing.solver_bench import CORPUS, diff, run_generated

    cases = run_generated(CORPUS[:2])
    assert set(cases) == {f"gen/{name}" for name, _, _ in CORPUS[:2]}
    for case in cases.values():
        assert case["ok"], case["violations"]
        assert case["fingerprint"]

    snapshot = {"cases": cases, "total_seconds": 1.0}
    assert not any(line.startswith("!!") for line in diff(snapshot, snapshot))
