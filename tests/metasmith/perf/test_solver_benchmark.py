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
    from metasmith.testing.solver_bench import run_templates

    cases = run_templates(metasmith_libraries_root)
    assert cases, f"no templates discovered under {metasmith_libraries_root}"
    unsound = {k: v["violations"] for k, v in cases.items() if not v["ok"]}
    assert not unsound, unsound


def test_the_benchmark_runner_reports_every_case_it_ran():
    from metasmith.testing.solver_bench import CORPUS, diff, run_generated

    cases = run_generated(CORPUS[:2])
    assert set(cases) == {f"gen/{name}" for name, _, _ in CORPUS[:2]}
    for case in cases.values():
        assert case["ok"], case["violations"]
        assert case["fingerprint"]

    snapshot = {"cases": cases, "total_seconds": 1.0}
    assert not any(line.startswith("!!") for line in diff(snapshot, snapshot))
