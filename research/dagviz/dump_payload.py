#!/usr/bin/env python3
"""Write the solver payload for a two-case E2 problem, without solving it here.

`CallEngine` is intercepted rather than re-implemented: what lands on disk is
the exact bytes the engine was handed, so `msm_solver solve < payload.json`
reproduces the refusal outside Python.
"""

import json
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, "/home/tony/agentic_workspace/projects/metasmith/engine/bench-run/research/metasmith_benchmark/drivers")
os.environ.setdefault("E2_CACHE_DIR", str(HERE / "e2cache"))

import _common as c  # noqa: E402
import e2_cami as e2  # noqa: E402
from metasmith.models import solver_engine  # noqa: E402
from metasmith.models.libraries.instances import DataInstanceLibraryView  # noqa: E402
from metasmith.models.solver import Transform, solve_by_mcts  # noqa: E402
from metasmith.models.workflow.plan import CollectSolverInputs  # noqa: E402
from metasmith.python_api import DataInstanceLibrary, TransformInstanceLibrary  # noqa: E402

OUT = Path(os.environ.get("OUT", HERE / "payload.json"))
ARMS = os.environ.get("ARMS", "short,long").split(",")
TARGETS = os.environ.get("TARGETS", "e2::assembly").split(",")

_real = solver_engine.CallEngine


def _tap(info, subcommand, payload=None, timeout=None):
    if subcommand == "solve" and payload is not None:
        OUT.write_text(json.dumps(payload))
        print(f"payload -> {OUT} ({OUT.stat().st_size // 1024} KiB)")
    return _real(info, subcommand, payload, timeout)


solver_engine.CallEngine = _tap
# `solver_wire` bound the name at import time.
from metasmith.models import solver_wire  # noqa: E402

solver_wire.CallEngine = _tap

# The payload carries property INDICES; the names live only on this side, so the
# sidecar is what makes a dumped plan readable.
_encode = solver_wire.encode_problem


def _tap_encode(*a, **kw):
    enc = _encode(*a, **kw)
    side = OUT.with_name(OUT.stem + "_names.json")
    side.write_text(json.dumps({
        "properties": enc.properties,
        "transform_names": [
            getattr(t, "_name", None) or f"transform_{i}"
            for i, t in enumerate(enc.transforms)
        ],
    }))
    print(f"names   -> {side}")
    return enc


solver_wire.encode_problem = _tap_encode


def main():
    arms = e2.enumerate_arms()
    cache = Path(os.environ["E2_CACHE_DIR"])
    groups = []
    for arm in ARMS:
        smith = c.agent_for("cami", False, cache / f"dryrun_home_{arm}")
        inputs, globals_lib = e2.declare_givens(smith, arm, arms[arm][:1], ensure=False)
        res = [
            DataInstanceLibraryView(lib) for lib in (
                DataInstanceLibrary.Load(c.LIBRARY / "resources" / "e2"),
                DataInstanceLibrary.Load(c.MLIB / "resources" / "lib"),
                globals_lib,
            )
        ]
        groups += [[v] + res for v in inputs.AsSamples("e2::read_metadata")]
    trlib = TransformInstanceLibrary.Load(c.LIBRARY / "transforms" / "e2")
    _, cases, transform2inst, _ = CollectSolverInputs(groups, [trlib])
    target = Transform()
    for name in TARGETS:
        target.AddRequirement(example=trlib.GetType(name))
    print(f"{len(cases)} cases, {len(transform2inst)} transforms")
    try:
        solve_by_mcts(given=cases, target=target, transforms=list(transform2inst.keys()))
    except Exception as e:
        print("refused:", str(e).splitlines()[0][:200])


if __name__ == "__main__":
    sys.exit(main())
