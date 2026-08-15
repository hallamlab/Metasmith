"""The bake gate: fast enough that nobody is tempted to skip it.

WHAT THIS IS FOR. Until the bake became a package, nothing here was reachable
except through a staged path and a subprocess, which is why none of it had a
test. The whole point of the move was to make the delicate parts assertable, and
the whole point of THIS suite is to stay in the seconds so it actually runs on
every edit. The way to keep it there is to run FEWER checks, not faster ones --
sulfur rather than carbon, ten reactions rather than the universe. If it starts
costing minutes, cut tests.

WHAT IT DOES NOT PROVE, which is worth stating because the campaign's stop line
is written in coverage and this measures none of it. It never invokes the five
external tools, so a shifted rdkit or torch inside a bake image -- the one thing
that would genuinely move biochemical accuracy -- is invisible here. It sees ten
hand-written reactions and one element's slice of a real bake, so nothing that
only appears across 83,796 reactions is in scope. It proves the METHOD does what
it says, not that a BAKE is unchanged; that answer is the full-universe
measurement, run separately.

ENVIRONMENT, and why it is two of them. The full gate wants rdkit AND scipy in
one interpreter, and no shipped env carries both ON PURPOSE: the bake images are
rdkit or torch, the measurement env is scipy or cobra, and keeping that seam is
the property `test_module_surface` exists to defend. So the gate degrades by
skipping rather than by failing, and the two useful runs are

    mamba run -n ecspr         python -m pytest tests/ecspr        # all of it, less rdkit
    mamba run -n rdkit-scratch python -m pytest tests/ecspr/bake   # all of the bake

A skip here is a real gap, not a pass. If a change touches the extractor or the
atom count, the second command is the one that has to be run.
"""
from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
DEPLOYED_BAKE = REPO / "data" / "fabfos" / "processed" / "metabolism_bake"


@pytest.fixture(scope="session")
def deployed_bake() -> dict:
    """The trio as it sits on disk, or a skip.

    A real artifact rather than a fixture, deliberately: the packing, the
    vocabulary join and the ratio flip are arithmetic over 2.4M rows, and a
    ten-row fixture would exercise the code without exercising the arithmetic.
    It is a DVC-materialised chunk, so a scope that has not mounted it skips
    rather than fails -- an unmounted chunk is a configuration fact, not a bug.
    """
    paths = {k: DEPLOYED_BAKE / f"{k}.parquet"
             for k in ("vocab", "atom_pairs", "direction")}
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        pytest.skip(f"the deployed metabolism_bake is not materialised here: {missing}")
    return paths
