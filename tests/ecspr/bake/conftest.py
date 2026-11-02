from __future__ import annotations

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
DEPLOYED_BAKE = REPO / "data" / "fabfos" / "processed" / "metabolism_bake"


@pytest.fixture(scope="session")
def deployed_bake() -> dict:
    paths = {k: DEPLOYED_BAKE / f"{k}.parquet"
             for k in ("vocab", "atom_pairs", "direction")}
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        pytest.skip(f"the deployed metabolism_bake is not materialised here: {missing}")
    return paths
