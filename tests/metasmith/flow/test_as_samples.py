from __future__ import annotations

from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.solver import Endpoint


def _types(lib: DataInstanceLibrary):
    lib.types["t"] = {
        "idx": Endpoint(properties={"idx"}),
        "rd": Endpoint(properties={"rd"}),
        "meta": Endpoint(properties={"meta"}),
    }


def _masks(lib: DataInstanceLibrary, index_type: str) -> list[set[str]]:
    return [{str(p) for p in v._mask} for v in lib.AsSamples(index_type)]


def test_as_samples_root_index_splits_per_item(tmp_path: Path):
    lib = DataInstanceLibrary(tmp_path / "roots.xgdb")
    _types(lib)
    for i in range(3):
        idx = lib.AddItem(Path(f"s{i}/id.txt"), "t::idx")
        lib.AddItem(Path(f"s{i}/reads.fq"), "t::rd", parents={idx})

    masks = _masks(lib, "t::idx")
    assert len(masks) == 3
    # the defect this pins returned three views of the right count whose
    # contents were the *first* item's descendants, silently
    assert sorted(masks, key=sorted) == [
        {f"s{i}/id.txt", f"s{i}/reads.fq"} for i in range(3)
    ]


def test_as_samples_shared_ancestor_collapses_to_one(tmp_path: Path):
    lib = DataInstanceLibrary(tmp_path / "shared.xgdb")
    _types(lib)
    meta = lib.AddItem(Path("meta.json"), "t::meta")
    for i in range(3):
        idx = lib.AddItem(Path(f"s{i}/id.txt"), "t::idx", parents={meta})
        lib.AddItem(Path(f"s{i}/reads.fq"), "t::rd", parents={idx})

    masks = _masks(lib, "t::idx")
    assert len(masks) == 1
    assert masks[0] == set(str(p) for p in lib.manifest)


def test_as_samples_root_index_deep_subtree(tmp_path: Path):
    lib = DataInstanceLibrary(tmp_path / "deep.xgdb")
    _types(lib)
    for i in range(2):
        idx = lib.AddItem(Path(f"s{i}/id.txt"), "t::idx")
        rd = lib.AddItem(Path(f"s{i}/reads.fq"), "t::rd", parents={idx})
        lib.AddItem(Path(f"s{i}/trimmed.fq"), "t::rd", parents={rd})

    masks = _masks(lib, "t::idx")
    assert sorted(masks, key=sorted) == [
        {f"s{i}/id.txt", f"s{i}/reads.fq", f"s{i}/trimmed.fq"} for i in range(2)
    ]
