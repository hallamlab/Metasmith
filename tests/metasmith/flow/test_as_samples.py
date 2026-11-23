from __future__ import annotations

from pathlib import Path

import pytest

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


def test_as_samples_shared_ancestor_is_context_not_a_merge(tmp_path: Path):
    lib = DataInstanceLibrary(tmp_path / "shared.xgdb")
    _types(lib)
    meta = lib.AddItem(Path("meta.json"), "t::meta")
    for i in range(3):
        idx = lib.AddItem(Path(f"s{i}/id.txt"), "t::idx", parents={meta})
        lib.AddItem(Path(f"s{i}/reads.fq"), "t::rd", parents={idx})

    masks = _masks(lib, "t::idx")
    assert sorted(masks, key=sorted) == [
        {"meta.json", f"s{i}/id.txt", f"s{i}/reads.fq"} for i in range(3)
    ]


def test_as_samples_among_picks_index_items(tmp_path: Path):
    lib = DataInstanceLibrary(tmp_path / "among.xgdb")
    _types(lib)
    meta = lib.AddItem(Path("meta.json"), "t::meta")
    lib.AddItem(Path("shared.fq"), "t::rd", parents={meta})
    for i in range(3):
        idx = lib.AddItem(Path(f"s{i}/id.txt"), "t::idx", parents={meta})
        lib.AddItem(Path(f"s{i}/reads.fq"), "t::rd", parents={idx})

    picked = [{str(p) for p in v._mask} for v in lib.AsSamples("t::idx", among=["s1/id.txt"])]
    assert picked == [{"meta.json", "shared.fq", "s1/id.txt", "s1/reads.fq"}]
    with pytest.raises(AssertionError, match="s1/reads.fq"):
        list(lib.AsSamples("t::idx", among=["s1/reads.fq"]))


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
