"""A library view walks its mask in a fixed order, not set order.

`DataInstanceLibraryView._mask` is a `set[Path]`, and `Path.__hash__` is the
string hash — which python randomizes per process. Walking it unsorted handed
the planner its transforms in a different order every run, and where two
transforms are interchangeable (same input and output types, different tool)
the planner then chose a different one each time. Solving one shipped template
in two processes produced two different plans, with two different tools, from
identical inputs.

Nothing downstream announced it: both plans are valid, both compile, and the
fast suite never solves a real library twice. It surfaced only once a
topological fingerprint was taken across two runs of unchanged code.
"""

from __future__ import annotations

from pathlib import Path

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


def _library(tmp_path: Path, names: list[str]) -> DataInstanceLibrary:
    types = DataTypeLibrary()
    types["thing"] = Endpoint(properties={"thing"})
    types_path = tmp_path / "mock.yml"
    types.Save(types_path)

    lib = DataInstanceLibrary(tmp_path / "lib.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for n in names:
        (lib.location / f"{n}.txt").write_text(n, encoding="utf-8")
        lib.AddItem(Path(f"{n}.txt"), "mock::thing")
    return lib


def test_a_masked_view_iterates_in_sorted_order(tmp_path: Path):
    names = ["zeta", "alpha", "mu", "beta", "omega", "gamma"]
    lib = _library(tmp_path, names)
    mask = set(lib.manifest)
    view = lib.AsView(mask)
    walked = [p for p, _, _ in view.Iterate()]
    assert walked == sorted(mask), (
        "view iteration order is set order, which is process-random -- the "
        "planner's transform choice rides on it"
    )


def test_an_unmasked_view_iterates_in_sorted_order(tmp_path: Path):
    lib = _library(tmp_path, ["c", "a", "b"])
    view = lib.AsView(set(lib.manifest))
    walked = [p for p, _, _ in view.Iterate()]
    assert walked == sorted(walked)
