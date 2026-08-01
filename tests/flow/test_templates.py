"""`Template`: a deferred spec, on disk, that travels.

The property worth pinning is portability. A template is written in one
checkout and read in another, so every library reference in it has to be
relative to the repository root and resolved against wherever that root turns
out to be. The failure mode of getting this wrong is quiet: the spec loads, the
solve reports an unknown type, and nothing points at the absolute path that
came along for the ride.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from metasmith.agents import Spec, Template
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.paths import DEFERRED
from metasmith.testing import mock_transforms as mt

from .conftest import _build_transform_lib, _build_type_lib


def _repo(root: Path) -> tuple[Path, Spec]:
    """A miniature library repository holding one deferred template."""
    (root / "data_types").mkdir(parents=True, exist_ok=True)
    types_path = _build_type_lib(root / "data_types" / "mock.yml")
    tr_lib = _build_transform_lib(
        root / "transforms" / "mock", types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )

    inputs = DataInstanceLibrary(root / "templates" / "assembly_to_bam" / "inputs.xgdb")
    inputs.AddTypeLibrary(types_path)
    inputs.AddItem(DEFERRED, "mock::assembly")
    inputs.Save()

    return root, Spec(
        input_library=inputs,
        sample_type="mock::assembly",
        target_types=["mock::bam"],
        transform_libraries=[tr_lib],
    )


def test_a_saved_template_is_written_with_relative_references(tmp_path: Path):
    root, spec = _repo(tmp_path / "lib")
    out = Template(name="assembly_to_bam", spec=spec, description="mock").Save(root)

    packed = Template.Load(out).Pack()
    for ref in packed["transform_libraries"]:
        assert not Path(ref).is_absolute(), ref
    # input_library is inline data (see `PackInline`), not a directory
    # reference -- only its type namespace paths are references to check.
    for ref in packed["input_library"]["types"].values():
        assert not Path(ref).is_absolute(), ref
    assert packed["input_library"]["types"] == {"mock": "data_types/mock.yml"}
    assert packed["description"] == "mock"


def test_a_reference_outside_the_root_is_refused(tmp_path: Path):
    """The one way to save successfully and still be useless everywhere else."""
    root, spec = _repo(tmp_path / "lib")
    outside = _build_transform_lib(
        tmp_path / "elsewhere", _build_type_lib(tmp_path / "other.yml"),
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
    )
    spec.transform_libraries = [outside]
    with pytest.raises(AssertionError, match="outside"):
        Template(name="assembly_to_bam", spec=spec).Save(root)


def test_a_template_solves_after_the_repository_moves(tmp_path: Path):
    """Written in one checkout, read in another -- the whole point."""
    root, spec = _repo(tmp_path / "lib")
    Template(name="assembly_to_bam", spec=spec, description="mock").Save(root)

    moved = tmp_path / "somewhere_else"
    shutil.copytree(root, moved)
    shutil.rmtree(root)

    (template,) = Template.Discover(moved)
    assert template.name == "assembly_to_bam"
    task = template.spec.Solve()
    assert task.ok and task.plan.steps


def test_discover_finds_nothing_in_a_repository_without_templates(tmp_path: Path):
    (tmp_path / "empty").mkdir()
    assert Template.Discover(tmp_path / "empty") == []


@pytest.mark.slow
def test_every_shipped_template_still_solves(metasmith_libraries_root: Path):
    """The standard library's own templates, end to end.

    `./dev.sh -b` in that repository is the primary gate; this is the same
    assertion from the consuming side, so a library pull that breaks one is
    noticed here too rather than in someone's new-workflow modal. Marked slow
    because solving the metagenomics template alone is ~30s -- real transform
    libraries, real search.
    """
    templates = Template.Discover(metasmith_libraries_root)
    if not templates:
        pytest.skip(f"no templates in {metasmith_libraries_root}")
    for template in templates:
        task = template.spec.Solve()
        assert task.ok, (
            f"[{template.name}] dropped {sorted(task.plan.dropped_targets)}"
        )
        assert template.description, f"[{template.name}] has no description to show"


@pytest.mark.slow
def test_every_shipped_template_still_solves_once_the_gui_owns_it(
    metasmith_libraries_root: Path, tmp_path: Path,
):
    """The same templates, through the door a user actually walks through.

    `spec.Solve()` above reads the template's own library. The GUI does not: it
    materializes that library, adopts one editable row per item, and rebuilds
    the library *from those rows* on every solve. Two of the four shipped
    templates once solved above and dropped every target here, over a difference
    in how adoption read a value file. Whatever the round trip does to a row is
    only visible from this side, so the assertion worth holding is this one.
    """
    from metasmith.ops import data as op_data
    from metasmith.ops import inputs as op_inputs
    from metasmith.ops import samples as op_samples

    templates = Template.Discover(metasmith_libraries_root)
    if not templates:
        pytest.skip(f"no templates in {metasmith_libraries_root}")
    types = sorted(str(p) for p in (metasmith_libraries_root / "data_types").glob("*.yml"))
    for template in templates:
        lib_path = str(tmp_path / template.name / "input.xgdb")
        # POST /workflows
        op_data.materialize_template(
            template.spec.input_library, lib_path, type_library_paths=types,
        )
        adopted = op_inputs.adopt(lib_path, [])
        op_samples.write_record(lib_path, adopted["record"])
        # POST /workflows/<name>/generate -- no sheet, and the GUI always sends
        # a null sample type, so the library is planned as it stands
        op_inputs.sync(lib_path, adopted["rows"], None)
        spec = Spec.Unpack(
            template.spec.Pack(relative_to=metasmith_libraries_root),
            root=metasmith_libraries_root, input_library=lib_path,
        )
        spec.sample_type = None
        task = spec.Solve()
        assert task.ok, (
            f"[{template.name}] dropped {sorted(task.plan.dropped_targets)} "
            f"from a recipe of {len(adopted['rows'])} rows"
        )


def test_deriving_a_template_strips_paths_but_keeps_shape(tmp_path: Path):
    """`derive_template_library`: what "save as template" builds on.

    A live library with a two-item chain (a read pair sharing a parent) goes
    in; what comes out has the same item count, the same types, and the same
    parent structure, but not one of the original paths -- every one is a
    freshly minted deferred placeholder.
    """
    from metasmith.ops import data as op_data

    root = tmp_path / "lib"
    (root / "data_types").mkdir(parents=True)
    types_path = _build_type_lib(root / "data_types" / "mock.yml")

    live = DataInstanceLibrary(tmp_path / "workflow" / "input.xgdb")
    live.AddTypeLibrary(types_path)
    raw = live.AddItem(tmp_path / "raw.txt", "mock::assembly")
    (tmp_path / "raw.txt").write_text("x")
    derived_child = live.AddItem(tmp_path / "child.txt", "mock::bam", parents=[raw])
    (tmp_path / "child.txt").write_text("y")
    live.Save()

    tmpl_lib = op_data.derive_template_library(str(live.location), type_library_paths=[str(types_path)])

    assert len(tmpl_lib.manifest) == 2
    assert set(tmpl_lib.manifest.values()) == {"mock::assembly", "mock::bam"}
    assert not (set(tmpl_lib.manifest) & {raw, derived_child})

    [child_path] = [p for p, d in tmpl_lib.manifest.items() if d == "mock::bam"]
    assert [p.path for p in tmpl_lib.parents[child_path]] == [
        p for p, d in tmpl_lib.manifest.items() if d == "mock::assembly"
    ]


def test_the_same_template_reloads_to_the_same_task_key(tmp_path: Path):
    """A deferred path is minted once and persisted; identity follows it.

    If it were regenerated on load, the DAG a template draws would differ from
    the DAG the build asserted, every time.
    """
    root, spec = _repo(tmp_path / "lib")
    Template(name="assembly_to_bam", spec=spec).Save(root)
    (a,) = Template.Discover(root)
    (b,) = Template.Discover(root)
    assert a.spec.Solve().plan._key == b.spec.Solve().plan._key
