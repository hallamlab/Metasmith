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
    for ref in [packed["input_library"], *packed["transform_libraries"]]:
        assert not Path(ref).is_absolute(), ref
    assert packed["input_library"] == "templates/assembly_to_bam/inputs.xgdb"
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
