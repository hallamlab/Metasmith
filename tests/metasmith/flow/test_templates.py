from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from metasmith.agents import Spec, Template
from metasmith.agents.templates import library_index
from metasmith.models.libraries import DataInstanceLibrary
from metasmith.models.paths import DEFERRED
from metasmith.testing import mock_transforms as mt

from .conftest import _build_transform_lib, _build_type_lib


def _repo(root: Path) -> tuple[Path, Spec]:
    (root / "data_types").mkdir(parents=True, exist_ok=True)
    types_path = _build_type_lib(root / "data_types" / "mock.yml")
    tr_lib = _build_transform_lib(
        root / "transforms", types_path,
        dict(mt.identity_transform("mock::assembly", "mock::bam")),
        library_name="mock",
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


def test_a_saved_template_is_written_with_names_only(tmp_path: Path):
    import yaml

    root, spec = _repo(tmp_path / "lib")
    out = Template(name="assembly_to_bam", spec=spec, description="mock").Save(root)
    written = yaml.safe_load(out.read_text())

    assert written["transform_libraries"] == ["mock"]
    assert "types" not in written["input_library"]
    assert written["description"] == "mock"

    def _strings(node):
        if isinstance(node, dict):
            for k, v in node.items():
                yield from _strings(k)
                yield from _strings(v)
        elif isinstance(node, list):
            for v in node: yield from _strings(v)
        elif isinstance(node, str):
            yield node

    stray = [s for s in _strings(written) if Path(s).is_absolute() and str(root) in s]
    assert not stray, stray


def test_a_library_the_repository_does_not_have_is_reported_not_raised(tmp_path: Path):
    root, spec = _repo(tmp_path / "lib")
    out = Template(name="assembly_to_bam", spec=spec).Save(root)
    shutil.rmtree(root / "transforms" / "mock")

    template = Template.Load(out)
    assert template.unresolved == ["transform library [mock]"]


def test_a_template_solves_after_the_repository_moves(tmp_path: Path):
    root, spec = _repo(tmp_path / "lib")
    Template(name="assembly_to_bam", spec=spec, description="mock").Save(root)

    moved = tmp_path / "somewhere_else"
    shutil.copytree(root, moved)
    shutil.rmtree(root)

    (template,) = Template.Discover(moved)
    assert template.name == "assembly_to_bam"
    task = template.spec.Solve()
    assert task.ok and task.plan.steps


def test_a_template_resolves_against_a_repository_it_did_not_ship_in(tmp_path: Path):
    root, spec = _repo(tmp_path / "lib")
    elsewhere = tmp_path / "project"
    out = Template(name="assembly_to_bam", spec=spec).Save(
        elsewhere, dirname="user_templates",
    )

    assert Template.Load(out).unresolved == [
        "type namespace [mock]", "transform library [mock]",
    ]

    template = Template.Load(out, libraries=library_index(root))
    assert not template.unresolved
    task = template.spec.Solve()
    assert task.ok and task.plan.steps


def test_discover_finds_nothing_in_a_repository_without_templates(tmp_path: Path):
    (tmp_path / "empty").mkdir()
    assert Template.Discover(tmp_path / "empty") == []


@pytest.mark.slow
def test_every_shipped_template_still_solves(metasmith_libraries_root: Path):
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
    from metasmith.ops import data as op_data
    from metasmith.ops import inputs as op_inputs
    from metasmith.ops import samples as op_samples

    templates = Template.Discover(metasmith_libraries_root)
    if not templates:
        pytest.skip(f"no templates in {metasmith_libraries_root}")
    types = sorted(str(p) for p in (metasmith_libraries_root / "data_types").glob("*.yml"))
    for template in templates:
        lib_path = str(tmp_path / template.name / "input.xgdb")
        op_data.materialize_template(
            template.spec.input_library, lib_path, type_library_paths=types,
        )
        adopted = op_inputs.adopt(lib_path, [])
        op_samples.write_record(lib_path, adopted["record"])
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
    root, spec = _repo(tmp_path / "lib")
    Template(name="assembly_to_bam", spec=spec).Save(root)
    (a,) = Template.Discover(root)
    (b,) = Template.Discover(root)
    assert a.spec.Solve().plan._key == b.spec.Solve().plan._key
