from __future__ import annotations

import os
from pathlib import Path

import pytest

from metasmith.models.build_libraries import CompileTransformLibrary
from metasmith.models.libraries import (
    DataInstanceLibrary, DataTypeLibrary, TransformInstanceLibrary,
)
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint


_TRANSFORM = '''\
from pathlib import Path

from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
HELPER = Path(__file__).parent/"_helper.py"

model = Transform()
inp = model.AddRequirement(lib.GetType("{ns_in}::{t_in}"))
out = model.AddProduct(lib.GetType("{ns_out}::{t_out}"))

def protocol(context: ExecutionContext):
    return ExecutionResult(manifest=[{{out: context.Output(out).local}}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=inp)
'''


@pytest.fixture
def stdlib(tmp_path) -> tuple[TransformInstanceLibrary, Path]:
    types = {}
    for namespace, names in {
        "seq": ["reads", "assembly", "orfs"],
        "ann": ["hits"],
        "unused": ["nobody_asks"],
    }.items():
        lib = DataTypeLibrary()
        for n in names:
            lib[n] = Endpoint(properties={namespace, n})
        types[namespace] = lib

    lib_dir = tmp_path/"transforms"
    lib_dir.mkdir()
    for name, (ns_in, t_in, ns_out, t_out) in {
        "assemble":  ("seq", "reads", "seq", "assembly"),
        "call_orfs": ("seq", "assembly", "seq", "orfs"),
        "annotate":  ("seq", "orfs", "ann", "hits"),
    }.items():
        (lib_dir/f"{name}.py").write_text(_TRANSFORM.format(
            ns_in=ns_in, t_in=t_in, ns_out=ns_out, t_out=t_out))
    # Not a manifest entry -- `_file_ok` skips it -- but `assemble.py` names it.
    (lib_dir/"_helper.py").write_text("SENTINEL = 1\n")
    CompileTransformLibrary(lib_dir, types)
    return TransformInstanceLibrary.Load(lib_dir), lib_dir


def _image(lib, mask: set[Path], dest: Path) -> Path:
    lib.AsView(mask).PrepTransfer(Source.FromLocal(dest), image_root=dest)
    return dest


def test_image_holds_the_masked_entries_and_no_others(tmp_path, stdlib):
    lib, _ = stdlib
    image = _image(lib, {Path("assemble.py")}, tmp_path/"image")

    assert (image/"assemble.py").is_file()
    assert not (image/"call_orfs.py").exists()
    assert not (image/"annotate.py").exists()


def test_a_non_manifest_sibling_rides_along(tmp_path, stdlib):
    lib, lib_dir = stdlib
    assert Path("_helper.py") not in lib.manifest
    image = _image(lib, {Path("assemble.py")}, tmp_path/"image")

    # `assemble.py` copies it out by Path(__file__).parent, so selecting from
    # the manifest would drop a real runtime dependency. The image subtracts.
    assert (image/"_helper.py").read_text() == "SENTINEL = 1\n"


def test_the_image_keys_the_same_as_the_library(tmp_path, stdlib):
    lib, _ = stdlib
    image = _image(lib, {Path("annotate.py")}, tmp_path/"image")

    reloaded = TransformInstanceLibrary.Load(image)
    assert reloaded.GetKey() == lib.GetKey()
    assert reloaded.manifest == lib.manifest
    assert {p: reloaded.Get(p).instance_id for p in reloaded.manifest} \
        == {p: lib.Get(p).instance_id for p in lib.manifest}


def test_the_image_carries_only_the_masked_transforms_namespaces(tmp_path, stdlib):
    lib, _ = stdlib
    # `unused` went at build time: CompileTransformLibrary prunes to the union
    # over every transform in the library.
    assert set(lib.types) == {"seq", "ann", "transforms"}

    image = _image(lib, {Path("assemble.py")}, tmp_path/"image")

    # `assemble` is seq->seq, so `ann` goes too; `transforms` stays because
    # every manifest entry is typed by it and the manifest is never narrowed.
    assert set(TransformInstanceLibrary.Load(image).types) == {"seq", "transforms"}


def test_an_empty_mask_still_produces_a_loadable_library(tmp_path, stdlib):
    lib, _ = stdlib
    image = _image(lib, set(), tmp_path/"image")

    reloaded = TransformInstanceLibrary.Load(image)
    assert reloaded.GetKey() == lib.GetKey()
    assert not any(image.glob("*.py")) or {p.name for p in image.glob("*.py")} == {"_helper.py"}


def test_pycache_never_ships(tmp_path, stdlib):
    lib, lib_dir = stdlib
    (lib_dir/"__pycache__").mkdir(exist_ok=True)
    (lib_dir/"__pycache__"/"assemble.cpython-312.pyc").write_bytes(b"\x00")

    image = _image(lib, {Path("assemble.py")}, tmp_path/"image")
    assert not (image/"__pycache__").exists()


def _data_lib(root: Path, types_path: Path) -> DataInstanceLibrary:
    types = DataTypeLibrary()
    for n in ["reads", "notes", "bundle"]:
        types[n] = Endpoint(properties={"mock", n})
    types.Save(types_path)

    lib = DataInstanceLibrary(root)
    lib.AddTypeLibrary(types_path, namespace="mock")
    (root/"reads.fq").write_text("payload")
    (root/"notes.txt").write_text("unused")
    (root/"bundle").mkdir()
    (root/"bundle"/"a.txt").write_text("a")
    (root/"bundle"/"nested").mkdir()
    (root/"bundle"/"nested"/"b.txt").write_text("b")
    for name in ["reads.fq", "notes.txt", "bundle"]:
        lib.AddItem(Path(name), f"mock::{name.split('.')[0]}")
    lib.Save()
    return lib


def test_a_directory_entry_is_reproduced_whole_or_not_at_all(tmp_path):
    lib = _data_lib(tmp_path/"lib", tmp_path/"mock.yml")

    kept = _image(lib, {Path("bundle")}, tmp_path/"kept")
    assert (kept/"bundle"/"nested"/"b.txt").read_text() == "b"
    assert not (kept/"reads.fq").exists()

    dropped = _image(lib, {Path("reads.fq")}, tmp_path/"dropped")
    assert not (dropped/"bundle").exists()


def test_a_symlink_arrives_as_a_symlink(tmp_path):
    outside = tmp_path/"outside.fq"
    outside.write_text("elsewhere")
    lib = _data_lib(tmp_path/"lib", tmp_path/"mock.yml")
    (lib.location/"linked.fq").symlink_to(outside)
    lib.AddItem(Path("linked.fq"), "mock::reads")
    lib.Save()

    image = _image(lib, {Path("linked.fq")}, tmp_path/"image")

    # Staging runs resolve_symlinks=False; resolving here would send the bytes.
    assert (image/"linked.fq").is_symlink()
    assert os.readlink(image/"linked.fq") == str(outside)


def test_an_unmasked_library_still_ships_its_whole_directory(tmp_path, stdlib):
    lib, _ = stdlib
    mover = lib.PrepTransfer(Source.FromLocal(tmp_path/"dest"))

    assert [str(src.address) for src, _dest in mover._queue] == [str(lib.location)]


def _plan_task(tmp_path, stdlib):
    from metasmith.agents.spec import Spec
    from metasmith.agents.targets import TargetBuilder

    lib, _ = stdlib
    samples = _data_lib(tmp_path/"samples", tmp_path/"mock.yml")
    # A sample library the plan resolves one entry of, alongside the two it
    # does not: `notes.txt` and `bundle` are what pruning is for.
    seq_types = DataTypeLibrary.Load(lib.location/lib._path_to_types/"seq.yml")
    samples.AddTypeLibrary(seq_types, namespace="seq")
    (samples.location/"in.fq").write_text("reads")
    samples.AddItem(Path("in.fq"), "seq::reads")
    samples.Save()

    targets = TargetBuilder()
    targets.Add("ann::hits")
    return Spec.SolveViews(
        samples=[samples], resources=[], transforms=[lib], targets=targets)


def test_a_pruned_bundle_reloads_to_the_same_plan(tmp_path, stdlib):
    task = _plan_task(tmp_path, stdlib)
    assert task.ok, task.plan.hints

    from metasmith.models.workflow import WorkflowTask

    bundles = {}
    for name, prune in (("full", False), ("pruned", True)):
        dest = tmp_path/name
        task.SaveAs(Source.FromLocal(dest), prune=prune)
        bundles[name] = WorkflowTask.Load(dest)

    full, pruned = bundles["full"], bundles["pruned"]
    assert pruned.GetKey() == full.GetKey()
    assert pruned.Pack() == full.Pack()
    assert pruned.plan.Pack() == full.plan.Pack()
    assert [s.transform.name for s in pruned.plan.steps] \
        == [s.transform.name for s in full.plan.steps]

    n_full = sum(1 for p in (tmp_path/"full").rglob("*") if p.is_file())
    n_pruned = sum(1 for p in (tmp_path/"pruned").rglob("*") if p.is_file())
    assert n_pruned < n_full


def test_pruning_ships_the_used_sample_entry_and_not_the_others(tmp_path, stdlib):
    task = _plan_task(tmp_path, stdlib)
    task.SaveAs(Source.FromLocal(tmp_path/"pruned"), prune=True)

    staged = {
        p.name for p in (tmp_path/"pruned"/"data").rglob("*")
        if p.is_file() and "_metadata" not in p.parts
    }
    assert "in.fq" in staged
    assert "notes.txt" not in staged
    assert "b.txt" not in staged


def test_a_pinned_library_images_without_rewriting_its_index(tmp_path):
    lib = _data_lib(tmp_path/"lib", tmp_path/"mock.yml")
    lib.Pin()
    assert lib.is_pinned

    image = _image(lib, {Path("reads.fq"), Path("bundle")}, tmp_path/"image")

    # PruneTypes refuses on a pinned library, and the stamp is a witness about
    # the library the image came from -- so the image is copied, not re-derived.
    reloaded = DataInstanceLibrary.Load(image)
    assert reloaded.is_pinned
    assert reloaded.GetKey() == lib.GetKey()
    assert not (image/"notes.txt").exists()


def test_a_directory_entry_keeps_its_mtime(tmp_path):
    lib = _data_lib(tmp_path/"lib", tmp_path/"mock.yml")
    image = _image(lib, {Path("bundle")}, tmp_path/"image")

    # A pinned directory entry's witness is its mtime, and writing a child bumps
    # the parent -- so the image restores it once the children are in place.
    assert (image/"bundle").stat().st_mtime_ns \
        == (lib.location/"bundle").stat().st_mtime_ns
