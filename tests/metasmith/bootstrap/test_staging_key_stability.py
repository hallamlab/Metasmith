import shutil
import tempfile
from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.solver import Endpoint


@pytest.fixture
def temp_dir():
    d = tempfile.mkdtemp()
    yield Path(d)
    shutil.rmtree(d)


def _build_lib(lib_path: Path) -> DataInstanceLibrary:
    types = DataTypeLibrary()
    types["metadata"] = Endpoint(properties={"metadata"})
    types["reads"] = Endpoint(properties={"reads"})
    types["assembly"] = Endpoint(properties={"assembly"})
    types_path = lib_path.parent / "types.yml"
    types.Save(types_path)

    lib = DataInstanceLibrary(lib_path)
    lib.AddTypeLibrary(types_path, namespace="mock")
    for i in range(3):
        for f in ["meta.json", "reads.fq", "asm.fa"]:
            p = lib_path / f"s{i}" / f
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("payload" * (i + 1))
        m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
        r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])
    lib.Save()
    return lib


def _stage_metadata_only(src_lib: Path, stage_root: Path) -> Path:
    staged = stage_root / src_lib.name
    staged.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_lib / "_metadata", staged / "_metadata")
    return staged


def _instance_keys(lib: DataInstanceLibrary) -> dict:
    out = {}
    for path in sorted(lib.manifest, key=str):
        inst = lib.Get(path)
        out[str(path)] = (
            inst.instance_id,
            inst._key,
            inst.legacy_key,
            inst.dtype.key,
        )
    return out


def test_staged_metadata_only_copy_preserves_all_keys(temp_dir):
    src_lib = temp_dir / "src" / "samples.xgdb"
    src_lib.parent.mkdir(parents=True)
    _build_lib(src_lib)

    original = DataInstanceLibrary.Load(src_lib)
    staged_path = _stage_metadata_only(src_lib, temp_dir / "node_local_stage" / "data")
    staged = DataInstanceLibrary.Load(staged_path)

    assert staged.GetKey() == original.GetKey()

    assert _instance_keys(staged) == _instance_keys(original)


def test_staged_copy_loads_without_data_files(temp_dir):
    src_lib = temp_dir / "src" / "samples.xgdb"
    src_lib.parent.mkdir(parents=True)
    _build_lib(src_lib)

    staged_path = _stage_metadata_only(src_lib, temp_dir / "stage" / "data")
    assert not (staged_path / "s0" / "asm.fa").exists()

    staged = DataInstanceLibrary.Load(staged_path)
    assert len(staged.manifest) == 9


def test_stage_root_relocation_is_path_independent(temp_dir):
    src_lib = temp_dir / "src" / "samples.xgdb"
    src_lib.parent.mkdir(parents=True)
    _build_lib(src_lib)

    a = DataInstanceLibrary.Load(_stage_metadata_only(src_lib, temp_dir / "rootA" / "data"))
    b = DataInstanceLibrary.Load(_stage_metadata_only(src_lib, temp_dir / "rootB" / "data"))

    assert a.GetKey() == b.GetKey()
    assert _instance_keys(a) == _instance_keys(b)
