"""Node-local control-plane staging must not desync lineage.

The SLURM array fan-out fix copies the small control-plane subset (a data
library's ``_metadata`` tree) into per-task node-local scratch and loads the
library from that copy instead of the shared ``/msm_home`` bind. This is only
safe because library and instance keys are *location-independent*:
``DataInstanceLibrary.GetKey`` hashes the raw ``index.yml`` content and
``DataInstance.instance_id`` derives from the relative path + dtype name +
library key — never from ``location``.

These tests lock that invariant in: a library loaded from a metadata-only copy
at a foreign root must produce byte-identical ``GetKey``, ``instance_id``,
``_key``, ``legacy_key`` and ``dtype.key`` for every item. If a future change
folds ``location`` into any of those keys, staging would silently point tasks
at a library the orchestrator's lineage no longer recognizes — and this fails.
"""

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
    """A small multi-sample library with lineage (meta -> reads -> asm)."""
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
            p.write_text("payload" * (i + 1))  # non-trivial, varied data files
        m = lib.AddItem(Path(f"s{i}/meta.json"), "mock::metadata")
        r = lib.AddItem(Path(f"s{i}/reads.fq"), "mock::reads", parents=[m])
        lib.AddItem(Path(f"s{i}/asm.fa"), "mock::assembly", parents=[r])
    lib.Save()
    return lib


def _stage_metadata_only(src_lib: Path, stage_root: Path) -> Path:
    """Mimic the host-side rsync: copy ONLY the _metadata tree to a new root.

    Reproduces the bootstrap staging block, which copies
    ``$AGENT_HOME/data/<lib>/_metadata`` and nothing else (the large data
    files are excluded). The returned path is the staged library root.
    """
    staged = stage_root / src_lib.name
    staged.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src_lib / "_metadata", staged / "_metadata")
    # Deliberately do NOT copy the data files — Load(check_integrity=False)
    # must not need them, exactly as on the compute node.
    return staged


def _instance_keys(lib: DataInstanceLibrary) -> dict:
    out = {}
    for path in sorted(lib.manifest, key=str):
        inst = lib.Get(path)  # keys populated in __post_init__ / RecalculateKey
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

    # Library-level content key is identical across roots.
    assert staged.GetKey() == original.GetKey()

    # Every per-instance key is byte-identical across roots.
    assert _instance_keys(staged) == _instance_keys(original)


def test_staged_copy_loads_without_data_files(temp_dir):
    """The staged library has no data files, only _metadata — Load must succeed."""
    src_lib = temp_dir / "src" / "samples.xgdb"
    src_lib.parent.mkdir(parents=True)
    _build_lib(src_lib)

    staged_path = _stage_metadata_only(src_lib, temp_dir / "stage" / "data")
    # No data files present under the staged root.
    assert not (staged_path / "s0" / "asm.fa").exists()

    staged = DataInstanceLibrary.Load(staged_path)  # check_integrity=False default
    assert len(staged.manifest) == 9  # 3 samples x 3 files


def test_stage_root_relocation_is_path_independent(temp_dir):
    """Copying the same metadata to two different roots yields identical keys."""
    src_lib = temp_dir / "src" / "samples.xgdb"
    src_lib.parent.mkdir(parents=True)
    _build_lib(src_lib)

    a = DataInstanceLibrary.Load(_stage_metadata_only(src_lib, temp_dir / "rootA" / "data"))
    b = DataInstanceLibrary.Load(_stage_metadata_only(src_lib, temp_dir / "rootB" / "data"))

    assert a.GetKey() == b.GetKey()
    assert _instance_keys(a) == _instance_keys(b)
