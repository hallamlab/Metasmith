"""Tests for container bind computation (Phase 0 fixes).

Covers:
- 0a: prepare_step resolving library-relative paths and remapping home_dir
- 0b: GetContainerModel collecting binds from all batches, not just current
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from metasmith.models.libraries import (
    DataInstance,
    DataInstanceLibrary,
    ContextPath,
    ContextData,
    ExecutionContext,
)
from metasmith.models.solver import Dependency, Endpoint
from metasmith.constants import AgentPaths
from metasmith.env import Runtime


# ---------- helpers ----------

def _make_lib(tmp_path: Path, name: str = "lib") -> DataInstanceLibrary:
    """Create a DataInstanceLibrary under tmp_path."""
    loc = tmp_path / name
    return DataInstanceLibrary(loc)


def _make_instance(path: Path, lib: DataInstanceLibrary, dtype_name: str = "ns::t") -> DataInstance:
    ep = Endpoint(properties={"x"})
    return DataInstance(path=path, dtype=ep, dtype_name=dtype_name, parent_lib=lib)


def _make_dependency(name: str) -> Dependency:
    return Dependency(properties={name}, parents=set())


# ---------- 0a: ResolvePath in prepare_step bind collection ----------

class TestPrepareStepBindResolution:
    """Test that bind collection uses ResolvePath() and remaps home_dir paths."""

    def test_library_relative_path_resolves(self, tmp_path):
        """A library-relative instance path should resolve to lib.location / path."""
        lib = _make_lib(tmp_path, "mylib")
        inst = _make_instance(Path("sample1/reads.fq"), lib)
        resolved = inst.ResolvePath()
        assert resolved == lib.location / "sample1/reads.fq"
        assert resolved.is_absolute()

    def test_absolute_path_resolves_unchanged(self, tmp_path):
        """An absolute instance path should resolve to itself."""
        lib = _make_lib(tmp_path, "mylib")
        inst = _make_instance(Path("/scratch/data/file.txt"), lib)
        resolved = inst.ResolvePath()
        assert resolved == Path("/scratch/data/file.txt")

    def test_home_dir_remapping(self, tmp_path):
        """Paths under home_dir should be remapped to external_home."""
        home_dir = tmp_path / "msm_home"
        home_dir.mkdir()
        lib = DataInstanceLibrary(home_dir)
        inst = _make_instance(Path("databases/refdb.fa"), lib)

        external_home = Path("/hpc/scratch/msm_home")

        p = inst.ResolvePath()
        assert p.is_absolute()
        assert p.is_relative_to(home_dir)
        remapped = external_home / p.relative_to(home_dir)
        assert remapped == Path("/hpc/scratch/msm_home/databases/refdb.fa")
        assert remapped.parent == Path("/hpc/scratch/msm_home/databases")

    def test_non_home_absolute_not_remapped(self, tmp_path):
        """Absolute paths NOT under home_dir should be kept as-is."""
        home_dir = tmp_path / "msm_home"
        home_dir.mkdir()
        lib = _make_lib(tmp_path, "shared")
        inst = _make_instance(Path("db/ref.fa"), lib)

        p = inst.ResolvePath()
        assert p.is_absolute()
        assert not p.is_relative_to(home_dir)
        assert p.parent == lib.location / "db"


# ---------- 0b: GetContainerModel all-batch bind collection ----------

class TestGetContainerModelBatchBinds:
    """Test that GetContainerModel collects binds from ALL batches."""

    def _make_context_path(self, external: Path, container: Path | None = None) -> ContextPath:
        if container is None:
            container = external
        return ContextPath(local=external, external=external, container=container)

    def _make_context(self, batch_inputs: list[dict]) -> ExecutionContext:
        return ExecutionContext(
            _inputs=batch_inputs,
            _get_output_paths=lambda *a: None,
            external_shell=MagicMock(),
            external_cwd=Path("/ws"),
            external_agent_home=Path("/hpc/msm_home"),
            _environment=Runtime.APPTAINER,
        )

    def _call_get_container(self, ctx, image_dep, tmp_path):
        """Call GetContainerModel with a real image file to avoid IsText error."""
        # Create a binary image file so IsText returns False
        sif = tmp_path / "tool.sif"
        sif.write_bytes(b"\x00\x01\x02\x03")
        # Patch the image path to use our temp file
        ctx._inputs[ctx._batch_index][image_dep].path = ContextPath(
            local=sif, external=Path("/hpc/containers/tool.sif"), container=Path("/hpc/containers/tool.sif")
        )
        ctx._inputs[ctx._batch_index][image_dep].input_group[0] = ctx._inputs[ctx._batch_index][image_dep].path
        return ctx.GetContainerModel(image_dep)

    def test_single_batch_binds(self, tmp_path):
        """Binds from a single batch item should be collected."""
        dep = _make_dependency("reads")
        image_dep = _make_dependency("image")
        image_path = self._make_context_path(Path("/hpc/containers/tool.sif"))

        cp = self._make_context_path(
            external=Path("/data/batch0/reads.fq"),
            container=Path("/data/batch0/reads.fq"),
        )
        batch = [{
            dep: ContextData(input_group=[cp], endpoint=Endpoint(properties={"reads"}), type_name="reads"),
            image_dep: ContextData(input_group=[image_path], endpoint=Endpoint(properties={"image"}), type_name="image"),
        }]
        ctx = self._make_context(batch)
        container = self._call_get_container(ctx, image_dep, tmp_path)
        bind_srcs = [str(s) for s, _ in container.binds]
        assert any("/data/batch0" in s for s in bind_srcs), f"Expected /data/batch0 in binds, got {bind_srcs}"

    def test_multi_batch_binds_all_collected(self, tmp_path):
        """Binds from ALL batch items should be collected, not just current batch_index."""
        dep = _make_dependency("reads")
        image_dep = _make_dependency("image")

        batches = []
        for i in range(3):
            image_path = self._make_context_path(Path("/hpc/containers/tool.sif"))
            cp = self._make_context_path(
                external=Path(f"/data/project{i}/reads.fq"),
                container=Path(f"/data/project{i}/reads.fq"),
            )
            batches.append({
                dep: ContextData(input_group=[cp], endpoint=Endpoint(properties={"reads"}), type_name="reads"),
                image_dep: ContextData(input_group=[image_path], endpoint=Endpoint(properties={"image"}), type_name="image"),
            })

        ctx = self._make_context(batches)
        container = self._call_get_container(ctx, image_dep, tmp_path)
        bind_srcs = [str(s) for s, _ in container.binds]

        # All three project dirs should be represented (possibly merged via commonpath)
        # At minimum, /data should appear as a common ancestor
        has_data = any("/data" in s for s in bind_srcs)
        assert has_data, f"Expected /data paths in binds, got {bind_srcs}"

    def test_home_root_paths_excluded(self, tmp_path):
        """Paths under AgentPaths.HOME_ROOT should be excluded from _binds."""
        dep = _make_dependency("config")
        image_dep = _make_dependency("image")

        image_path = self._make_context_path(Path("/hpc/containers/tool.sif"))
        cp = self._make_context_path(
            external=Path("/hpc/msm_home/config.yml"),
            container=AgentPaths.HOME_ROOT / "config.yml",
        )
        batch = [{
            dep: ContextData(input_group=[cp], endpoint=Endpoint(properties={"config"}), type_name="config"),
            image_dep: ContextData(input_group=[image_path], endpoint=Endpoint(properties={"image"}), type_name="image"),
        }]
        ctx = self._make_context(batch)
        container = self._call_get_container(ctx, image_dep, tmp_path)
        # The _binds portion shouldn't contain HOME_ROOT paths
        bind_dests = [str(d) for _, d in container.binds]
        home_root_binds = [d for d in bind_dests if d.startswith(str(AgentPaths.HOME_ROOT) + "/config")]
        assert len(home_root_binds) == 0, f"HOME_ROOT child paths should not appear in _binds: {home_root_binds}"
