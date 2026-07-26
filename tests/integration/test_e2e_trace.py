"""Integration tests: Trace on workflow results via real Nextflow execution.

Tests verify the full pipeline works end-to-end:
1. Orchestrator.groovy correctly maintains lineage through Nextflow execution
2. CollectResults correctly builds a DataInstanceLibrary with parent relationships
3. Trace() works correctly on the resulting library

Tests use Nextflow stub mode (processes create empty output files).
"""

import json
import shutil
import subprocess
import sys
import pytest
from pathlib import Path

from metasmith.agents import CollectResults
from metasmith.constants import MODULE_PATH, AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import DataInstanceLibrary, DataInstanceLibraryView, DataTypeLibrary
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import WorkflowPlan, WorkflowTask, NextflowGenContext
from metasmith.testing.mock_transforms import (
    alignment_transform,
    binner_transforms,
    branching_transforms,
    identity_transform,
    shared_input_transform,
)

from .conftest import create_transform_library

pytestmark = [pytest.mark.docker, pytest.mark.slow]

ORCHESTRATOR_SRC = MODULE_PATH / "nextflow_config/Orchestrator.groovy"


def _assert_nxf_ok(result: subprocess.CompletedProcess) -> bool:
    """Tolerate upstream nextflow-io/nextflow#6757 (negative Duration in
    invokeOnComplete). Returns True iff the run produced normal exit; False
    iff it exited non-zero solely due to the upstream Duration assertion —
    in that case manifests are still on disk and downstream parsing should
    proceed. Raises AssertionError on any other failure mode.
    """
    nxf_duration_bug = (
        "Duration unit cannot be a negative number" in result.stdout
        or "Duration unit cannot be a negative number" in (result.stderr or "")
    )
    if result.returncode == 0:
        return True
    if nxf_duration_bug:
        print(
            "WARN: tolerated upstream nextflow-io/nextflow#6757 (negative Duration "
            "assertion); workflow body completed, optional report/timeline/trace "
            "artifacts may be missing.",
            file=sys.stderr,
        )
        return False
    raise AssertionError(
        f"Nextflow stub run failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )


def run_stub_workflow(
    task: WorkflowTask,
    work_dir: Path,
    docker_image: str,
    timeout: int = 180,
) -> DataInstanceLibrary:
    """Run a stub workflow and collect results.

    1. Calls PrepareNextflow to generate workflow.nf, inputs/, lineage JSON
    2. Copies Orchestrator.groovy into lib/
    3. Runs nextflow in stub mode inside Docker
    4. Calls CollectResults on the output
    5. Saves, loads (triggers transitive closure), returns loaded library
    """
    work_dir.mkdir(parents=True, exist_ok=True)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=work_dir,
        home_dir=work_dir,
        external_home=work_dir,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    task.PrepareNextflow(context)

    # Copy Orchestrator.groovy into lib/
    lib_dir = work_dir / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(ORCHESTRATOR_SRC, lib_dir / "Orchestrator.groovy")

    # Run nextflow in stub mode inside Docker.
    # Mount the tmp root at the same path so container paths == host paths.
    # This ensures both work_dir and input data paths are accessible.
    tmp_root = work_dir
    while tmp_root.parent != tmp_root and tmp_root.parent != Path("/tmp"):
        tmp_root = tmp_root.parent
    # tmp_root is now /tmp/pytest-of-XXX or similar
    result = subprocess.run(
        [
            "docker", "run", "--rm",
            "-v", f"{tmp_root}:{tmp_root}",
            "-w", str(work_dir),
            docker_image,
            "nextflow", "run", AgentPaths.NXF_WORKFLOW,
            "-stub",
            "-lib", "./lib",
            "-ansi-log", "false",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    nxf_clean_exit = _assert_nxf_ok(result)

    # Fix file ownership (Docker runs as root)
    subprocess.run(
        ["docker", "run", "--rm",
         "-v", f"{tmp_root}:{tmp_root}",
         docker_image,
         "chmod", "-R", "a+rw", str(work_dir)],
        capture_output=True, timeout=120,
    )

    # Collect results. Manifests are emitted by `publish` channels BEFORE
    # Nextflow's invokeOnComplete fires, so they survive the upstream
    # Duration assertion. If parsing fails after a tolerated Duration bug,
    # surface that as the likely culprit instead of a generic error.
    output_path = work_dir / "results"
    inputs_dir = work_dir / "inputs"
    manifests_path = output_path / "_manifests"
    manifests_path.mkdir(parents=True, exist_ok=True)

    try:
        output = CollectResults(
            task=task,
            output_path=output_path,
            inputs_dir=inputs_dir,
            manifests_path=manifests_path,
        )
        # Save and reload to trigger transitive closure
        output.Save()
        loaded = DataInstanceLibrary.Load(output.location)
    except Exception as e:
        if not nxf_clean_exit:
            raise AssertionError(
                f"CollectResults failed after a tolerated upstream Duration "
                f"assertion (nextflow-io/nextflow#6757). Manifests should be "
                f"on disk before invokeOnComplete fires — if they are not, "
                f"the workflow body itself failed.\nUnderlying error: {e!r}\n"
                f"STDOUT:\n{result.stdout[-2000:]}\n"
                f"STDERR:\n{(result.stderr or '')[-2000:]}"
            ) from e
        raise
    return loaded


def _make_samples(
    temp_dir, mock_types, n_samples=3,
    types: list[tuple[str, str]] | None = None,
) -> DataInstanceLibrary:
    """Create n samples with configurable lineage.

    Args:
        temp_dir: Directory for the library.
        mock_types: Path to mock types YAML.
        n_samples: Number of samples.
        types: List of (type_name, file_ext) tuples defining the lineage chain.
            Each type's parent is the previous type in the list.
            Defaults to sample_metadata -> reads -> assembly.
    """
    if types is None:
        types = [
            ("mock::sample_metadata", "json"),
            ("mock::reads", "fq"),
            ("mock::assembly", "fa"),
        ]

    lib_path = temp_dir / "samples.xgdb"
    lib = DataInstanceLibrary(lib_path)
    lib.AddTypeLibrary(mock_types, namespace="mock")

    for i in range(n_samples):
        sample_id = f"sample_{i:02d}"
        sample_dir = lib.location / sample_id
        sample_dir.mkdir(parents=True, exist_ok=True)

        prev_path = None
        for type_name, ext in types:
            short_name = type_name.split("::")[-1]
            fname = f"{short_name}.{ext}"
            (sample_dir / fname).write_text(f"mock {short_name} {i}")
            parents = [prev_path] if prev_path is not None else []
            prev_path = lib.AddItem(
                Path(f"{sample_id}/{fname}"), type_name, parents=parents
            )

    lib.Save()
    return lib


def _make_task(
    samples: DataInstanceLibrary,
    mock_types: Path,
    temp_dir: Path,
    transforms: dict[str, str],
    target_properties: list[set[str]],
    target_names: list[str],
    given_type: str = "mock::assembly",
) -> WorkflowTask:
    """Build a WorkflowTask from transforms and samples."""
    tr_lib = create_transform_library(temp_dir / "transforms", mock_types, transforms)

    given = [[sv] for sv in samples.AsSamples(given_type)]
    target_model = Transform()
    for props in target_properties:
        target_model.AddRequirement(properties=props)

    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[tr_lib],
        target_names=target_names,
        target_model=target_model,
    )

    assert isinstance(plan, WorkflowPlan)
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[samples],
        transform_libraries=[tr_lib],
    )


# ---------------------------------------------------------------------------
# TestTraceLinearChain
# ---------------------------------------------------------------------------


class TestTraceLinearChain:
    """Single transform, 1 output per input.

    Topology: reads + assembly -> bam (alignment).
    Given lineage: sample_metadata -> reads -> assembly.
    """

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = _make_samples(tmp_path / "data", mock_types, n_samples=3)
        transforms = alignment_transform()
        task = _make_task(
            samples=samples,
            mock_types=mock_types,
            temp_dir=tmp_path / "task",
            transforms=transforms,
            target_properties=[{"bam"}],
            target_names=["bam"],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    def test_output_to_immediate_input(self, result_lib):
        """Trace output->input (direct parent) yields 3 pairs."""
        pairs = list(result_lib.Trace("mock::bam", "mock::assembly"))
        assert len(pairs) == 3

    def test_output_to_transitive_ancestor(self, result_lib):
        """Trace output->root ancestor yields 3 pairs."""
        pairs = list(result_lib.Trace("mock::bam", "mock::reads"))
        assert len(pairs) == 3

    def test_reverse_input_to_output(self, result_lib):
        """Trace input->output (descendant) yields 3 pairs."""
        pairs = list(result_lib.Trace("mock::assembly", "mock::bam"))
        assert len(pairs) == 3

    def test_no_cross_sample_contamination(self, result_lib):
        """Each output traces only to its own sample's inputs."""
        for bam_inst, asm_inst in result_lib.Trace("mock::bam", "mock::assembly"):
            # Extract sample ID from path
            bam_sample = str(bam_inst.path).split("/")[0] if "/" in str(bam_inst.path) else None
            asm_sample = str(asm_inst.path).split("/")[0] if "/" in str(asm_inst.path) else None
            # Both should belong to the same sample
            if bam_sample and asm_sample:
                assert bam_sample == asm_sample, (
                    f"Cross-sample contamination: bam={bam_inst.path} traces to asm={asm_inst.path}"
                )


# ---------------------------------------------------------------------------
# TestTraceFanOutMerge
# ---------------------------------------------------------------------------


class TestTraceFanOutMerge:
    """Branching into parallel paths then merging.

    Topology: assembly -> {branch_a, branch_b}, then branch_a + branch_b -> merged.

    With the default WorkflowPlan.publish_intermediates=True, every produced
    instance (including branch_a and branch_b) is published to the result
    library alongside the final merged target.
    """

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = _make_samples(
            tmp_path / "data", mock_types, n_samples=3,
            types=[("mock::assembly", "fa")],
        )
        transforms = branching_transforms()
        task = _make_task(
            samples=samples,
            mock_types=mock_types,
            temp_dir=tmp_path / "task",
            transforms=transforms,
            target_properties=[{"merged"}],
            target_names=["merged"],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    def test_merged_output_exists(self, result_lib):
        """Merged outputs are present in the result library."""
        merged_items = [
            p for p, n in result_lib.manifest.items()
            if n == "mock::merged"
        ]
        assert len(merged_items) == 3

    def test_merged_has_assembly_ancestor(self, result_lib):
        """Each merged output has at least one assembly ancestor."""
        pairs = list(result_lib.Trace("mock::merged", "mock::assembly"))
        assert len(pairs) >= 3

    def test_assembly_traces_to_merged(self, result_lib):
        """Reverse trace: assembly->merged (descendant direction)."""
        pairs = list(result_lib.Trace("mock::assembly", "mock::merged"))
        assert len(pairs) >= 3

    def test_intermediates_published_in_output(self, result_lib):
        """Intermediate branch types are persisted alongside the merged target."""
        type_names = set(result_lib.manifest.values())
        assert "mock::branch_a" in type_names
        assert "mock::branch_b" in type_names


# ---------------------------------------------------------------------------
# TestTraceMultiStepDiamond
# ---------------------------------------------------------------------------


class TestTraceMultiStepDiamond:
    """Multi-step chain with fan-out at the end.

    Topology: reads + assembly -> bam (alignment), assembly + bam -> {metabat2, maxbin2, concoct}_bins.
    Given lineage: sample_metadata -> reads -> assembly.
    """

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = _make_samples(tmp_path / "data", mock_types, n_samples=3)
        transforms = alignment_transform() | binner_transforms()
        task = _make_task(
            samples=samples,
            mock_types=mock_types,
            temp_dir=tmp_path / "task",
            transforms=transforms,
            target_properties=[
                {"bins", "method:metabat2"},
                {"bins", "method:maxbin2"},
                {"bins", "method:concoct"},
            ],
            target_names=["metabat2_bins", "maxbin2_bins", "concoct_bins"],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    def test_final_output_to_root(self, result_lib):
        """Trace bins->reads (deep transitive) yields 3 pairs per output type."""
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::reads"))
            assert len(pairs) == 3, f"{bin_type}->reads: expected 3, got {len(pairs)}"

    def test_final_output_to_given_input(self, result_lib):
        """Trace bins->assembly (direct given parent) yields 3 pairs per output type."""
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::assembly"))
            assert len(pairs) == 3, f"{bin_type}->assembly: expected 3, got {len(pairs)}"

    def test_different_bin_types_same_count(self, result_lib):
        """All 3 binner outputs produce the same number of results."""
        counts = {}
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::assembly"))
            counts[bin_type] = len(pairs)
        assert all(c == 3 for c in counts.values()), f"Uneven counts: {counts}"

    def test_no_cross_sample_contamination_multi_output(self, result_lib):
        """All output types correctly paired per sample."""
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            for bin_inst, asm_inst in result_lib.Trace(bin_type, "mock::assembly"):
                bin_sample = str(bin_inst.path).split("/")[0] if "/" in str(bin_inst.path) else None
                asm_sample = str(asm_inst.path).split("/")[0] if "/" in str(asm_inst.path) else None
                if bin_sample and asm_sample:
                    assert bin_sample == asm_sample, (
                        f"Cross-sample: {bin_type} {bin_inst.path} -> asm {asm_inst.path}"
                    )


# ---------------------------------------------------------------------------
# TestTraceScaling
# ---------------------------------------------------------------------------


class TestTraceScaling:
    """Many samples with linear chain."""

    N_SAMPLES = 8

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = _make_samples(tmp_path / "data", mock_types, n_samples=self.N_SAMPLES)
        transforms = alignment_transform()
        task = _make_task(
            samples=samples,
            mock_types=mock_types,
            temp_dir=tmp_path / "task",
            transforms=transforms,
            target_properties=[{"bam"}],
            target_names=["bam"],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    def test_many_samples_correct_count(self, result_lib):
        """All N pairs returned."""
        pairs = list(result_lib.Trace("mock::bam", "mock::assembly"))
        assert len(pairs) == self.N_SAMPLES

    def test_many_samples_correct_pairing(self, result_lib):
        """Every pair correctly matched."""
        for bam_inst, asm_inst in result_lib.Trace("mock::bam", "mock::assembly"):
            bam_sample = str(bam_inst.path).split("/")[0] if "/" in str(bam_inst.path) else None
            asm_sample = str(asm_inst.path).split("/")[0] if "/" in str(asm_inst.path) else None
            if bam_sample and asm_sample:
                assert bam_sample == asm_sample


# ---------------------------------------------------------------------------
# TestTracePersistence
# ---------------------------------------------------------------------------


class TestTracePersistence:
    """Save/load round-trip on real results."""

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = _make_samples(tmp_path / "data", mock_types, n_samples=3)
        transforms = alignment_transform()
        task = _make_task(
            samples=samples,
            mock_types=mock_types,
            temp_dir=tmp_path / "task",
            transforms=transforms,
            target_properties=[{"bam"}],
            target_names=["bam"],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    def test_trace_survives_save_load(self, result_lib, tmp_path):
        """Save, load, verify Trace results identical."""
        original_pairs = set(
            (str(a.path), str(b.path))
            for a, b in result_lib.Trace("mock::bam", "mock::assembly")
        )
        assert len(original_pairs) == 3

        # Save to new location and reload
        dest = tmp_path / "roundtrip1.xgdb"
        shutil.copytree(result_lib.location, dest)
        reloaded = DataInstanceLibrary.Load(dest)

        reloaded_pairs = set(
            (str(a.path), str(b.path))
            for a, b in reloaded.Trace("mock::bam", "mock::assembly")
        )
        assert original_pairs == reloaded_pairs

    def test_trace_survives_double_roundtrip(self, result_lib, tmp_path):
        """Save/load twice, Trace still identical."""
        original_pairs = set(
            (str(a.path), str(b.path))
            for a, b in result_lib.Trace("mock::bam", "mock::assembly")
        )

        # First round-trip
        dest1 = tmp_path / "roundtrip_a.xgdb"
        shutil.copytree(result_lib.location, dest1)
        lib1 = DataInstanceLibrary.Load(dest1)

        # Second round-trip
        dest2 = tmp_path / "roundtrip_b.xgdb"
        shutil.copytree(lib1.location, dest2)
        lib2 = DataInstanceLibrary.Load(dest2)

        final_pairs = set(
            (str(a.path), str(b.path))
            for a, b in lib2.Trace("mock::bam", "mock::assembly")
        )
        assert original_pairs == final_pairs


# ---------------------------------------------------------------------------
# TestTraceSharedInputs
# ---------------------------------------------------------------------------


class TestTraceSharedInputs:
    """Shared-input topology: 1 container broadcast to N per-sample assemblies.

    Topology: container + assembly -> annotated (via shared_input_transform).
    The container is a parent of each assembly in the data library, so the
    Orchestrator treats it as a broadcast/shared input via .combine().

    This reproduces the production bug where _batch() in-place mutation of
    index HashMaps corrupts the pending_tasks HashSet, causing rare lineage
    key loss in output manifests.
    """

    N_SAMPLES = 15

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = self._make_shared_input_samples(tmp_path / "data", mock_types)
        transforms = shared_input_transform()
        tr_lib = create_transform_library(
            tmp_path / "task" / "transforms", mock_types, transforms,
        )

        # Build given views manually: each view contains {assembly_i, container}.
        # AsSamples can't correctly handle shared inputs that are parents of
        # per-sample data, so we construct views by hand.
        container_path = Path("container/container.txt")
        given = []
        for i in range(self.N_SAMPLES):
            asm_path = Path(f"sample_{i:02d}/assembly.fa")
            mask = {asm_path, container_path}
            view = DataInstanceLibraryView(original=samples, mask=mask)
            given.append([view])

        target_model = Transform()
        target_model.AddRequirement(properties={"annotated"})
        target_names = ["annotated"]

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )

        assert isinstance(plan, WorkflowPlan)
        task = WorkflowTask(
            ok=True,
            plan=plan,
            data_libraries=[samples],
            transform_libraries=[tr_lib],
        )
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    @staticmethod
    def _make_shared_input_samples(
        data_dir: Path, mock_types: Path,
    ) -> DataInstanceLibrary:
        """Create 1 container + N assemblies with container as parent."""
        lib_path = data_dir / "samples.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        # Single shared container
        container_dir = lib.location / "container"
        container_dir.mkdir(parents=True, exist_ok=True)
        (container_dir / "container.txt").write_text("mock container")
        container_path = lib.AddItem(
            Path("container/container.txt"), "mock::container",
        )

        # Per-sample assemblies, each with the container as parent
        for i in range(TestTraceSharedInputs.N_SAMPLES):
            sample_id = f"sample_{i:02d}"
            sample_dir = lib.location / sample_id
            sample_dir.mkdir(parents=True, exist_ok=True)
            (sample_dir / "assembly.fa").write_text(f"mock assembly {i}")
            lib.AddItem(
                Path(f"{sample_id}/assembly.fa"),
                "mock::assembly",
                parents=[container_path],
            )

        lib.Save()
        return lib

    def test_all_manifests_have_all_parent_keys(self, tmp_path, mock_types, docker_image):
        """Every manifest entry's lineage dict contains ALL expected type keys.

        This directly catches the production bug: when _batch() corrupts the
        HashSet, some entries lose parent keys from their lineage index.
        """
        samples = self._make_shared_input_samples(tmp_path / "mdata", mock_types)
        transforms = shared_input_transform()
        tr_lib = create_transform_library(
            tmp_path / "mtask" / "transforms", mock_types, transforms,
        )

        container_path = Path("container/container.txt")
        given = []
        for i in range(self.N_SAMPLES):
            asm_path = Path(f"sample_{i:02d}/assembly.fa")
            mask = {asm_path, container_path}
            view = DataInstanceLibraryView(original=samples, mask=mask)
            given.append([view])

        target_model = Transform()
        target_model.AddRequirement(properties={"annotated"})
        target_names = ["annotated"]

        plan = WorkflowPlan.Generate(
            given=given,
            transforms=[tr_lib],
            target_names=target_names,
            target_model=target_model,
        )
        assert isinstance(plan, WorkflowPlan)
        task = WorkflowTask(
            ok=True,
            plan=plan,
            data_libraries=[samples],
            transform_libraries=[tr_lib],
        )

        work_dir = tmp_path / "mws"
        work_dir.mkdir(parents=True, exist_ok=True)

        context = NextflowGenContext(
            workflow_file=AgentPaths.NXF_WORKFLOW,
            work_dir=work_dir,
            external_work=work_dir,
            home_dir=work_dir,
            external_home=work_dir,
            runtime=Runtime.DOCKER,
            resources_file=AgentPaths.NXF_RES,
        )
        task.PrepareNextflow(context)

        # Copy Orchestrator.groovy
        lib_dir = work_dir / "lib"
        lib_dir.mkdir(exist_ok=True)
        shutil.copy(ORCHESTRATOR_SRC, lib_dir / "Orchestrator.groovy")

        # Run nextflow stub
        tmp_root = work_dir
        while tmp_root.parent != tmp_root and tmp_root.parent != Path("/tmp"):
            tmp_root = tmp_root.parent
        result = subprocess.run(
            [
                "docker", "run", "--rm",
                "-v", f"{tmp_root}:{tmp_root}",
                "-w", str(work_dir),
                docker_image,
                "nextflow", "run", AgentPaths.NXF_WORKFLOW,
                "-stub",
                "-lib", "./lib",
                "-ansi-log", "false",
            ],
            capture_output=True, text=True, timeout=300,
        )
        nxf_clean_exit = _assert_nxf_ok(result)

        # Fix ownership
        subprocess.run(
            ["docker", "run", "--rm",
             "-v", f"{tmp_root}:{tmp_root}",
             docker_image,
             "chmod", "-R", "a+rw", str(work_dir)],
            capture_output=True, timeout=120,
        )

        # Check raw manifest JSON files for missing keys. These are emitted by
        # `publish` BEFORE invokeOnComplete fires, so they survive a tolerated
        # upstream Duration assertion.
        manifests_path = work_dir / "results" / "_manifests"
        manifests_path.mkdir(parents=True, exist_ok=True)
        manifest_files = list(manifests_path.glob("*.json"))
        assert len(manifest_files) > 0, (
            "No manifest JSON files found."
            + (
                " Nextflow exited non-zero solely due to the upstream Duration "
                "assertion (#6757), but no manifests reached disk — the workflow "
                "body itself failed earlier." if not nxf_clean_exit else ""
            )
        )

        # Determine which instance keys should appear in lineage
        # by reading the inputs directory
        inputs_dir = work_dir / "inputs"
        expected_keys = {p.name for p in inputs_dir.iterdir() if p.is_file()}

        all_entries = []
        for mf in manifest_files:
            with open(mf) as f:
                entries = json.load(f)
            for lin_str, path in entries:
                lineage = json.loads(lin_str)
                all_entries.append((path, lineage))

        assert len(all_entries) == self.N_SAMPLES, (
            f"Expected {self.N_SAMPLES} manifest entries, got {len(all_entries)}"
        )

        # Every entry's lineage must contain ALL expected parent keys
        missing = []
        for path, lineage in all_entries:
            lineage_keys = set(lineage.keys())
            for ek in expected_keys:
                if ek not in lineage_keys:
                    missing.append((path, ek))

        assert len(missing) == 0, (
            f"{len(missing)} manifest entries are missing parent keys:\n"
            + "\n".join(f"  {path} missing key '{k}'" for path, k in missing[:20])
        )

    def test_trace_output_to_per_sample_input(self, result_lib):
        """Trace annotated -> assembly yields N pairs, correctly paired."""
        pairs = list(result_lib.Trace("mock::annotated", "mock::assembly"))
        assert len(pairs) == self.N_SAMPLES

    def test_trace_output_to_shared_container(self, result_lib):
        """Trace annotated -> container yields N pairs (all same container)."""
        pairs = list(result_lib.Trace("mock::annotated", "mock::container"))
        assert len(pairs) == self.N_SAMPLES
        # All should point to the same single container
        containers = {str(c.path) for _, c in pairs}
        assert len(containers) == 1, (
            f"Expected 1 unique container, got {len(containers)}: {containers}"
        )

    def test_no_cross_sample_contamination(self, result_lib):
        """Each annotated output traces to exactly its own assembly."""
        for ann_inst, asm_inst in result_lib.Trace("mock::annotated", "mock::assembly"):
            ann_sample = str(ann_inst.path).split("/")[0] if "/" in str(ann_inst.path) else None
            asm_sample = str(asm_inst.path).split("/")[0] if "/" in str(asm_inst.path) else None
            if ann_sample and asm_sample:
                assert ann_sample == asm_sample, (
                    f"Cross-sample contamination: annotated={ann_inst.path} "
                    f"traces to assembly={asm_inst.path}"
                )
