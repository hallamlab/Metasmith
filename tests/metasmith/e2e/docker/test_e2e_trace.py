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

    lib_dir = work_dir / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(ORCHESTRATOR_SRC, lib_dir / "Orchestrator.groovy")

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
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    nxf_clean_exit = _assert_nxf_ok(result)

    subprocess.run(
        ["docker", "run", "--rm",
         "-v", f"{tmp_root}:{tmp_root}",
         docker_image,
         "chmod", "-R", "a+rw", str(work_dir)],
        capture_output=True, timeout=120,
    )

    from metasmith.caching.promote import promote_run
    cache_root = work_dir / "task_cache"
    try:
        promote_run(workspace=work_dir, cache_root=cache_root)
    except Exception as e:
        if not nxf_clean_exit:
            raise AssertionError(
                f"promote_run failed after a tolerated upstream Duration "
                f"assertion. Underlying error: {e!r}\n"
                f"STDOUT:\n{result.stdout[-2000:]}\n"
                f"STDERR:\n{(result.stderr or '')[-2000:]}"
            ) from e
        raise

    output_path = work_dir / "results"
    inputs_dir = work_dir / "inputs"

    try:
        output = CollectResults(
            task=task,
            output_path=output_path,
            inputs_dir=inputs_dir,
        )
        output.Save()
        loaded = DataInstanceLibrary.Load(output.location)
    except Exception as e:
        if not nxf_clean_exit:
            raise AssertionError(
                f"CollectResults failed after a tolerated upstream Duration "
                f"assertion (nextflow-io/nextflow#6757).\nUnderlying error: {e!r}\n"
                f"STDOUT:\n{result.stdout[-2000:]}\n"
                f"STDERR:\n{(result.stderr or '')[-2000:]}"
            ) from e
        raise
    return loaded


def _sample_of(inst) -> str:
    """The sample a given belongs to: `_make_samples` puts one directory per sample."""
    return Path(inst.path).parent.name


def assert_one_given_each(pairs, n_samples: int, label: str) -> None:
    """Each produced file traces to exactly one sample, and no two share one.

    A mis-attribution that hands every output the same producer still yields the
    right pair COUNT, so counting cannot see it. Distinctness can.
    """
    by_output: dict[str, set[str]] = {}
    for produced, given in pairs:
        by_output.setdefault(str(produced.path), set()).add(_sample_of(given))
    assert len(by_output) == n_samples, (
        f"{label}: expected {n_samples} produced files, got {sorted(by_output)}"
    )
    ambiguous = {k: v for k, v in by_output.items() if len(v) != 1}
    assert not ambiguous, f"{label}: outputs tracing to several samples: {ambiguous}"
    claimed = [next(iter(v)) for v in by_output.values()]
    assert len(set(claimed)) == n_samples, (
        f"{label}: {n_samples} outputs claim only {sorted(set(claimed))}"
    )


def _make_samples(
    temp_dir, mock_types, n_samples=3,
    types: list[tuple[str, str]] | None = None,
) -> DataInstanceLibrary:
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


class TestTraceLinearChain:
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
        pairs = list(result_lib.Trace("mock::bam", "mock::assembly"))
        assert len(pairs) == 3

    def test_output_to_transitive_ancestor(self, result_lib):
        pairs = list(result_lib.Trace("mock::bam", "mock::reads"))
        assert len(pairs) == 3

    def test_reverse_input_to_output(self, result_lib):
        pairs = list(result_lib.Trace("mock::assembly", "mock::bam"))
        assert len(pairs) == 3

    def test_no_cross_sample_contamination(self, result_lib):
        assert_one_given_each(
            result_lib.Trace("mock::bam", "mock::assembly"), 3, "bam->assembly"
        )


class TestTraceFanOutMerge:
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
        merged_items = [
            p for p, n in result_lib.manifest.items()
            if n == "mock::merged"
        ]
        assert len(merged_items) == 3

    def test_merged_has_assembly_ancestor(self, result_lib):
        pairs = list(result_lib.Trace("mock::merged", "mock::assembly"))
        assert len(pairs) >= 3

    def test_assembly_traces_to_merged(self, result_lib):
        pairs = list(result_lib.Trace("mock::assembly", "mock::merged"))
        assert len(pairs) >= 3

    def test_intermediates_published_in_output(self, result_lib):
        type_names = set(result_lib.manifest.values())
        assert "mock::branch_a" in type_names
        assert "mock::branch_b" in type_names

    def test_each_merged_traces_to_one_sample(self, result_lib):
        assert_one_given_each(
            result_lib.Trace("mock::merged", "mock::assembly"),
            3, "merged->assembly",
        )

    def test_each_merged_takes_both_branches_of_its_sample(self, result_lib):
        branch_sample = {
            str(b.path): _sample_of(a)
            for branch in ("mock::branch_a", "mock::branch_b")
            for b, a in result_lib.Trace(branch, "mock::assembly")
        }
        for merged, _ in result_lib.Trace("mock::merged", "mock::assembly"):
            feeding = {
                branch_sample[str(b.path)]
                for branch in ("mock::branch_a", "mock::branch_b")
                for m, b in result_lib.Trace("mock::merged", branch)
                if str(m.path) == str(merged.path)
            }
            assert len(feeding) == 1, (
                f"{merged.path} merges branches from samples {sorted(feeding)}"
            )


class TestTraceMultiStepDiamond:
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
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::reads"))
            assert len(pairs) == 3, f"{bin_type}->reads: expected 3, got {len(pairs)}"

    def test_final_output_to_given_input(self, result_lib):
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::assembly"))
            assert len(pairs) == 3, f"{bin_type}->assembly: expected 3, got {len(pairs)}"

    def test_different_bin_types_same_count(self, result_lib):
        counts = {}
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            pairs = list(result_lib.Trace(bin_type, "mock::assembly"))
            counts[bin_type] = len(pairs)
        assert all(c == 3 for c in counts.values()), f"Uneven counts: {counts}"

    def test_no_cross_sample_contamination_multi_output(self, result_lib):
        for bin_type in ["mock::metabat2_bins", "mock::maxbin2_bins", "mock::concoct_bins"]:
            assert_one_given_each(
                result_lib.Trace(bin_type, "mock::assembly"),
                3, f"{bin_type}->assembly",
            )


class TestTraceScaling:
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
        pairs = list(result_lib.Trace("mock::bam", "mock::assembly"))
        assert len(pairs) == self.N_SAMPLES

    def test_many_samples_correct_pairing(self, result_lib):
        assert_one_given_each(
            result_lib.Trace("mock::bam", "mock::assembly"),
            self.N_SAMPLES, "bam->assembly",
        )


class TestTracePersistence:
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
        original_pairs = set(
            (str(a.path), str(b.path))
            for a, b in result_lib.Trace("mock::bam", "mock::assembly")
        )
        assert len(original_pairs) == 3

        dest = tmp_path / "roundtrip1.xgdb"
        shutil.copytree(result_lib.location, dest)
        reloaded = DataInstanceLibrary.Load(dest)

        reloaded_pairs = set(
            (str(a.path), str(b.path))
            for a, b in reloaded.Trace("mock::bam", "mock::assembly")
        )
        assert original_pairs == reloaded_pairs

    def test_trace_survives_double_roundtrip(self, result_lib, tmp_path):
        original_pairs = set(
            (str(a.path), str(b.path))
            for a, b in result_lib.Trace("mock::bam", "mock::assembly")
        )

        dest1 = tmp_path / "roundtrip_a.xgdb"
        shutil.copytree(result_lib.location, dest1)
        lib1 = DataInstanceLibrary.Load(dest1)

        dest2 = tmp_path / "roundtrip_b.xgdb"
        shutil.copytree(lib1.location, dest2)
        lib2 = DataInstanceLibrary.Load(dest2)

        final_pairs = set(
            (str(a.path), str(b.path))
            for a, b in lib2.Trace("mock::bam", "mock::assembly")
        )
        assert original_pairs == final_pairs


class TestTraceSharedInputs:
    N_SAMPLES = 15

    @pytest.fixture
    def result_lib(self, tmp_path, mock_types, docker_image):
        samples = self._make_shared_input_samples(tmp_path / "data", mock_types)
        transforms = shared_input_transform()
        tr_lib = create_transform_library(
            tmp_path / "task" / "transforms", mock_types, transforms,
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
        return run_stub_workflow(task, tmp_path / "ws", docker_image)

    @staticmethod
    def _make_shared_input_samples(
        data_dir: Path, mock_types: Path,
    ) -> DataInstanceLibrary:
        lib_path = data_dir / "samples.xgdb"
        lib = DataInstanceLibrary(lib_path)
        lib.AddTypeLibrary(mock_types, namespace="mock")

        container_dir = lib.location / "container"
        container_dir.mkdir(parents=True, exist_ok=True)
        (container_dir / "container.txt").write_text("mock container")
        container_path = lib.AddItem(
            Path("container/container.txt"), "mock::container",
        )

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

    def test_trace_records_all_parent_keys(self, tmp_path, mock_types, docker_image):
        from metasmith.telemetry import TraceIndex

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
        run_stub_workflow(task, work_dir, docker_image, timeout=300)

        trace_path = work_dir / "_metasmith" / "trace.jsonl"
        assert trace_path.exists(), f"no trace.jsonl at {trace_path}"
        trace = TraceIndex.read(trace_path)

        promote_events = [
            ev for ev in trace.events
            if ev.status in ("promoted", "hit", "miss") and ev.produces
        ]
        assert len(promote_events) >= self.N_SAMPLES, (
            f"Expected >= {self.N_SAMPLES} non-sentinel events, "
            f"got {len(promote_events)}"
        )

        inputs_dir = work_dir / "inputs"
        expected_keys = {p.name for p in inputs_dir.iterdir() if p.is_file()}
        assert len(expected_keys) >= 2, (
            f"could not derive expected slot keys; got {expected_keys}"
        )

        missing: list[tuple[str, str]] = []
        for ev in promote_events:
            for ek in expected_keys:
                if ek not in ev.consumes:
                    missing.append((ev.task_hash, ek))

        assert len(missing) == 0, (
            f"{len(missing)} events are missing parent slot keys:\n"
            + "\n".join(f"  task_hash={th} missing '{k}'" for th, k in missing[:20])
        )

    def test_trace_output_to_per_sample_input(self, result_lib):
        pairs = list(result_lib.Trace("mock::annotated", "mock::assembly"))
        assert len(pairs) == self.N_SAMPLES

    def test_trace_output_to_shared_container(self, result_lib):
        pairs = list(result_lib.Trace("mock::annotated", "mock::container"))
        assert len(pairs) == self.N_SAMPLES
        containers = {str(c.path) for _, c in pairs}
        assert len(containers) == 1, (
            f"Expected 1 unique container, got {len(containers)}: {containers}"
        )

    def test_no_cross_sample_contamination(self, result_lib):
        assert_one_given_each(
            result_lib.Trace("mock::annotated", "mock::assembly"),
            self.N_SAMPLES, "annotated->assembly",
        )


class TestStubTraceHasInvocationEvents:
    N_SAMPLES = 3

    @pytest.fixture
    def workspace_after_stub_run(self, tmp_path, mock_types, docker_image):
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
        work_dir = tmp_path / "ws"
        run_stub_workflow(task, work_dir, docker_image)
        return work_dir

    def test_trace_has_promote_events(self, workspace_after_stub_run):
        trace = workspace_after_stub_run / "_metasmith" / "trace.jsonl"
        assert trace.exists(), f"no trace.jsonl at {trace}"
        events = [
            json.loads(l)
            for l in trace.read_text().splitlines()
            if l.strip() and json.loads(l).get("event") != "session_start"
        ]
        n_steps = 1
        assert len(events) >= n_steps * self.N_SAMPLES, (
            f"expected >= {n_steps * self.N_SAMPLES} non-sentinel events "
            f"(n_steps × n_samples), got {len(events)} — docker stub bypasses promote_run"
        )
