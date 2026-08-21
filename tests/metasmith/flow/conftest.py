from __future__ import annotations

import shutil
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import pytest
import yaml

from metasmith.agents import RunWorkflow
from metasmith.constants import AgentPaths, MODULE_PATH
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint, Transform
from metasmith.models.workflow import (
    NextflowGenContext,
    WorkflowPlan,
    WorkflowTask,
)
from metasmith.testing import mock_transforms as mt
from metasmith.testing.plan_oracle import PlanExecutionOracle
from metasmith.testing.virtual_runtime import VirtualE2ERuntime


@pytest.fixture
def attached_library() -> Callable[[Path], DataInstanceLibrary]:
    def _load(path: Path) -> DataInstanceLibrary:
        return DataInstanceLibrary.Load(path, attach_trace=True)

    return _load


@pytest.fixture
def oracle() -> Callable[[WorkflowTask], PlanExecutionOracle]:
    def _oracle(task: WorkflowTask) -> PlanExecutionOracle:
        return PlanExecutionOracle(task=task)

    return _oracle


_MOCK_TYPE_PROPERTIES: dict[str, set[str]] = {
    "sample_metadata": {"sample_metadata"},
    "reads": {"reads"},
    "assembly": {"assembly"},
    "bam": {"bam"},
    "metabat2_bins": {"bins", "method:metabat2"},
    "maxbin2_bins": {"bins", "method:maxbin2"},
    "concoct_bins": {"bins", "method:concoct"},
    "branch_a": {"branch_a"},
    "branch_b": {"branch_b"},
    "branch_c": {"branch_c"},
    "branch_d": {"branch_d"},
    "branch_e": {"branch_e"},
    "branch_f": {"branch_f"},
    "branch_g": {"branch_g"},
    "branch_h": {"branch_h"},
    "merged": {"merged"},
    "container": {"container"},
    "annotated": {"annotated"},
    "grouped": {"grouped"},
    "unfolded": {"unfolded"},
    "label": {"label"},
    "h1": {"h1"},
    "h2": {"h2"},
    "h3": {"h3"},
    "h4": {"h4"},
    "h5": {"h5"},
    "data": {"data"},
}


def _build_type_lib(out_path: Path, names: Iterable[str] | None = None) -> Path:
    types = DataTypeLibrary()
    selected = list(names) if names is not None else list(_MOCK_TYPE_PROPERTIES)
    for n in selected:
        props = _MOCK_TYPE_PROPERTIES.get(n, {n})
        types[n] = Endpoint(properties=props)
    for i in range(8):
        n = f"slot_{i}"
        types[n] = Endpoint(properties={n})
    types.Save(out_path)
    return out_path


def _write_input(path: Path, text: str) -> None:
    # Leave an input file alone when its content already matches.
    #
    # A leaf id is the file's path and mtime, and several tests here model a
    # re-run by calling a builder twice against one `tmp_path`. Rewriting
    # identical bytes would move mtime, re-key every input, and turn the second
    # run into a cold cache -- which is the thing those tests are measuring.
    if path.is_file() and path.read_text(encoding="utf-8") == text:
        return
    path.write_text(text, encoding="utf-8")


def _build_samples_lib(
    tmp_path: Path,
    types_path: Path,
    *,
    n_samples: int = 1,
    dtype: str = "assembly",
    namespace: str = "mock",
    shared_root: bool = False,
) -> DataInstanceLibrary:
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace=namespace)
    parents: list = []
    if shared_root:
        _write_input(lib.location / "root.json", '{"id": "root"}')
        root = lib.AddItem(Path("root.json"), f"{namespace}::sample_metadata")
        parents = [root]
    for i in range(n_samples):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        _write_input(sdir / f"{dtype}.txt", f">{sid}\nACGT\n")
        lib.AddItem(
            Path(f"{sid}/{dtype}.txt"),
            f"{namespace}::{dtype}",
            parents=parents or None,
        )
    lib.Save()
    return lib


def _build_transform_lib(
    base_dir: Path,
    types_path: Path,
    transforms: dict[str, str],
    *,
    namespace: str = "mock",
    library_name: str = "transforms.xgdb",
) -> TransformInstanceLibrary:
    tr_path = base_dir / library_name
    tr_path.mkdir(parents=True, exist_ok=True)
    meta = tr_path / "_metadata"
    types_dir = meta / "types"
    types_dir.mkdir(parents=True, exist_ok=True)
    _write_input(types_dir / f"{namespace}.yml",
                 types_path.read_text(encoding="utf-8"))
    _write_input(types_dir / "transforms.yml",
        textwrap.dedent(
            """\
            schema: v1
            ontology:
              name: EDAM
              version: '1.25'
              doi: https://doi.org/10.1093/bioinformatics/btt113
              strict: false
            types:
              transform:
                properties:
                - metasmith
                - transform
            """
        ),
    )
    manifest: dict[str, dict[str, str]] = {}
    for name, code in transforms.items():
        _write_input(tr_path / f"{name}.py", code)
        manifest[f"{name}.py"] = {"type": "transforms::transform"}
    _write_input(meta / "index.yml",
                 yaml.dump({"manifest": manifest, "schema": "v1"}))
    return TransformInstanceLibrary.Load(tr_path)


def _make_target_model(target_props: list[set[str]]) -> Transform:
    target = Transform()
    for props in target_props:
        target.AddRequirement(properties=props)
    return target


def _generate_plan(
    samples: DataInstanceLibrary,
    transforms_lib: TransformInstanceLibrary,
    *,
    sample_dtype: str,
    target_props: list[set[str]],
    target_names: list[str],
    namespace: str = "mock",
) -> WorkflowPlan:
    given = [[sv] for sv in samples.AsSamples(f"{namespace}::{sample_dtype}")]
    target_model = _make_target_model(target_props)
    plan = WorkflowPlan.Generate(
        given=given,
        transforms=[transforms_lib],
        target_names=target_names,
        target_model=target_model,
    )
    assert isinstance(plan, WorkflowPlan), f"planner did not converge: {plan!r}"
    return plan


@dataclass
class BuiltPlan:
    plan: WorkflowPlan
    data_library: DataInstanceLibrary
    transform_libraries: list[TransformInstanceLibrary]

    def as_task(self) -> WorkflowTask:
        return WorkflowTask(
            ok=True,
            plan=self.plan,
            data_libraries=[self.data_library],
            transform_libraries=list(self.transform_libraries),
        )


def build_linear_plan(
    tmp_path: Path,
    n_steps: int = 2,
    dtype_chain: list[str] | None = None,
) -> BuiltPlan:
    assert n_steps >= 1, "linear plan needs at least 1 step"
    if dtype_chain is None:
        defaults = ["assembly", "bam", "branch_a", "branch_b", "merged", "annotated"]
        if n_steps + 1 > len(defaults):
            pytest.skip(
                f"build_linear_plan: no default dtype_chain for n_steps={n_steps}"
            )
        dtype_chain = defaults[: n_steps + 1]
    assert len(dtype_chain) == n_steps + 1, "dtype_chain must have n_steps+1 entries"

    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype=dtype_chain[0])
    transforms: dict[str, str] = {}
    for i in range(n_steps):
        transforms.update(
            mt.identity_transform(
                f"mock::{dtype_chain[i]}", f"mock::{dtype_chain[i + 1]}"
            )
        )
    tr_lib = _build_transform_lib(tmp_path / "tr", types_path, transforms)
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype=dtype_chain[0],
        target_props=[_MOCK_TYPE_PROPERTIES[dtype_chain[-1]]],
        target_names=[dtype_chain[-1]],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_multi_input_plan(tmp_path: Path, slots: int = 2) -> BuiltPlan:
    if slots != 2:
        pytest.skip(
            f"build_multi_input_plan: only slots=2 wired (alignment_transform)"
        )
    types_path = _build_type_lib(tmp_path / "types.yml")
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for i in range(1):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        _write_input(sdir / "reads.fq", f">r_{i}\nACGT\n")
        _write_input(sdir / "assembly.fa", f">a_{i}\nACGTACGT\n")
        r = lib.AddItem(Path(f"{sid}/reads.fq"), "mock::reads")
        lib.AddItem(Path(f"{sid}/assembly.fa"), "mock::assembly", parents=[r])
    lib.Save()
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.alignment_transform()
    )
    plan = _generate_plan(
        lib,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["bam"]],
        target_names=["bam"],
    )
    return BuiltPlan(plan=plan, data_library=lib, transform_libraries=[tr_lib])


def build_branching_plan(
    tmp_path: Path, fanout: int = 2, n_samples: int = 1
) -> BuiltPlan:
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(
        tmp_path, types_path, n_samples=n_samples, dtype="assembly"
    )
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.branching_transforms(n=fanout)
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["merged"]],
        target_names=["merged"],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_branching_with_failure_plan(tmp_path: Path) -> BuiltPlan:
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
    tr_lib = _build_transform_lib(
        tmp_path / "tr",
        types_path,
        mt.failing_at_slot_k(k=1, slots=2),
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[{"slot_0"}, {"slot_1"}],
        target_names=["slot_0", "slot_1"],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_fan_out_plan(tmp_path: Path, n_slots: int = 2) -> BuiltPlan:
    if n_slots > 8:
        pytest.skip(f"build_fan_out_plan: type catalogue caps n_slots at 8")
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.multi_slot_producer(slots=n_slots)
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[{f"slot_{i}"} for i in range(n_slots)],
        target_names=[f"slot_{i}" for i in range(n_slots)],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_one_group_fan_out_plan(tmp_path: Path, n_products: int = 2) -> BuiltPlan:
    if n_products > 8:
        pytest.skip("build_one_group_fan_out_plan: type catalogue caps at 8")
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
    tr_lib = _build_transform_lib(
        tmp_path / "tr",
        types_path,
        mt.multi_product_one_group(products=n_products),
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[{f"slot_{i}"} for i in range(n_products)],
        target_names=[f"slot_{i}" for i in range(n_products)],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_batched_plan(
    tmp_path: Path,
    n_inputs: int = 3,
    batch_size: int = 2,
    group_key_fn: Callable[[int], str] | None = None,
) -> BuiltPlan:
    _ = group_key_fn
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(
        tmp_path, types_path, n_samples=n_inputs, dtype="assembly"
    )
    tr_lib = _build_transform_lib(
        tmp_path / "tr",
        types_path,
        mt.batched_transform(batch_size=batch_size),
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["bam"]],
        target_names=["bam"],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_group_then_split_plan(tmp_path: Path) -> BuiltPlan:
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(
        tmp_path,
        types_path,
        n_samples=2,
        dtype="assembly",
        shared_root=True,
    )
    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.group_then_unfold()
    )
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["unfolded"]],
        target_names=["unfolded"],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def build_labelled_collection_plan(
    tmp_path: Path, n_samples: int = 3, shuffle: bool = False
) -> BuiltPlan:
    types_path = _build_type_lib(tmp_path / "types.yml")
    lib = DataInstanceLibrary(tmp_path / "labelled.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    _write_input(lib.location / "root.json", '{"id": "root"}')
    root = lib.AddItem(Path("root.json"), "mock::sample_metadata")

    order = list(range(n_samples))
    for i in order[::-1] if shuffle else order:
        sid = f"sample_{i:02d}"
        _write_input(lib.location / f"{sid}.label", f"name-of-{sid}")
        label = lib.AddItem(Path(f"{sid}.label"), "mock::label", parents=[root])
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        _write_input(sdir / "assembly.txt", f">{sid}\nACGT\n")
        lib.AddItem(Path(f"{sid}/assembly.txt"), "mock::assembly", parents=[label])
    lib.Save()

    tr_lib = _build_transform_lib(
        tmp_path / "tr", types_path, mt.labelled_collection()
    )
    plan = _generate_plan(
        lib,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["merged"]],
        target_names=["merged"],
    )
    return BuiltPlan(plan=plan, data_library=lib, transform_libraries=[tr_lib])


def build_lineage_fork_plan(tmp_path: Path, parent_count: int = 2) -> BuiltPlan:
    _ = parent_count
    # TODO: depends on a mock_transforms shape emitting two parent-distinct
    pytest.skip(
        "build_lineage_fork_plan: needs a mock_transforms shape with "
        "two parent-distinct producers (relocate from repro_135)"
    )


def build_mixed_cacheability_plan(tmp_path: Path) -> BuiltPlan:
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
    transforms: dict[str, str] = {}
    chain = ["assembly", "bam", "branch_a", "merged"]
    cacheable_per_step = [True, False, True]
    for i, cache_flag in enumerate(cacheable_per_step):
        transforms.update(
            _make_identity_with_cacheable(
                name=f"id_{chain[i + 1]}",
                input_type=f"mock::{chain[i]}",
                output_type=f"mock::{chain[i + 1]}",
                cacheable=cache_flag,
            )
        )
    tr_lib = _build_transform_lib(tmp_path / "tr", types_path, transforms)
    plan = _generate_plan(
        samples,
        tr_lib,
        sample_dtype="assembly",
        target_props=[_MOCK_TYPE_PROPERTIES["merged"]],
        target_names=["merged"],
    )
    return BuiltPlan(plan=plan, data_library=samples, transform_libraries=[tr_lib])


def _make_identity_with_cacheable(
    *, name: str, input_type: str, output_type: str, cacheable: bool
) -> dict[str, str]:
    return {
        name: textwrap.dedent(
            f"""
            from pathlib import Path
            from metasmith.models.libraries import (
                TransformInstanceLibrary,
                TransformInstance,
                ExecutionContext,
                ExecutionResult,
            )
            from metasmith.models.solver import Transform

            lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
            model = Transform()
            dep = model.AddRequirement(lib.GetType("{input_type}"))
            out = model.AddProduct(lib.GetType("{output_type}"))

            def protocol(context: ExecutionContext):
                out_path = Path("out.txt")
                out_path.write_text("{name} body")
                return ExecutionResult(manifest=[{{out: out_path}}], success=True)

            TransformInstance(
                protocol=protocol, model=model, group_by=dep, cacheable={cacheable!r}
            )
            """
        )
    }


def build_empty_plan(tmp_path: Path) -> BuiltPlan:
    # TODO: depends on WorkflowPlan.Generate returning a PlanHint object for
    pytest.skip(
        "build_empty_plan: WorkflowPlan.Generate currently raises on empty "
        "input; needs a PlanHint return shape to assert against"
    )


def build_dead_output_plan(tmp_path: Path) -> BuiltPlan:
    # TODO: depends on a mock_transforms shape with a dangling product slot
    pytest.skip(
        "build_dead_output_plan: needs a multi-slot transform with one "
        "intentionally-unconsumed product slot"
    )


def build_5hop_dag_plan(tmp_path: Path) -> BuiltPlan:
    chain = ["assembly", "h1", "h2", "h3", "h4", "h5"]
    return build_linear_plan(tmp_path, n_steps=5, dtype_chain=chain)


def _stage_and_run(
    rt: VirtualE2ERuntime, plan: WorkflowPlan | BuiltPlan
) -> tuple[Path, WorkflowTask]:
    if isinstance(plan, BuiltPlan):
        task = plan.as_task()
    else:
        raise TypeError(
            "_stage_and_run: pass a BuiltPlan; raw WorkflowPlan needs library bindings"
        )
    key = task.GetKey()
    task_path = AgentPaths.to_task(key)
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task.SaveAs(Source.FromLocal(task_path))

    staged = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    workspace = task_path.parent.parent
    workspace.mkdir(parents=True, exist_ok=True)

    context = NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=workspace,
        external_work=workspace,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=AgentPaths.HOME_ROOT,
        runtime=Runtime.DOCKER,
        resources_file=AgentPaths.NXF_RES,
    )
    staged.PrepareNextflow(context)
    lib_dir = workspace / "lib"
    lib_dir.mkdir(exist_ok=True)
    shutil.copy(
        MODULE_PATH / "nextflow_config/Orchestrator.groovy",
        lib_dir / "Orchestrator.groovy",
    )

    RunWorkflow(
        key=key,
        log_dir=Path("_metasmith/logs.virtual"),
        host=rt.host,
        stub_delay=0.0,
    )
    return workspace, staged


def run_and_load(
    rt: VirtualE2ERuntime, plan: BuiltPlan
) -> tuple[WorkflowTask, DataInstanceLibrary]:
    workspace, staged = _stage_and_run(rt, plan)
    results_dir = workspace / "results"
    assert results_dir.exists(), (
        f"results directory missing after RunWorkflow: {results_dir}"
    )
    lib = DataInstanceLibrary.Load(results_dir, attach_trace=True)
    return staged, lib


def assert_lineage_chain(
    lib: DataInstanceLibrary,
    target_id: str,
    expected_dtype_ancestors: list[str],
) -> None:
    nodes = list(lib.walk_ancestors(target_id))
    got = [getattr(n, "dtype_name", None) for n in nodes]
    assert got == expected_dtype_ancestors, (
        f"lineage chain mismatch: expected {expected_dtype_ancestors}, got {got}"
    )


def assert_walks_to_roots(
    lib: DataInstanceLibrary,
    target_id: str,
    expected_root_dtypes: set[str],
) -> None:
    seen: set[str] = set()
    for node in lib.walk_ancestors(target_id):
        dt = getattr(node, "dtype_name", None)
        if dt is not None:
            seen.add(dt)
    missing = expected_root_dtypes - seen
    assert not missing, (
        f"walk_ancestors did not reach roots: missing {missing}, saw {seen}"
    )


def assert_invocation_status(
    lib: DataInstanceLibrary,
    transform_key: str,
    status: str,
    count: int = 1,
) -> None:
    events = lib.find_invocations(transform_key=transform_key, status=status)
    assert len(events) == count, (
        f"expected {count} events for transform_key={transform_key} "
        f"status={status}, got {len(events)}"
    )


def assert_no_failures(lib: DataInstanceLibrary) -> None:
    failures = lib.find_failures()
    assert not failures, f"unexpected failures in trace: {failures}"


def assert_failure_count(
    lib: DataInstanceLibrary, n: int, transform_key: str | None = None
) -> None:
    failures = lib.find_failures()
    if transform_key is not None:
        failures = [f for f in failures if f.transform_key == transform_key]
    assert len(failures) == n, (
        f"expected {n} failures (transform_key={transform_key}), got {len(failures)}"
    )


def assert_arity_via_oracle(
    events: list[dict[str, Any]], task: WorkflowTask
) -> None:
    oracle = PlanExecutionOracle(task=task)
    oracle.validate_trace(events)


__all__ = [
    "BuiltPlan",
    "attached_library",
    "oracle",
    "build_linear_plan",
    "build_multi_input_plan",
    "build_branching_plan",
    "build_branching_with_failure_plan",
    "build_fan_out_plan",
    "build_one_group_fan_out_plan",
    "build_batched_plan",
    "build_group_then_split_plan",
    "build_lineage_fork_plan",
    "build_mixed_cacheability_plan",
    "build_empty_plan",
    "build_dead_output_plan",
    "build_5hop_dag_plan",
    "run_and_load",
    "assert_lineage_chain",
    "assert_walks_to_roots",
    "assert_invocation_status",
    "assert_no_failures",
    "assert_failure_count",
    "assert_arity_via_oracle",
]
