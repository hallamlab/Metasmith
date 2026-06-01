"""Shared scaffolding for tests/flow.

Three layers:

1. **Fixtures** — `virtual_runtime` (reused from project conftest),
   `attached_library`, `oracle`.
2. **Plan builders** — small factories that hand a runnable `WorkflowPlan`
   (often plus the backing libraries) to a test. Reuse `mock_transforms`
   for stimulus; never write inline transform code.
3. **Assertion helpers** — telemetry-only checks against
   `DataInstanceLibrary.Load(attach_trace=True)`. No `.nf` text reads, no
   workdir filename inspection.

Builders that depend on missing telemetry surface or stimulus shapes
mark themselves with `pytest.skip(reason)` so downstream tests still
collect cleanly.
"""

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
from metasmith.coms.containers import ContainerRuntime
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


# ---------------------------------------------------------------------------
# Section 1: Fixtures
# ---------------------------------------------------------------------------


# The project-wide `virtual_runtime` fixture is defined in tests/conftest.py
# and is therefore visible here without re-declaration.


@pytest.fixture
def attached_library() -> Callable[[Path], DataInstanceLibrary]:
    """Factory: load a results library with trace.jsonl attached."""

    def _load(path: Path) -> DataInstanceLibrary:
        return DataInstanceLibrary.Load(path, attach_trace=True)

    return _load


@pytest.fixture
def oracle() -> Callable[[WorkflowTask], PlanExecutionOracle]:
    """Factory: build a `PlanExecutionOracle` for a staged `WorkflowTask`."""

    def _oracle(task: WorkflowTask) -> PlanExecutionOracle:
        return PlanExecutionOracle(task=task)

    return _oracle


# ---------------------------------------------------------------------------
# Internal: type catalogue + library builders
# ---------------------------------------------------------------------------


# Master type list — extend here when a builder needs a new dtype name.
# Keep names aligned with `mock_transforms` so the stimulus library imports
# them by `lib.GetType("mock::<name>")` cleanly.
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
    # 5-hop chain dtypes for build_5hop_dag_plan / build_linear_plan(n>=5).
    "h1": {"h1"},
    "h2": {"h2"},
    "h3": {"h3"},
    "h4": {"h4"},
    "h5": {"h5"},
    # generic dtype "data" for empty/dead-output builders.
    "data": {"data"},
}


def _build_type_lib(out_path: Path, names: Iterable[str] | None = None) -> Path:
    """Write a mock DataTypeLibrary YAML containing the named endpoints."""
    types = DataTypeLibrary()
    selected = list(names) if names is not None else list(_MOCK_TYPE_PROPERTIES)
    # Always include slot_0..slot_N up to a small upper bound so
    # multi_slot_producer / failing_at_slot_k can resolve their types.
    for n in selected:
        props = _MOCK_TYPE_PROPERTIES.get(n, {n})
        types[n] = Endpoint(properties=props)
    for i in range(8):
        n = f"slot_{i}"
        types[n] = Endpoint(properties={n})
    types.Save(out_path)
    return out_path


def _build_samples_lib(
    tmp_path: Path,
    types_path: Path,
    *,
    n_samples: int = 1,
    dtype: str = "assembly",
    namespace: str = "mock",
    shared_root: bool = False,
) -> DataInstanceLibrary:
    """Build an N-sample DataInstanceLibrary with one instance of `dtype` each."""
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace=namespace)
    parents: list = []
    if shared_root:
        (lib.location / "root.json").write_text('{"id": "root"}', encoding="utf-8")
        root = lib.AddItem(Path("root.json"), f"{namespace}::sample_metadata")
        parents = [root]
    for i in range(n_samples):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / f"{dtype}.txt").write_text(f">{sid}\nACGT\n", encoding="utf-8")
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
) -> TransformInstanceLibrary:
    """Build a transforms.xgdb that uses the given types_path under `namespace`."""
    tr_path = base_dir / "transforms.xgdb"
    tr_path.mkdir(parents=True, exist_ok=True)
    meta = tr_path / "_metadata"
    types_dir = meta / "types"
    types_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(types_path, types_dir / f"{namespace}.yml")
    (types_dir / "transforms.yml").write_text(
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
        encoding="utf-8",
    )
    manifest: dict[str, dict[str, str]] = {}
    for name, code in transforms.items():
        (tr_path / f"{name}.py").write_text(code, encoding="utf-8")
        manifest[f"{name}.py"] = {"type": "transforms::transform"}
    (meta / "index.yml").write_text(
        yaml.dump({"manifest": manifest, "schema": "v1"}), encoding="utf-8"
    )
    return TransformInstanceLibrary.Load(tr_path)


def _make_target_model(target_props: list[set[str]]) -> Transform:
    """Build a Transform with one requirement per `set` of properties."""
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
    """Wrap WorkflowPlan.Generate with the canonical mock-fixture arguments."""
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
    """Bundle returned by plan-builder helpers."""

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


# ---------------------------------------------------------------------------
# Section 2: Plan-builder helpers
# ---------------------------------------------------------------------------


def build_linear_plan(
    tmp_path: Path,
    n_steps: int = 2,
    dtype_chain: list[str] | None = None,
) -> BuiltPlan:
    """N-step identity chain: assembly → bam → ... — drives L1-L3."""
    assert n_steps >= 1, "linear plan needs at least 1 step"
    if dtype_chain is None:
        # Default chain uses assembly → bam → branch_a → branch_b → merged.
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
    """Single transform that consumes `slots` inputs — drives L4.

    Uses alignment_transform (reads + assembly → bam) for slots=2; for
    other slot counts it skips (no out-of-the-box multi-input mock).
    """
    if slots != 2:
        pytest.skip(
            f"build_multi_input_plan: only slots=2 wired (alignment_transform)"
        )
    types_path = _build_type_lib(tmp_path / "types.yml")
    # Build samples with both reads and assembly per sample.
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    lib.AddTypeLibrary(types_path, namespace="mock")
    for i in range(1):
        sid = f"sample_{i:02d}"
        sdir = lib.location / sid
        sdir.mkdir(parents=True, exist_ok=True)
        (sdir / "reads.fq").write_text(f">r_{i}\nACGT\n", encoding="utf-8")
        (sdir / "assembly.fa").write_text(f">a_{i}\nACGTACGT\n", encoding="utf-8")
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


def build_branching_plan(tmp_path: Path, fanout: int = 2) -> BuiltPlan:
    """N-way branching plan: assembly → {branch_a, ..., branch_<n-1>} → merged.

    fanout must be in [2, 8] — driven by `mock_transforms.branching_transforms(n)`.
    """
    types_path = _build_type_lib(tmp_path / "types.yml")
    samples = _build_samples_lib(tmp_path, types_path, dtype="assembly")
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
    """Branch where one slot fails — drives B3 sibling-branch independence."""
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
    """Single transform producing N distinct slots — drives F1-F4."""
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


def build_batched_plan(
    tmp_path: Path,
    n_inputs: int = 3,
    batch_size: int = 2,
    group_key_fn: Callable[[int], str] | None = None,
) -> BuiltPlan:
    """N inputs through a batched transform — drives G1-G6, G8."""
    _ = group_key_fn  # reserved; batched_transform groups by the input dep
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
    """Group-then-unfold pair — drives GS1-GS3."""
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


def build_lineage_fork_plan(tmp_path: Path, parent_count: int = 2) -> BuiltPlan:
    """Two TargetBuilder.Add() calls with distinct parents — LP1/LP2 shape.

    Marked skip until a stimulus shape exists in mock_transforms that
    naturally produces two distinct subtypes of the same dtype (the
    inbox #135 duplicate-producer trap). Downstream tests covering
    LP1-LP3 can reuse the repro file directly.
    """
    _ = parent_count
    # TODO: depends on a mock_transforms shape emitting two parent-distinct
    # subtypes of the same dtype.
    pytest.skip(
        "build_lineage_fork_plan: needs a mock_transforms shape with "
        "two parent-distinct producers (relocate from repro_135)"
    )


def build_mixed_cacheability_plan(tmp_path: Path) -> BuiltPlan:
    """Three-step chain with the middle step `cacheable=False` — drives C1.

    Generates three identity transforms via the _cache_harness pattern,
    flipping cacheable=False on the middle hop. Reuses the local helper
    rather than mock_transforms because mock_transforms does not expose
    a cacheable=False knob.
    """
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
    """Local identity-transform builder with an explicit cacheable flag."""
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
    """Empty input library — drives E1/G4.

    The planner raises when no targets are reachable; this builder catches
    that and skips so downstream E1 tests still collect. A future change
    that makes Generate return PlanHint instead of raising will let this
    return a real (failed) plan for assertion.
    """
    # TODO: depends on WorkflowPlan.Generate returning a PlanHint object for
    # unreachable targets rather than raising.
    pytest.skip(
        "build_empty_plan: WorkflowPlan.Generate currently raises on empty "
        "input; needs a PlanHint return shape to assert against"
    )


def build_dead_output_plan(tmp_path: Path) -> BuiltPlan:
    """Plan with a transform whose output is never consumed — drives E4.

    Currently mocked by a two-step plan where the target is satisfied at
    step 1 and step 2 is reachable but irrelevant. Real dead-output
    semantics need a multi-product transform with one consumed and one
    dangling slot — skip until that shape exists.
    """
    # TODO: depends on a mock_transforms shape with a dangling product slot
    # (multi-slot producer where only some slots are demanded).
    pytest.skip(
        "build_dead_output_plan: needs a multi-slot transform with one "
        "intentionally-unconsumed product slot"
    )


def build_5hop_dag_plan(tmp_path: Path) -> BuiltPlan:
    """5-hop linear DAG (assembly → h1 → ... → h5) — drives S4 walk_ancestors."""
    chain = ["assembly", "h1", "h2", "h3", "h4", "h5"]
    return build_linear_plan(tmp_path, n_steps=5, dtype_chain=chain)


# ---------------------------------------------------------------------------
# Section 3: Assertion helpers (telemetry-only)
# ---------------------------------------------------------------------------


def _stage_and_run(
    rt: VirtualE2ERuntime, plan: WorkflowPlan | BuiltPlan
) -> tuple[Path, WorkflowTask]:
    """Stage a task, run RunWorkflow against the virtual runtime, return paths."""
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
        container_runtime=ContainerRuntime.DOCKER,
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
    """Stage + run `plan`, return `(staged_task, results_library)`."""
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
    """Assert `walk_ancestors(target_id)` yields the named dtype chain in order."""
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
    """Assert ancestors reachable from `target_id` cover `expected_root_dtypes`."""
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
    """Assert N events with `(transform_key, status)` appear in the trace."""
    events = lib.find_invocations(transform_key=transform_key, status=status)
    assert len(events) == count, (
        f"expected {count} events for transform_key={transform_key} "
        f"status={status}, got {len(events)}"
    )


def assert_no_failures(lib: DataInstanceLibrary) -> None:
    """Assert `find_failures()` is empty."""
    failures = lib.find_failures()
    assert not failures, f"unexpected failures in trace: {failures}"


def assert_failure_count(
    lib: DataInstanceLibrary, n: int, transform_key: str | None = None
) -> None:
    """Assert exactly `n` failures (optionally filtered by transform_key)."""
    failures = lib.find_failures()
    if transform_key is not None:
        failures = [f for f in failures if f.transform_key == transform_key]
    assert len(failures) == n, (
        f"expected {n} failures (transform_key={transform_key}), got {len(failures)}"
    )


def assert_arity_via_oracle(
    events: list[dict[str, Any]], task: WorkflowTask
) -> None:
    """Run `PlanExecutionOracle(task).validate_trace(events)`."""
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
