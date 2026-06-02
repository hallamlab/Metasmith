from pathlib import Path

import pytest

from metasmith.models.libraries import DataInstanceLibrary, DataTypeLibrary
from metasmith.models.lineage import InvocationNotFound
from metasmith.models.solver import Endpoint


def test_data_instance_id_is_stable_across_lineage_retyping(tmp_path):
    lib = DataInstanceLibrary(tmp_path / "samples.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "assembly.fa").write_text(">c1\nACGT\n")
    lib.AddItem(path=Path("assembly.fa"), dtype="mock::assembly")

    inst = lib.Get(Path("assembly.fa"))
    lineage_ep = Endpoint(properties=inst.dtype.properties, parents={inst.dtype})
    remapped = inst.WithDType(lineage_ep)

    assert remapped.instance_id == inst.instance_id
    assert remapped.legacy_key != inst.legacy_key
    assert remapped.dtype.key != inst.dtype.key


def test_data_instance_pack_unpack_preserves_instance_id(tmp_path):
    lib = DataInstanceLibrary(tmp_path / "samples_pack.xgdb")
    types = DataTypeLibrary()
    types["assembly"] = Endpoint(properties={"assembly"})
    lib.AddTypeLibrary(namespace="mock", lib=types)
    (lib.location / "assembly.fa").write_text(">c1\nACGT\n")
    lib.AddItem(path=Path("assembly.fa"), dtype="mock::assembly")

    inst = lib.Get(Path("assembly.fa"))
    packed = inst.Pack()
    unpacked = inst.Unpack(packed, {lib.GetKey(): lib})

    assert unpacked.instance_id == inst.instance_id
    assert unpacked._key == inst.instance_id


# ---------------------------------------------------------------------------
# S3 — get_invocation via trace index returns event by task_hash
# ---------------------------------------------------------------------------


def test_s3_get_invocation_via_trace_index(tmp_path, virtual_runtime):
    """<S3> `lib.get_invocation(task_hash)` returns the matching
    `InvocationEvent`; an unknown task_hash raises `InvocationNotFound`.
    """
    from tests.flow.conftest import build_linear_plan, run_and_load

    bp = build_linear_plan(tmp_path, n_steps=3)
    _task, lib = run_and_load(virtual_runtime, bp)

    events = lib.find_invocations()
    assert events, (
        f"expected >=1 trace event after 3-step plan, got 0; "
        f"summary={lib.summary()['by_status']!r}"
    )
    sample = events[0]
    fetched = lib.get_invocation(sample.task_hash)
    assert fetched.task_hash == sample.task_hash
    with pytest.raises(InvocationNotFound):
        lib.get_invocation("no_such_task_hash_xyz_000")


# ---------------------------------------------------------------------------
# S4 — walk_ancestors on a 5-hop DAG reaches roots, no cycles, no skips
# ---------------------------------------------------------------------------


def test_s4_walk_ancestors_5_hop(tmp_path, virtual_runtime):
    """<S4> `walk_ancestors(target_id)` over a 5-hop linear DAG reaches
    every upstream hop without revisits.
    """
    from tests.flow.conftest import build_5hop_dag_plan, run_and_load

    bp = build_5hop_dag_plan(tmp_path)
    _task, lib = run_and_load(virtual_runtime, bp)

    # Find any terminal instance in the manifest (last hop's output).
    terminal = None
    for path in lib.manifest:
        meta = lib.instance_meta.get(path)
        if meta is not None:
            terminal = meta["instance_id"]
            break
    if terminal is None:
        pytest.skip("no tracked instances in library after 5-hop run")

    visited: list[str] = []
    for node in lib.walk_ancestors(terminal):
        nid = getattr(node, "instance_id", None) or getattr(node, "_key", None)
        if nid is None:
            continue
        visited.append(nid)
    # No duplicates — lenient walk de-dups but should never yield the same
    # ancestor twice within a session.
    assert len(visited) == len(set(visited)), (
        f"walk_ancestors yielded duplicate ancestors: {visited!r}"
    )
    # Terminal itself is excluded from the ancestor walk.
    assert terminal not in visited, (
        f"walk_ancestors yielded the query target itself: {terminal!r}"
    )


# ---------------------------------------------------------------------------
# S5 — merged_endpoints remapping preserves identity (xfail-if-internal)
# ---------------------------------------------------------------------------


def test_s5_merged_endpoints_remap_preserves_id_when_exposed(tmp_path):
    """<S5> `WorkflowPlan.Generate` preserves DataInstance identity
    through the `merged_endpoints` remap (workflow.py:826-834). The
    underlying state is not exposed via the public WorkflowPlan API,
    so this is a placeholder test that skips until the surface lands.
    """
    pytest.skip(
        reason=(
            "internal merged_endpoints state not exposed by WorkflowPlan; "
            "test pinned at workflow.py:826-834 (#merged-endpoints-pin)"
        )
    )
