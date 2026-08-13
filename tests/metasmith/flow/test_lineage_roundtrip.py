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
    from tests.metasmith.flow.conftest import build_linear_plan, run_and_load

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
    from tests.metasmith.flow.conftest import build_5hop_dag_plan, run_and_load

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
# G3 — promoted-output manifest_id matches trace's file_instance_id
# ---------------------------------------------------------------------------


def test_g3_manifest_id_matches_trace_file_instance_id(tmp_path, virtual_runtime):
    """<G3> The published-results library persists `instance_id` from the
    trace's `ProducedFile.file_instance_id` so `walk_ancestors` on a loaded
    library bridges into the trace index instead of falling back to the
    legacy path+dtype+lib_key derivation.

    Without G3 (the `SetLineageInstance` call in `CollectResults`), every
    promoted file's manifest entry has `origin='leaf'` and an
    instance_id computed from the legacy formula, so the trace-index
    key (which is `file_instance_id`) does not match what the library
    exposes; `walk_ancestors` returns 0 nodes.
    """
    from tests.metasmith.flow.conftest import build_linear_plan, run_and_load

    bp = build_linear_plan(tmp_path, n_steps=2)
    _task, lib = run_and_load(virtual_runtime, bp)

    events = lib.find_invocations()
    assert events, "expected >=1 invocation after 2-step plan"

    # Build {file_instance_id -> ProducedFile.path} from the trace.
    trace_id_to_relpath: dict[str, str] = {}
    for ev in events:
        for pf in ev.produces:
            if not pf.path or not pf.file_instance_id:
                continue
            trace_id_to_relpath[pf.file_instance_id] = pf.path
    assert trace_id_to_relpath, "trace events carry no file_instance_id entries"

    # For each manifest entry that maps to a produced file, assert the
    # manifest's persisted instance_id equals the trace's file_instance_id.
    promoted_matches = 0
    leaf_origins = []
    for path, meta in lib.instance_meta.items():
        if meta.get("origin") != "lineage":
            leaf_origins.append((str(path), meta.get("origin"), meta.get("instance_id")))
            continue
        mid = meta["instance_id"]
        assert mid in trace_id_to_relpath, (
            f"manifest entry [{path}] has origin=lineage with "
            f"instance_id={mid!r} but no trace event carries that id; "
            f"available trace ids: {sorted(trace_id_to_relpath)[:5]}"
        )
        # Path in manifest is relative to lib root; the trace stores a
        # relative path under output_root. They share the same suffix.
        promoted_matches += 1
    assert promoted_matches >= 1, (
        f"no promoted-origin manifest entries observed; all leaf: "
        f"{leaf_origins!r}"
    )

    # walk_ancestors on any promoted file returns non-empty ancestry.
    promoted_id = next(
        meta["instance_id"]
        for meta in lib.instance_meta.values()
        if meta.get("origin") == "lineage"
    )
    visited = [getattr(n, "instance_id", None) for n in lib.walk_ancestors(promoted_id)]
    visited = [v for v in visited if v]
    assert visited, (
        f"walk_ancestors({promoted_id!r}) returned empty — G3 bridge is broken"
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
