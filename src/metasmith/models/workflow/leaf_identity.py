# Re-deriving leaf identity on the host that owns the files.
#
# A leaf id is the file's path and mtime, minted where the row was registered --
# the client, which cannot stat an input living on the agent's host and so falls
# back to a fresh random id per registration. Staging is the first moment the
# files and the code are on the same machine, so it is where identity settles:
# `StageWorkflow` calls this before compiling, and the cache keys the codegen
# bakes into the `.nf` are built from whatever ids the plan carries by then.
#
# **The caller must write the re-derived plan back to `task.yml`.**
# `CollectResults` reloads the task after the run and joins the trace's instance
# ids against the plan's; a plan on disk still carrying pre-restat ids makes that
# join raise on inputs the run legitimately read.
#
# A path this host cannot stat keeps the id it arrived with. Substituting a
# derivable id for one that cannot be derived is how a false cache hit is built.

from __future__ import annotations

import os

from ...logging import Log
from ..libraries.identity import stat_leaf_id
from ..paths import is_deferred


def _leaf_inputs(task):
    # The plan's given inputs, plus the step-side objects that name them.
    #
    # Membership is by id rather than by origin: a step's *produced* instances
    # also read `origin="leaf"` until `compute_cache_decisions` stamps them,
    # and their paths do not exist yet, so walking by origin would report every
    # output of every run as an input this host cannot see.
    given_ids: set[str] = set()
    for inst in task.plan.given:
        if inst.origin != "leaf" or is_deferred(inst.path):
            continue
        given_ids.add(inst.instance_id)
        yield inst
    for step in task.plan.steps:
        for insts in step.dependency_map.values():
            for inst in insts:
                if inst.instance_id in given_ids:
                    yield inst


def restat_leaf_ids(task) -> dict:
    # Re-mint every leaf id from this host's view of the file, in place.
    #
    # The derivation is a pure function of (absolute path, mtime), so the several
    # DataInstance objects that name one file -- `plan.given` and each step's
    # `dependency_map` hold separate objects joined only by id -- land on the same
    # new id without a translation table.
    if os.environ.get("METASMITH_LEAF_RANDOM"):
        return {"restated": 0, "unreachable": []}

    restated = 0
    unreachable: list[str] = []
    for inst in _leaf_inputs(task):
        lib = inst.parent_lib
        if lib.is_pinned:
            # A pinned library's recorded ids are the contract downstream cache
            # keys were built from; see libraries/pinned.py.
            continue
        abs_path = inst.ResolvePath()
        new = stat_leaf_id(abs_path, lib.fork_id)
        if new is None:
            unreachable.append(str(abs_path))
            continue
        if new == inst.instance_id:
            continue
        inst.instance_id = new
        inst._refresh_derived_keys()
        meta = lib.instance_meta.get(inst.path)
        if meta is not None and meta.get("origin", "leaf") == "leaf":
            meta["instance_id"] = new
        restated += 1

    unreachable = sorted(set(unreachable))
    if restated:
        Log.Info(f"re-derived [{restated}] leaf identities from this host's files")
    if unreachable:
        _shown = "\n".join(f"    {p}" for p in unreachable[:5])
        _more = f"\n    ... and {len(unreachable)-5} more" if len(unreachable) > 5 else ""
        Log.Warn(
            f"could not stat [{len(unreachable)}] input(s) from here, so they keep"
            f" the identity they were registered with and will not reuse a cached"
            f" result:\n{_shown}{_more}"
        )
    return {"restated": restated, "unreachable": unreachable}
