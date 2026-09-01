# Re-deriving leaf identity on the host that owns the files.
#
# A leaf id is the file's path and mtime. Where the row was registered decides
# who could mint it: the client owns the libraries it builds and stats those
# itself, but it cannot stat an input that lives on the agent's host, and falls
# back to a fresh random id per registration. Staging is the first moment those
# files and the code are on the same machine, so it is where the second kind of
# identity settles: `StageWorkflow` calls this before compiling, and the cache
# keys the codegen bakes into the `.nf` are built from whatever ids the plan
# carries by then.
#
# **Only the second kind.** An input the client staged out of its own library
# already has an identity the client derived from its own copy, and that reading
# is plan-independent. The staged copy is not: it lives under `runs/<task key>/`,
# a directory named after the whole plan, so re-deriving there answers with a
# different id for the same bytes as soon as any step in the workflow moves.
# That is what made adding one sample re-run every sample already done. So this
# pass skips an input that is library-owned and whose library was not itself
# fetched from somewhere else, and touches the rest.
#
# Both halves of that condition carry weight. A library loaded as an index-only
# image (`remote_src` set) has the manifest and none of the files, so its
# library-owned entries really are placeholders -- and the agent materialises
# such a library into `<home>/data/`, which is not named after the plan, so
# re-deriving there is stable.
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


def _client_owned(inst) -> bool:
    # True when the client both had this file and staged it here, so the id it
    # arrived with is the authoritative one. A relative path is one the library
    # resolves, and a library with no `remote_src` is one the client built rather
    # than pulled as a bare index.
    return not inst.path.is_absolute() and inst.parent_lib.remote_src is None


def restat_leaf_ids(task) -> dict:
    # Re-mint the leaf ids this host is the authority for, in place.
    #
    # The derivation is a pure function of (absolute path, mtime), so the several
    # DataInstance objects that name one file -- `plan.given` and each step's
    # `dependency_map` hold separate objects joined only by id -- land on the same
    # new id without a translation table.
    if os.environ.get("METASMITH_LEAF_RANDOM"):
        return {"restated": 0, "kept": 0, "unreachable": [], "absent": []}

    restated = 0
    kept = 0
    unreachable: list[str] = []
    absent: list[str] = []
    for inst in _leaf_inputs(task):
        lib = inst.parent_lib
        if lib.is_pinned:
            # A pinned library's recorded ids are the contract downstream cache
            # keys were built from; see libraries/pinned.py.
            continue
        if _client_owned(inst):
            kept += 1
            # An id the client minted without being able to stat the file is a
            # random placeholder, and it is indistinguishable from a real one
            # here -- both are a multihash of the same width. What IS visible is
            # a file that is not on this host, which never reuses anything and is
            # about to fail the run, so report that much.
            if not inst.ResolvePath().exists():
                absent.append(str(inst.ResolvePath()))
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
        # The library's own record moves with the plan's, but only the plan is
        # written back to disk by the caller -- the staged index keeps the ids it
        # was staged with, and that is what names its directory.
        meta = lib.instance_meta.get(inst.path)
        if meta is not None and meta.get("origin", "leaf") == "leaf":
            meta["instance_id"] = new
        restated += 1

    unreachable = sorted(set(unreachable))
    absent = sorted(set(absent))
    if restated or kept:
        Log.Info(
            f"leaf identity: [{kept}] kept as the client registered them,"
            f" [{restated}] re-derived from this host's files"
        )

    def _report(paths: list[str], msg: str):
        _shown = "\n".join(f"    {p}" for p in paths[:5])
        _more = f"\n    ... and {len(paths)-5} more" if len(paths) > 5 else ""
        Log.Warn(f"{msg}:\n{_shown}{_more}")

    if unreachable:
        _report(unreachable, (
            f"could not stat [{len(unreachable)}] input(s) from here, so they keep"
            f" the identity they were registered with and will not reuse a cached"
            f" result"
        ))
    if absent:
        _report(absent, (
            f"[{len(absent)}] library input(s) are not on this host; they keep the"
            f" identity the client registered and nothing here can check them"
        ))
    return {
        "restated": restated,
        "kept": kept,
        "unreachable": unreachable,
        "absent": absent,
    }
