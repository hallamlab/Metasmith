Nextflow integration
############################################################

Metasmith generates a Nextflow workflow per task and runs it via Nextflow's
own executor (`local`, `slurm`, `k8s`, …). Resume / cache semantics are
**owned by Metasmith**, not by Nextflow — every Nextflow run looks fresh
from the JVM's perspective. A lineage-addressed task cache lives in
``<agent_home>/task_cache/``; the compile-time pass decides per-step
hit/miss, and a post-execution promote pass deposits outputs atomically.

What the cache covers
============================================================

Cache identity is **provenance, not bytes**. A transform invocation's
key is::

    key = multihash(blake3, canonical_cbor({
        transform_key,
        signature,
        [sorted (slot_key, input_instance_id)],
    }))

No file bytes are read for identity. Mutating any component — the
transform definition, the container input, an upstream lineage id —
invalidates downstream entries. This means a re-run that reproduces the
same lineage chain hits the cache cross-DAG: a DB-download transform
that ran in one workspace will short-circuit in another as long as the
input identities (typically: leaf inputs and the container image)
resolve to the same instance_ids.

Two sources of identity:

- **Leaf** (``origin="leaf"``) — minted by ``DataInstanceLibrary.AddItem``
  and re-derived at staging time by ``restat_leaf_ids``.
  **Stat-addressed** (``multihash("stat" ‖ abspath ‖ mtime_ns)``): one
  stat, no bytes read, so a 24 GB reference costs what a small file costs.
  Re-submitting the same files, unmodified, at the same paths therefore
  reuses cached results across runs. The absolute path belongs to the host
  that ran the stat, which is why staging re-derives: the client that
  registered a remote input cannot see it, and the agent that will read it
  can. Two hosts holding identical bytes at different paths do not agree,
  and an in-place edit that restores mtime is invisible — the same
  asymmetry ``models/libraries/pinned.py`` spells out. Falls back to a
  unique-per-call random id where nothing can stat the path. Force the
  legacy random id with ``METASMITH_LEAF_RANDOM=1``.
- **Lineage** (``origin="lineage"``) — set by the planner /
  post-execution promote: ``instance_id = lineage_key(...)`` over the
  transform's static metadata + input identities. Two workspaces running
  the same leaf-less transform produce the same output instance_id.
- **Imported** (``origin="imported"``) — carried through by
  ``msm data import-library`` (see below).

Per-transform ``cacheable``
============================================================

Every ``TransformInstance`` carries a ``cacheable: bool = True`` field.
Default-True means: any transform whose ``protocol`` is deterministic
(same inputs → same outputs) needs no annotation. Opt out by passing
``cacheable=False`` when you know the protocol is non-deterministic
(wall-clock sampling, RNG without a fixed seed, external state)::

    TransformInstance(
        protocol=protocol,
        model=model,
        group_by=dep,
        cacheable=False,  # this transform is intentionally non-deterministic
    )

A non-cacheable transform emits no ``publishDir`` directive to the cache
and writes no SQLite row, but its outputs still flow into downstream
lineage keys — the downstream entries' identity therefore captures the
fact that the non-deterministic step ran on a specific input chain.

Global kill-switch: ``METASMITH_CACHE=0`` disables probe + promote for
the entire run. Useful for forcing a known-good baseline run or for
diagnosing a cache-correlated bug without editing transforms.

.. warning::

   It has to be set in the environment of the **agent process**, not the
   client's. The reads that matter — the probe in
   ``models/workflow/cache_decisions.py`` and promote in
   ``agents/runner.py`` — happen inside the agent, and a containerised
   agent does not inherit the environment of whatever drove it. Exporting
   ``METASMITH_CACHE=0`` in the shell that calls ``RunWorkflow``, or in
   the agent's ``setup_commands``, was measured to leave the cache fully
   active (the run still logged ``cache probe matched N/M step(s)``).
   Both were tried against 0.20.1; neither worked, and no client-side
   spelling is currently known to. Until that is plumbed, the reliable
   levers are ``cacheable=False`` on the transform and ``msm cache gc``
   on the shard — and if you need certainty that a run used no cached
   result, check the agent log for the ``cache probe`` line rather than
   assuming the variable took effect.

Cross-workspace library import
============================================================

The ``msm data import-library`` op transfers a library from one
workspace into another and, in the destination, upserts every imported
``origin in {"lineage","imported"}`` DataInstance as an
``origin="imported"`` cache row. ``origin="leaf"`` entries are **not**
upserted — leaf identities are unique-per-AddItem and not
cache-meaningful::

    # In workspace A
    msm data save-as ./db.xgdb file:///shared/db_export

    # In workspace B
    msm data import-library file:///shared/db_export ./db.xgdb \
        --cache-root ~/.metasmith/agent_home/task_cache

A subsequent workflow in B that consumes the imported library will
cache-hit the import-bearing steps and execute only the downstream
deltas.

``msm cache`` and ``msm status``
============================================================

``msm cache list``
    Print every non-tombstoned cache entry (key, transform_key, origin,
    size, last_hit_at, hit_count). Pass ``--include-tombstoned`` to also
    list entries pending physical delete.

``msm cache gc --older-than SECONDS [--max-size BYTES] [--delete]``
    Two-phase garbage collector. Tombstones any entry whose
    ``last_hit_at`` is older than ``--older-than``, or LRU-tombstones
    enough entries to bring total size under ``--max-size``. By default
    tombstones only; pass ``--delete`` to also physically unlink any
    entry whose tombstone is past the 24h grace period
    (``--grace SECONDS`` to override).

``msm cache explain KEY``
    Decode the entry's CBOR manifest and surface its full lineage chain,
    transform_key, origin, and on-disk output_root.

``msm status <run_dir>``
    Join ``<run_dir>/_metasmith/trace.jsonl`` with
    ``workflow.step_N.meta`` files to render per-task status. Uses the
    stable step ordinal + cache_key — not Nextflow's volatile ``(N)``
    channel-arrival number.

Troubleshooting
============================================================

**``cache_root`` straddling mounts.** ``rename(<key>.tmp/, <key>/)`` is
only atomic on the same filesystem. The compile pass refuses to emit a
workflow whose ``cache_root`` and ``work_dir`` resolve to different
mounts (``/proc/self/mountinfo`` longest-prefix match). Move one of
the two roots so they share a mount.

**Network filesystem detected → ``mode: 'copy'``.** On Lustre, NFS,
GPFS, BeeGFS, etc., the ``publishDir`` directive emits ``mode: 'copy'``
rather than ``'link'`` — hardlinks across the same mount are still
non-atomic on most network filesystems. Override explicitly via
``params.metasmith_cache_strategy``.

**Emergency off.** ``METASMITH_CACHE=0`` (env) bypasses the probe pass
and skips post-exec promote. The on-disk ``task_cache/`` is untouched.

**Stable identifiers.** Nextflow's ``(N)`` ordinal in process names
reflects channel-arrival order and shifts across re-runs; use
``msm status`` for resume-stable per-task identifiers (step order +
cache_key).
