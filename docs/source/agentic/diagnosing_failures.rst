Diagnosing failures
############################################################

The solver and the runner both surface structured diagnostics. This
page documents the shapes a CLI caller can rely on.

Plan failures: ``hints``
============================================================

When ``metasmith plan`` cannot produce a complete plan, the response
is ``{"success": false, "step_count": <partial>,
"dropped_targets": [...], "hints": [...]}`` (use ``--json`` to capture
this structurally). Each hint has:

==========================  ============================================================
Field                       Meaning
==========================  ============================================================
``kind``                    ``unreachable_target`` | ``missing_input`` | ``lineage_mismatch``
``target``                  The target type (or step input) the hint is about
``message``                 Human-readable description of why the solver gave up
``chain``                   Reverse-BFS chain of unsatisfied requirements
``candidate_transforms``    Transform names that *could* fire if a requirement were met
``near_misses``             ``did-you-mean`` types ranked by property-Jaccard to givens
==========================  ============================================================

``missing_input`` hints are de-duped by demand shape and sorted by
similarity to your givens — the most actionable suggestion is first.

For a previously cached plan, ``metasmith task hints <task_key>``
returns the same array without re-running the solver.

Typical recoveries
------------------------------------------------------------

- **unreachable_target** with a non-empty ``candidate_transforms`` →
  one of these transforms would fire if you gave it the missing
  requirement listed in the chain. ``metasmith data add-value`` or
  ``metasmith data add-item`` for that requirement and re-run
  ``metasmith plan``.
- **missing_input** with strong ``near_misses`` → the requirement
  shape almost matches a registered type. Check whether you tagged
  inputs with the wrong type, or use
  ``metasmith type compat SRC TGT`` to confirm.
- **lineage_mismatch** → an item exists but its parent chain does not
  satisfy a transform's structural needs.
  ``metasmith data set-parents`` to fix the lineage.

Runtime failures
============================================================

After ``metasmith workflow run``, the agent log carries a single
sentinel line ``run completed at`` when the nextflow process exits
cleanly.

If ``metasmith workflow wait`` returns ``status="errored"``,
``PID.lock`` disappeared without the sentinel — typically a nextflow
crash. The ``tail`` field of the response is the last 20 lines of
``agent.log``; ``metasmith workflow tail AGENT TASK --source main
--lines 200`` will dump the nextflow stdout/stderr stream.

If ``status="timeout"``, the run is still going. Increase
``--timeout`` or call ``metasmith workflow wait`` again with the same
``task_key`` — the wait is idempotent.

If ``status="missing"``, the run directory was never created. Most
likely the launcher script itself failed; check
``metasmith workflow tail AGENT TASK --source agent --lines 20`` on
the closest run index, or ``metasmith workflow check TASK`` for the
full report.
