Diagnosing failures
############################################################

The solver and the runner both surface structured diagnostics. This
page documents the shapes an agent can rely on.

Plan failures: ``hints``
============================================================

When ``plan_workflow`` cannot produce a complete plan, the response is
``{"success": false, "step_count": <partial>, "dropped_targets": [...],
"hints": [...]}``. Each hint has:

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

For a saved plan, ``get_plan_hints(task_key)`` returns the same array
without re-running the solver.

Typical recoveries
------------------------------------------------------------

- **unreachable_target** with a non-empty ``candidate_transforms`` →
  one of these transforms would fire if you gave it the missing
  requirement listed in the chain. ``add_data_value`` or
  ``add_data_item`` for that requirement and replan.
- **missing_input** with strong ``near_misses`` → the requirement
  shape almost matches a registered type. Check whether you tagged
  inputs with the wrong type, or use ``check_type_compatibility`` to
  confirm.
- **lineage_mismatch** → an item exists but its parent chain does
  not satisfy a transform's structural needs. ``set_item_parents``
  to fix the lineage.

Runtime failures
============================================================

After ``run_workflow``, the agent log carries a single sentinel line
``run completed at`` when the nextflow process exits cleanly.

If ``wait_for_workflow`` returns ``status="errored"``, ``PID.lock``
disappeared without the sentinel — typically a nextflow crash. The
``tail`` field of the response is the last 20 lines of ``agent.log``;
``tail_workflow_log(source="main", lines=200)`` will dump the
nextflow stdout/stderr stream.

If ``status="timeout"``, the run is still going. Increase ``timeout_s``
or call again with the same ``task_key`` — the wait is idempotent.

If ``status="missing"``, the run directory was never created. Most
likely the launcher script itself failed; check
``tail_workflow_log(source="agent", lines=20)`` on whichever run
index is closest, or ``check_workflow`` for the full report.
