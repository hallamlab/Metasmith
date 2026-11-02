Lifecycle Recipes
############################################################

Short, concrete patterns for running workflows from the CLI.

Detached run with sentinel wait
============================================================

``metasmith workflow run`` returns as soon as the launcher detaches
under ``nohup``. To avoid racing past it, follow with
``metasmith workflow wait`` so the wait blocks until the
``run completed at`` sentinel appears (and is not fooled by a previous
run's sentinel line).

1. ``metasmith workflow run AGENT TASK`` → starts the detached driver
2. ``metasmith workflow wait AGENT TASK --timeout S`` → blocks on the
   sentinel
3. ``metasmith workflow tail AGENT TASK --source agent --lines 50`` →
   final agent output for context

The wait returns ``status="completed"``, ``"timeout"``, ``"missing"``,
or ``"errored"``. ``"errored"`` means ``PID.lock`` disappeared without
the sentinel — usually a nextflow crash.

Cancel and restage
============================================================

To stop a run cleanly:

1. ``metasmith workflow cancel AGENT TASK`` → removes
   ``workspace/PID.lock``; the launcher loop observes the absence and
   ``kill $PID; wait $PID`` on nextflow. Falls back to ``pkill`` if the
   lock is gone but the driver is still alive.
2. ``metasmith workflow stage AGENT TASK --on-exist update_workflow``
   → push only the workflow scripts again (preserves staged inputs)
3. ``metasmith workflow run AGENT TASK`` → relaunch

Collect to a remote destination
============================================================

The result source the agent returns is typically a Globus or local
path. ``metasmith workflow collect`` accepts any URI
``metasmith source parse`` understands — ``ssh://``, ``http(s)://``,
``globus://``, or a local path.

.. code-block:: bash

    metasmith workflow collect smith <task_key> \
      --dest ssh://workstation/data/results/run42

Poll without holding state
============================================================

For long jobs where the controlling shell may disconnect, the
workspace caches everything under a ``task_key``. After reconnect:

1. ``metasmith task list`` to see all cached tasks
2. ``metasmith workflow runs AGENT TASK`` for run indices
3. ``metasmith workflow tail AGENT TASK`` and
   ``metasmith workflow check TASK`` to inspect

No state is held outside ``--workspace`` (and the agent's own
``runs/`` directory) that cannot be recovered.
