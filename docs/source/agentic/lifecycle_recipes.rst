Lifecycle Recipes
############################################################

Short, concrete patterns for running workflows from MCP.

Detached run with sentinel wait
============================================================

``run_workflow`` returns as soon as the launcher detaches under
``nohup``. To avoid racing past it, capture an mtime stamp first and
pass it to ``wait_for_workflow`` so the wait is not fooled by a
previous run's "run completed at" line.

1. ``run_workflow`` → starts the detached driver
2. ``wait_for_workflow`` with ``timeout_s`` and (optionally) the
   workflow start mtime → blocks until the sentinel appears
3. ``tail_workflow_log(source="agent", lines=50)`` → final agent
   output for context

The wait returns ``status="completed"``, ``"timeout"``, ``"missing"``,
or ``"errored"``. ``"errored"`` means ``PID.lock`` disappeared without
the sentinel — usually a nextflow crash.

Cancel and restage
============================================================

To stop a run cleanly:

1. ``cancel_workflow`` → removes ``workspace/PID.lock``; the launcher
   loop observes the absence and ``kill $PID; wait $PID`` on
   nextflow. Falls back to ``pkill`` if the lock is gone but the
   driver is still alive.
2. ``stage_workflow`` with ``on_exist="update_workflow"`` → push only
   the workflow scripts again (preserves staged inputs)
3. ``run_workflow`` to relaunch

Collect to a remote destination
============================================================

The result source the agent returns is typically a Globus or local
path. ``collect_results`` accepts any URI ``parse_source`` understands
— ``ssh://``, ``http(s)://``, ``globus://``, or a local path.

.. code-block:: json
    :caption: collect_results

    {
      "agent_name": "smith",
      "task_key": "<task_key>",
      "dest_uri": "ssh://workstation/data/results/run42"
    }

Poll without holding state
============================================================

For long jobs where the MCP session may reconnect, the workspace
caches everything under a ``task_key``. After reconnect:

1. ``server_status`` to confirm workspace path
2. ``list_workflow_tasks`` to see all cached tasks
3. ``list_workflow_runs(agent_name, task_key)`` for run indices
4. ``tail_workflow_log`` and ``check_workflow`` to inspect

No state is held in the MCP session that cannot be recovered from
``--workspace``.
