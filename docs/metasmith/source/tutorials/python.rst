.. role:: python(code)
    :language: python

Python basics
############################################################

The tutorials are written as Jupyter notebooks for interactive exploration,
but everything in them also works as plain :python:`.py` scripts. This page
collects the one piece that differs between the two modes: waiting for a
workflow to finish.

Waiting for workflow completion
============================================================

:python:`Agent.RunWorkflow` launches Nextflow under :python:`nohup ... &` on
the agent side and returns as soon as the launch script exits. In a Jupyter
notebook that is exactly what you want — you advance the next cell once the
log output settles down. In a plain Python script the next line of code runs
immediately, so calls like :python:`GetResultSource` or :python:`CheckWorkflow`
will race past the still-running workflow and fail with errors such as:

.. code-block:: text

    FileNotFoundError: '/.../runs/<key>/_metasmith/logs.<ts>/main.log'
    AssertionError: path [.../runs/<key>/results] does not exist

When the Nextflow process exits the agent writes a single sentinel line to
:python:`agent.log` inside the latest log directory of the task:

.. code-block:: text

    runs/<key>/_metasmith/logs.<latest>/agent.log
        ...
        run completed at [<timestamp>]

A small helper that polls for that sentinel is enough to convert any tutorial
into a plain Python script:

.. code-block:: python
    :linenos:
    :caption: wait_for_run helper

    import time
    from pathlib import Path

    def wait_for_run(task_dir: Path, poll_s: float = 10.0, timeout: float | None = None) -> bool:
        """Block until the agent writes 'run completed at' to the latest agent.log."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while deadline is None or time.monotonic() < deadline:
            logs = sorted((task_dir / "_metasmith").glob("logs.*"))
            if logs:
                agent_log = logs[-1] / "agent.log"
                if agent_log.exists() and "run completed at" in agent_log.read_text():
                    return True
            time.sleep(poll_s)
        return False

Use it around :python:`RunWorkflow`:

.. code-block:: python
    :linenos:
    :caption: Driving a tutorial from a plain Python script

    smith.RunWorkflow(task, config_file=smith.GetNxfConfigPresets()["local"])

    task_dir = Path(agent_home.GetPath()) / "runs" / task.GetKey()
    if not wait_for_run(task_dir, poll_s=10):
        raise RuntimeError("workflow did not complete in time")

    results_path = smith.GetResultSource(task).GetPath()
    results = DataInstanceLibrary.Load(results_path)

The helper works whether the agent home is local or remote — for remote agents
the :python:`task_dir` resolves to the mounted/synced view of the run
directory exposed by the home :python:`Source`.

.. note::

    A first-class :python:`wait=True` kwarg (or :python:`WaitForRun(task)`
    method) on :python:`Agent.RunWorkflow` is planned for a future release.
    Until then, use the helper above.
