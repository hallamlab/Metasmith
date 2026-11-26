.. role:: python(code)
    :language: python

Python basics
############################################################

The tutorials are Jupyter notebooks, and everything in them also works as plain :python:`.py`
scripts. This page covers the two things a script does differently: waiting for a run to finish,
and starting from a shipped template instead of building inputs by hand.

Waiting for workflow completion
============================================================

:python:`Agent.RunWorkflow` launches Nextflow in the background on the agent and returns at once.
In a notebook you run the next cell when the log settles. In a script the next line runs
immediately, so a call such as :python:`GetResultSource` finds no results yet. Call
:python:`Agent.WaitForWorkflow` between the two.

.. code-block:: python
    :linenos:

    smith.RunWorkflow(task)
    result = smith.WaitForWorkflow(task, timeout_s=4 * 3600, poll_s=10)
    if result["status"] != "completed":
        raise SystemExit("\n".join(result["tail"]))
    results = DataInstanceLibrary.Load(smith.GetResultSource(task).GetPath())

:python:`result["status"]` is ``completed``, ``failed``, ``errored``, ``timeout`` or ``missing``.
:python:`result["tail"]` holds the last lines of the run's log. It works for local and remote agents.

.. _running-a-shipped-template:

Running a shipped template
============================================================

A template is the GUI's unit of work: a sample type, the targets and a frozen input library.
The installed package carries the standard library and its templates uncompiled. Copy them into
a writable folder first, the same copy ``msm gui`` and ``msm lab`` make.

1. Copy and compile the library with :python:`clone_stdlib`. It returns the path of
   ``MetasmithLibraries`` under the folder you give, and skips the copy when one exists.
2. Find the template with :python:`Template.Discover` and read its :python:`spec`.
3. Register your files and values with :python:`Agent.PoolGivens`, then build them into an input
   library.
4. Plan with :python:`GenerateWorkflow`, passing the spec's transform and resource libraries.
5. Stage, run and wait as above.

.. code-block:: python
    :linenos:

    from metasmith.python_api import Agent, DataInstanceLibrary, Runtime, Source
    from metasmith.python_api import Template, TargetBuilder, TransformInstanceLibrary
    from metasmith.gui.stdlib import clone_stdlib

    root = Path(clone_stdlib(work)["path"])
    (template,) = [t for t in Template.Discover(root) if t.name == "my_template"]
    spec = template.spec

    smith = Agent(home=Source.FromLocal(work/"msm_home"), runtime=Runtime.DOCKER)
    smith.Deploy()
    givens = smith.PoolGivens()
    givens.Add(my_file, spec.sample_type, name="sample_1")
    inputs = givens.Build(work/"inputs.xgdb", type_library_paths=[root/"data_types"/"my_types.yml"])

    targets = TargetBuilder()
    for t in spec.target_types:
        targets.Add(t)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples(spec.sample_type)),
        resources=[*(DataInstanceLibrary.Load(p) for p in spec.resource_libraries), inputs],
        transforms=[TransformInstanceLibrary.Load(p) for p in spec.transform_libraries],
        targets=targets,
    )
    assert task.ok, task.plan.dropped_targets

``research/geospatial_query/geoquery_run.py`` is a complete script in this shape. It adds one
value per source under the sample, and it publishes the results as CSV tables. Read
:doc:`../workflows/geoquery` for that template.
