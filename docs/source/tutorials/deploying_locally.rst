.. role:: python(code)
   :language: python

My first agent
############################################################

This tutorial will demonstrate a minimal use case for running analyses with Metasmith.

Prerequisites
============================================================

- `Metasmith is installed </setup/install.html>`_
- `Docker or Apptainer is installed </setup/deployment.html#Locally>`_, since we will be deploying an agent locally

Setup
============================================================

.. code-block:: console
    :caption: Terminal

    $ msm lab --tutorial deploying_locally

.. button-link:: http://127.0.0.1:8080
    :color: primary
    
    **Connect to Jupyter Lab**

.. code-block::

    example_resources/
    └── tutorials/
        └── deploying_locally.ipynb

.. code-block:: python
    :caption: Jupyter
    :linenos:

    from pathlib import Path
    from metasmith.python_api import Agent, ContainerRuntime
    from metasmith.python_api import DataTypeLibrary, DataInstanceLibrary, TransformInstanceLibrary
    from metasmith.python_api import Source, SshSource, HttpSource, Logistics
    from metasmith.python_api import Resources, Size
    from metasmith.python_api import ipynbButtonLink

    WORKSPACE = Path("../../").resolve() # back twice since we are in example_resources/tutorials
    WORKSPACE

1 - Deploy an agent
============================================================

.. code-block:: python
    :linenos:

    agent_home = Source.FromLocal(WORKSPACE/"msm_home")
    smith = Agent(
        home = agent_home,
        runtime=ContainerRuntime.DOCKER,
    )

    smith.Deploy()

2 - Register inputs
============================================================

.. code-block:: python
    :linenos:

    local_input_file = WORKSPACE/"epi300.gbk"

    mover = Logistics()
    mover.QueueTransfer(
        src=HttpSource(url="https://github.com/hallamlab/MetasmithLibraries/releases/download/data.epi300.1/epi300.gbk").AsSource(),
        dest=Source.FromLocal(local_input_file),
    )
    mover.ExecuteTransfers()

.. code-block:: python
    :linenos:

    MLIB = WORKSPACE/"MetasmithLibraries"
    CACHE = WORKSPACE/"cache"
    in_dir = CACHE/"inputs/pangenome3.xgdb"

    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()
    inputs.AddTypeLibrary("ncbi", DataTypeLibrary.Load(MLIB/"data_types/ncbi.yml"))
    inputs.AddTypeLibrary("sequences", DataTypeLibrary.Load(MLIB/"data_types/sequences.yml"))
    inputs.AddTypeLibrary("pangenome", DataTypeLibrary.Load(MLIB/"data_types/pangenome.yml"))

    group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
    inputs.AddValue("DH10b", "GCF_000019425.1", "ncbi::accession", parents={group})
    inputs.AddValue("K12", "GCF_000005845.2", "ncbi::accession", parents={group})
    inputs.AddItem(WORKSPACE/"epi300.gbk", "sequences::gbk", parents={group})
    inputs.LocalizeContents()
    inputs.Save()

3 - Generate workflow
============================================================

.. code-block:: python
    :linenos:

    resources = [
        DataInstanceLibrary.Load(MLIB/f"resources/{n}")
        for n in ["containers", "lib"]
    ]

    transforms = [
        TransformInstanceLibrary.Load(MLIB/f"transforms/{n}")
        for n in ["logistics", "pangenome"]
    ]

    task = smith.GenerateWorkflow(
        samples=inputs.AsSamples(),
        resources=resources,
        transforms=transforms,
        targets=[inputs.GetType("pangenome::heatmap")]
    )

.. code-block:: python
    :linenos:

    print(f'generated plan has [{len(task.plan.steps)}] steps')

    workflow_dag = task.plan.RenderDAG(CACHE/f"{task.GetKey()}.dag.svg")
    url = f'../../{workflow_dag.relative_to(WORKSPACE)}'
    ipynbButtonLink(url, "view workflow diagram")

4 - Execute workflow
============================================================

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")

.. code-block:: python
    :linenos:

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        resource_overrides={
            "all": Resources(
                memory=Size.GB(2),
            )
        }
    )

.. code-block:: python
    :linenos:

    smith.CheckWorkflow(task)

5 - Receive outputs
============================================================

.. code-block:: python
    :linenos:

    results_path = smith.GetResultSource(task).GetPath()
    results = DataInstanceLibrary.Load(results_path)

.. code-block:: python
    :linenos:

    to_show = [
        "_metadata/logs.latest/nxf_report.html",
        "_metadata/logs.latest/nxf_timeline.html",
    ] + [path for path, type_name, endpoint in results.Iterate()]

    for file in to_show:
        url = Path(results_url)/file
        ipynbButtonLink(f'{url}', f'view {url.parent.name}/{url.name}')
