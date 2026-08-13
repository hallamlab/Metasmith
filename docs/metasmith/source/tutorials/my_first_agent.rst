.. role:: python(code)
    :language: python

My first agent
############################################################

This tutorial will demonstrate the minimal steps for using Metasmith.
As an example, we will perform pangenome analysis on three E. coli genomes
identified by their NCBI assembly accessions.

The Jupyter notebook for this tutorial can be obtained by:

.. code-block:: bash
    :caption: Terminal

    $ msm get tutorials/my_first_agent.ipynb

Prerequisites
============================================================

- `Metasmith is installed <../setup/install.html>`_ along with either Docker or Apptainer, since we will be deploying an agent locally
- `A tutorial workspace has been setup for Jupyter notebooks <../setup/tutorials.html>`_

1 - Deploy an agent
============================================================

Agents, in this context, are virtual workers that perform complex tasks on the user's behalf.
To spwan an agent, Metasmith creates then deploys the agent to a home directory in which it will live.
For this tutorial, the agent will live locally (on the same machine that Metasmith is installed on). 

.. tip::

    More on `deploying agents <../setup/deployment.html>`_, including to remote machines

Metasmith outsources the steps that compose an overall analysis
pipeline to external software tools. To ensure that these tools can be reliably executed by an agent, 
self-contained software environments called "containers" are used. A container runtime downloads and manages
the lifetime of these containers. 

Metasmith can use the following container runtimes:

- :python:`DOCKER`
- :python:`APPTAINER`

Let's create an agent called :python:`smith` and give him a home in the current workspace under :python:`msm_home`.
We will instruct :python:`smith` to manage containers with :python:`DOCKER`.

.. code-block:: python
    :linenos:

    agent_home = Source.FromLocal(WORKSPACE/"msm_home")
    smith = Agent(
        home = agent_home,
        runtime=Runtime.DOCKER,
    )

    smith.Deploy()

2 - Register inputs
============================================================

Pangenome analysis seeks to compare a panel of genomes at the level of genes.
Three E. coli genomes will be used as input, identified by their NCBI assembly
accessions.

Metasmith accepts inputs in the form of files or values that become registered
as :python:`DataInstances` within a managed folder called a
:python:`DataInstanceLibrary`. Registering an input involves attaching a
:python:`DataType` that describes how it can be used. Computational steps
specify a :python:`DataType` for each of their inputs such that any
:python:`DataInstance` with a matching :python:`DataType` can be consumed.
Registering inputs as typed :python:`DataInstances` enables Metasmith to
determine which tools are capable of consuming them.

.. code-block:: python
    :linenos:

    in_dir = WORKSPACE/"3pangenome.xgdb"
    inputs = DataInstanceLibrary(in_dir)
    inputs.Purge()  # clear the input folder, in case this is not the first time this cell was ran
    inputs.AddTypeLibrary(MLIB/"data_types/ncbi.yml")
    inputs.AddTypeLibrary(MLIB/"data_types/sequences.yml")
    inputs.AddTypeLibrary(MLIB/"data_types/pangenome.yml")

    group = inputs.AddValue("pangenome", "e coli", "pangenome::pangenome")
    inputs.AddValue("DH10b",   "GCF_000019425.1", "ncbi::assembly_accession", parents={group})
    inputs.AddValue("K12",     "GCF_000005845.2", "ncbi::assembly_accession", parents={group})
    inputs.AddValue("EPI300",  "GCF_049667475.1", "ncbi::assembly_accession", parents={group})
    inputs.Save()

.. note::

    The concrete subtype :python:`ncbi::assembly_accession` is required here —
    the more general :python:`ncbi::accession` is abstract and will not match
    :python:`getNcbiAssembly`'s input contract.

    If you would rather start from a local :python:`.gbk` file you have on
    disk, you can register it directly as :python:`sequences::gbk` with
    :python:`inputs.AddItem(local_path, "sequences::gbk", parents={group})`.
    Be aware that when an input already satisfies a downstream tool's
    requirement, the planner will not schedule an upstream fetch for it — so
    mixing one local genome with two accessions will produce a workflow with
    a single-genome ppanggolin step rather than a three-genome one. Use
    accessions for all inputs, or local files for all inputs, when you want
    every genome to participate.

.. tip::

    More on `DataTypes, DataInstances, and DataInstanceLibraries <usage/data.html>`_

    Using a :python:`try/except` block to load the input library if is already created (instead of creating it each time)
    will enable automatic caching mechanisms in step 4 to reduce redundant computation:

    .. code-block:: python
        :linenos:

        in_dir = Path(WORKSPACE"/3pangenome.xgdb")
        try:
            inputs = DataInstanceLibrary.Load(in_dir)
        except:
            inputs = DataInstanceLibrary(in_dir)
            inputs.Purge() # just to be safe
            inputs.AddTypeLibrary(...)
            ...
            inputs.AddValue(...)
            ...

3 - Generate workflow
============================================================

Since computational steps transform input :python:`DataInstances` into output :python:`DataInstances`,
they are called :python:`TransformInstances` and are organized into special :python:`DataInstanceLibraries`
called a :python:`TransformInstanceLibrary`. :python:`TransformInstances` can be chained into workflows by
matching the :python:`DataType` of the upstream output to the :python:`DataType` of the downstream input.
The protocol of a :python:`TransformInstance` is called a :python:`Transform` and it may appear multiple times
within a workflow.

When given a :python:`DataInstanceLibrary` of inputs, a :python:`TransformInstanceLibrary` of
available tools, and target :python:`DataTypes`, the agent can generate a workflow to produce
:python:`DataInstances` that match the target :python:`DataTypes`, as long as a solution exists.
Here, we request that targets of the type :python:`pangenome::heatmap` be produced.

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

    targets = TargetBuilder()
    targets.Add("pangenome::heatmap")

    task = smith.GenerateWorkflow(
        # divide the inputs into samples
        # we want all targets to be produced from each sample
        samples=inputs.AsSamples("ncbi::assembly_accession"),
        resources=resources,    # these are available for each sample, but need not be used
        transforms=transforms,
        targets=targets,
    )

.. tip::

    More on `Transforms, and TransformInstances, and TransformInstanceLibraries <../usage/transforms.html>`_

Each :python:`task` has a code name or :python:`key` composed of case sensitive letters and numbers that is calculated from the inputs
and workflow steps. A :python:`task` contains all the context required to execute a workflow.

.. code-block:: python
    :linenos:
    print(f'this workflow is called [{len(task.GetKey())}]')

The steps of the workflow can be rendered as a directed acyclic graph (DAG) or more commonly known as a flowchart.
A DAG is a specific type of flowchart that has 2 properties:

- "directed" indicates that for any two connected steps, data always flows from one to the other and never in reverse.
- "acyclic" promises an implicit ording of steps such that once a step is performed, it will never be needed again.

Let's take a look at the DAG for this generated workflow.

.. code-block:: python
    :linenos:

    print(f'generated plan has [{len(task.plan.steps)}] steps')

    workflow_diagram_path = f"{task.GetKey()}.dag.svg"
    task.plan.RenderDAG(workflow_diagram_path)
    print(f'diagram at [{workflow_diagram_path}]')

    ipynbButtonLink(f"{workflow_diagram_path}", text="view workflow diagram")

.. figure:: /_static/dag_pangenome.svg
   :align: center
   :width: 70%
   :alt: the generated pangenome workflow

.. important::

    Notice how the :python:`ncbi::assembly_accession` inputs are automatically "transformed" into :python:`sequences::gbk` files
    by :python:`getNcbiAssembly` to satisfy the input requirements of the pangenome analysis tool :python:`ppanggolin`.
    All data types (shown in boxes) are valid as inputs or targets. Try different targets:

    .. code-block:: python
        :linenos:

        targets = TargetBuilder()
        targets.Add("pangenome::ppanggolin_matrix")
        
        # or
        targets = TargetBuilder()
        targets.Add("sequences::orfs")

    Beware that there may not be a valid path for certain combinations of inputs and outputs, such as from
    :python:`sequences::gbk` to :python:`sequences::orfs`.

.. tip

    .. More on `generating workflows <../usage/workflow_generation.html>`_

4 - Execute workflow
============================================================

To execute the workflow, it must first be staged to the agent's home.
This involves sending over the inputs, transform protocols, and translated nextflow workflow definition.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")

Since the agent home is local, you can view the results directly in the panel on the left.

.. code-block::

    metasmith_ws/
    └──msm_home/
        ├── lib/
        ├── relay/
        ├── runs/
        │   └── ... # look for the tasks's key
        └── msm

A nextflow configuration is generated just before a run is triggered.
We will use the "local" preset and lower the memory requirement to 2GB for all steps.
The default resource estimates are liberal, but we know our task will only need to work with three genomes.

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

.. tip::

    More on `configuration <../usage/nextflow.html>`_
    
    - `Transforms, and TransformInstances, and TransformInstanceLibraries <../usage/transforms.html>`_
    - `generating workflows <../usage/workflow_generation.html>`_

.. note::

    Multiple runs can be triggered, but nextflow will fail if consecutive runs are triggered too soon.

.. note::

    :python:`RunWorkflow` returns as soon as Nextflow has been launched in the
    background. In a Jupyter notebook this is fine — you advance the next
    cell after the run finishes. If you are running this tutorial as a plain
    :python:`.py` script, see `Python basics <./python.html>`_ for a
    :python:`wait_for_run` helper that polls the agent log for the
    :python:`"run completed at"` sentinel before continuing.

Once a task is running, the main log output can be viewed like so:

.. code-block:: python
    :linenos:

    smith.CheckWorkflow(task)

.. note::

    The logs of the latest run will be shown by default. Older logs can be selected.
    The following selects the first run, regardless of how many there are in total.

    .. code-block:: python
        :linenos:

        smith.CheckWorkflow(task, run=1)


5 - Receive outputs
============================================================

We can ask for the location of a task from the agent and use it to load
the produced :python:`DataInstanceLibrary` that contains the workflow's outputs.

.. code-block:: python
    :linenos:

    results_path = smith.GetResultSource(task).GetPath()
    results = DataInstanceLibrary.Load(results_path)

Once loaded, we can iterate through the results to find the heatmap since there should only be one
output. We also make links to the main report files.

.. code-block:: python
    :linenos:

    ipynbButtonLink(results_path/"_metadata/logs.latest/nxf_report.html")
    ipynbButtonLink(results_path/"_metadata/logs.latest/nxf_timeline.html")

    for path, type_name, endpoint in results.Iterate():
        if path.is_absolute(): continue # inputs have absolute paths
        ipynbButtonLink(results_path/path, f'view {type_name} {path.name}')

Next steps
============================================================

Other tutorials are available in the section panel on the left.
