.. raw:: html

    <style>
        /* since no sidebars on homepage, reset this to default */
        .bd-main .bd-content .bd-article-container {
            max-width: 60em;
        }
    </style>

.. role:: python(code)
   :language: python

Overview
############################################################

Metasmith organizes bioinformatics data into machine-readable structures that enable the automated
generation of `Nextflow <https://www.nextflow.io/docs/latest/index.html>`_ workflows. Deployed agents
can then autonomously execute these workflows locally or on remote machines.

Usage in 5 steps
============================================================

Metasmith provides a python interface designed for `Jupyter notebooks <https://jupyter.org/>`_.

1 - Deploy an agent
-----------------------------------------------------------

Metasmith itself is not an agent. Rather, Metasmith spawns and deploys agents to machines on which
you intend to execute workflows. Agent deployment is automated; you do not need to install Metasmith
again if executing workflows on remote machines.

.. code-block:: python
    :linenos:

    smith = Agent(...)  # spawn an agent called "smith"
    smith.Deploy()      # deploy agent smith

2 - Register inputs
-----------------------------------------------------------

Metasmith uses a type system to model how data can be consumed by protocols. 
Inputs must be registered by assigning a type to them.

.. code-block:: python
    :linenos:

    inputs = DataInstanceLibrary(...)   # create a managed folder for inputs
    inputs.AddItem(                     # register an input file...
        path="/path/to/genome.fasta",   # ...from this file path
        dtype="sequences::genome"       # ...as a genomic sequence
    )

3 - Generate workflow
-----------------------------------------------------------

.. code-block:: python
    :linenos:

    task = smith.GenerateWorkflow(
        samples=inputs.AsSamples(),                     # for each of these inputs...
        resources=resources,                            # ...and these supplementary files
        transforms=transforms,                          # ...using these tools
        targets=[inputs.GetType("pangenome::heatmap")]  # make a figure after performing pangenome analysis
    )

The :python:`samples` format is a convenience for repeating the workflow across a batch of inputs.
Each :python:`sample` is guaranteed to be processed while inputs given as :python:`resources` will only be used if necessary.

.. tip::
    More on
    
    - `Transforms, and TransformInstances, and TransformInstanceLibraries <usage/transforms.html>`_
    - `generating workflows <usage/workflow_generation.html>`_
    
    Browse the `standard library <workflows/_index.html>`_ of transforms.

4 - Execute workflow
-----------------------------------------------------------

We can prepare an agent to execute the generated workflow by asking it to stage the input
:python:`DataInstances` and required :python:`TransformInstances` on to the machine it is deployed to.
Staging also translates the workflow into a syntax that Nextflow can execute. After the workflow is staged,
the agent can be asked to execute the workflow using Nextflow. Software dependencies for each tool is provided
as input :python:`DataInstances` such as containers or executable binaries.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task)
    smith.RunWorkflow(task)
    smith.CheckWorkflow(task)

.. tip::
    More on `executing workflows, configurations, and expected results <usage/workflow_execution.html>`_

5 - Receive outputs
-----------------------------------------------------------

Successful execution of a workflow produces a :python:`DataInstanceLibrary` that can be directly
used as the input to another workflow or downloaded from the deployed agent for interpretation.

.. code-block:: python
    :linenos:

    outputs = DataInstanceLibrary.LoadFrom(
        src=smith.GetResultSource(task),
        dest="/where/results/will/be/downloaded/to",
    )

Getting started
-----------------------------------------------------------

After `installing Metasmith <setup/install.html>`_, try 
`this tutorial <tutorials/deploying_locally.html>`_ for a basic demo of the 5 steps described above.

Documentation
############################################################

.. toctree::
    :maxdepth: 2
    
    setup/_index
    tutorials/_index
    usage/_index
    workflows/_index
