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

    smith = Agent(...)
    smith.Deploy()

2 - Register inputs
-----------------------------------------------------------

Metasmith uses a type system to model how data can be consumed by protocols. 
Inputs must be registered by assigning a type to them.

.. code-block:: python
    :linenos:

    inputs = DataInstanceLibrary(...) 
    inputs.AddItem(
        <file path>,
        <data type>,
    )

3 - Generate workflow
-----------------------------------------------------------

Protocols describe transformations between data types. By providing the inputs and a list of
available transformations, we can ask the agent to generate a workflow to produce target data types
by chaining multiple protocols together.

.. code-block:: python
    :linenos:

    task = smith.GenerateWorkflow(
        <inputs>,
        <transforms>,
        <targets>,
    )

.. tip::
    
    Browse the `standard library <workflows/_index.html>`_ of transforms.

4 - Execute workflow
-----------------------------------------------------------

Staging a workflow translates it into Nextflow's syntax and prepares default
configurations for various platforms. When ready, execution is delegated to Nextflow.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task)
    smith.RunWorkflow(task)
    smith.CheckWorkflow(task)

5 - Receive outputs
-----------------------------------------------------------

Produced outputs are presented as a structured data product, but exists as a simple folder
to maintain accessibility by both humans and machines.

.. code-block:: python
    :linenos:

    outputs = DataInstanceLibrary.LoadFrom(
        smith.GetResultSource(task),
        ...
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
