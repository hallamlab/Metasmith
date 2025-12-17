Tutorial
############################################################

.. role:: python(code)
   :language: python

.. _quickstart:

.. note::

    `Looking for install instructions? <install.html>`_
    
    `More workflow examples here. <../modules/index.html>`_

Genomics Annotation
============================================================
This section will demonstrate a typical protocol to generate and run a workflow with Metasmith
for genomics annotation.

Metasmith comes bundled with a jupyter lab. Let's start it now.

.. code-block:: bash

    msm lab 

In the output, look for a link that starts with ``http://127.0.0.1:8080/lab/...``, this will take you
to the jupyter lab UI, where a starter notebook will be waiting for you.

To begin, import the tools and structures needed from the Metasmith python API. We will also load some resources from the standard library using `Std()`.

.. code-block:: python
    :linenos:

    from pathlib import Path
    from metasmith.python_api import Agent, Source, Std, DataInstanceLibrary
    from metasmith.python_api import Resources, Size, Duration

    dtypes, containers, transforms = Std()

- :python:`dtypes` is a :python:`DataTypeLibrary`, `more here <data.html#data-types>`_
- :python:`containers` is a :python:`DataInstanceLibraries`, `more here <data.html#data-instances>`_
- :python:`transforms` is a :python:`TransformInstanceLibrary`, `more here <transforms.html>`_

Metasmith executes workflows through agents on your behalf. Each agent is given a workspace. Let's make one called "smith".

.. code-block:: python

    smith = Agent(
        home = Source.FromLocal(Path("./local_home").resolve()),
    )
    smith.Deploy()

.. note::
    `The location can be remote. <data.html#logistics>`_

For this demo, we will use long reads from the model organism *Eschichia coli* EPI300, but we only have its SRA accession "SRR35110061". For now, all inputs must be files so let's create one with the EPI300 accession number.

.. code-block:: python

    inputs_folder = Path("./std_assembly_data")
    inputs_accession_file = Path("./epi300.acc")
    with open(inputs_accession_file, "w") as f:
        f.write("SRR35110061")

We need to register the input into Metasmith's ecosystem by givging it a datatype. This lists all data types with "accession" in its name.

.. code-block:: python

    for k in dtypes.types:
        if "accession" not in k: continue
        print(k)

Using the ``long_reads_accession`` datatype, create a ``DataInstanceLibrary``. This structure keeps track of multiple files and is essentially a filesystem folder managed by Metasmith.
`More on DataInstanceLibrary usage here <data.html#data-instances>`_

.. code-block:: python
    :linenos:

    inputs = DataInstanceLibrary(inputs_folder)
    inputs.AddItem(inputs_accession_file.resolve(), "std::long_reads_accession")
    inputs.Save()


Let's see what annotations are available. We will use "busco_annotations" since it will be the fastest to process.

.. code-block:: python
    :linenos:

    for k in dtypes.types:
        if "annotations" not in k: continue
        print(k)

We can now ask the metasmith agent to generate a workflow that produces "busco_annotations" from a "long_reads_accession" using available resources and transform steps. The generated workflow with references to requried inputs are stored in `task`.  

.. code-block:: python
    :linenos:

    task = smith.GenerateWorkflow(
        samples=[inputs],
        resources=[containers],
        transforms=[transforms],
        targets=[
            dtypes["busco_annotations"],
        ],
    )

The generated plan can be viewed with graphviz. We should inspect it and ensure it is sensible.

.. code-block:: python
    :linenos:

    from IPython.display import Image
    dagf = Path("dag")
    task.plans[0][0].RenderDAG(dagf, format="png")
    Image(filename=f"{dagf}.png")

.. image:: /_static/example_metagenomics_dag.svg
   :align: center

|

Staging the task transfers required files over to the agent's workspace.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")

Before starting the run, let's alter some resource constraints and specify the local executor.

.. code-block:: python
    :linenos:

    for path, tr in transforms.IterateTransforms():
    print(tr.name)

.. code-block:: python
    :linenos:

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        resource_overrides={
            "all": Resources(
                cpus=8,
            ),
            transforms["busco_ref"]: Resources(
                cpus=2,
            ),
            transforms["fasterq_long"]: Resources(
                cpus=2,
            ),
        }
    )

The task will execute asynchronously and its progress can be checked with:

.. code-block:: python
    :linenos:

    smith.CheckWorkflow(task)

.. note::

    Learn how to run `available standard analyses here. <../modules/index.html>`_
