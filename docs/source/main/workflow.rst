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
This section will show the minimal steps to generate and run a workflow with Metasmith
using example data and transforms themed after genomics annotation.

.. code-block:: python
    :linenos:

    from pathlib import Path
    from metasmith.python_api import *
    from metasmith import examples
    dtypes, contigs, references, transforms = examples.GenomicsAnnotationExample()

- :python:`dtypes` is a :python:`DataTypeLibrary`, `more here <data.html#data-types>`_
- :python:`contigs` and :python:`references` are :python:`DataInstanceLibraries`, `more here <data.html#data-instances>`_
- :python:`transforms` is a :python:`TransformInstanceLibrary`, `more here <transforms.html>`_

contents of :python:`contigs`

.. code-block::

    fosmid.fna          (genomics::contigs)

contents of :python:`references`

.. code-block::

    blast.oci.uri       (genomics::oci_image_blast)
    prodigal.oci.uri    (genomics::oci_image_prodigal)
    swissprot_bcaa.faa  (genomics::protein_reference_fasta)

contents of :python:`transforms`

.. code-block::

    blast
    prodigal

Next we will create an agent to manage workflows on our behalf and deploy it to the specified location.

.. code-block:: python
    :linenos:

    path_to_agent_home = Path("./metasmith_home").resolve()
    smith = Agent(
        home = Source.FromLocal(path_to_agent_home),
    )
    smith.Deploy()

.. note::
    `The location can be remote. <data.html#logistics>`_


The agent is able to generate workflows that connect input and output data ``Endpoints``.
Let's see what ``Endpoints`` are available.

.. code-block:: python
    :linenos:

    for k in dtypes.types:
        print(k)

.. code-block::

    aa_sequences
    contigs
    oci_image_blast
    oci_image_prodigal
    orf_annotations
    protein_reference_fasta

We can now ask the agent to generate a workflow to produce a target data type from given data instances.
Here ``contigs`` is a ``DataInstanceLibrary`` that contains the input files to the workflow.
`See how to create your own inputs here <data.html#data-instances>`_

.. code-block:: python
    :linenos:

    task = smith.GenerateWorkflow(
        samples=[contigs],
        resources=[references],
        transforms=[transforms],
        targets=[
            dtypes["orf_annotations"].WithLineage([dtypes["contigs"]]),
        ]
    )

Take a peek of the workflow using the following code.

.. code-block:: python
    :linenos:

    task.plans[0][0].RenderDAG("./dag", format="png")

.. image:: /_static/example_metagenomics_dag.svg
   :align: center

|
Asking the agent to execute the workflow in its deployed workspace involves two commands.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(task)

The workflow will execute asynchronously and its progress can be monitored with:

.. code-block:: python
    :linenos:

    smith.CheckWorkflow(task)

.. note::

    Learn how to run `available standard analyses here. <../modules/index.html>`_
