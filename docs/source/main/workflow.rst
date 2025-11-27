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

We can now ask the agent to generate a workflow to produce the target data type from given data instances.

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

This is the workflow that the agent has generated:

.. code-block::

    step 1: prodigal
        uses:   ['fosmid.fna', 'prodigal.oci.uri']
        makes:  ['orfs.faa']

    step 2: blast
        uses:   ['orfs.faa', 'swissprot_bcaa.faa', 'blast.oci.uri']
        makes:  ['annotations.csv']


Asking the agent to execute the workflow in its deployed workspace involves two commands.

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(task)

The workflow will execute asynchronously and its progress can be monitored with:

.. code-block:: python
    :linenos:

    smith.CheckWorkflow(task)
