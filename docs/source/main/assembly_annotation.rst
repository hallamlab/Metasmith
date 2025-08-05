Assembly & Annotation
############################################################

.. role:: python(code)
   :language: python

.. note::
    This is not a guide for getting started with Metasmith. See `the tutorial <workflow.html>`_ instead.


Usage
============================================================
Metasmith includes a built-in library of transformations covering common workflows in genomic assembly and functional annotation.

.. code-block:: python
    :linenos:

    from metasmith.python_api import *
    dtypes, containers, transforms = Std()

- :python:`dtypes`: the `data types <data.html#data-types>`_ provided by the library
- :python:`transforms`: the `transforms <transforms.html>`_ provided by the library
- :python:`containers`: a `data instance library <data.html#data-instances>`_ providing containers for execution of the :python:`transforms`

You can use these APIs when generating a workflow. For example:

.. code-block:: python
    :linenos:

    smith.GenerateWorkflow(
        given      = [containers, inputs],
        transforms = [transforms],
        targets    = [dtypes["functional_annotation"]]
    )
assuming :python:`inputs` is a :python:`DataInstanceLibrary` with some user-provided inputs.


Data Types
============================================================
The assembly and annotation library provides the following `data types <data.html#data-types>`_:

Quality Control
------------------------------------------------------------
- :python:`read_stats`: short OR long read quality stats

Assembly
------------------------------------------------------------
- :python:`long_reads_filtered`: filtered to remove low-quality reads (based on length and identity)
- :python:`long_reads_assembly`
- :python:`short_reads_trimmed`: quality trimmed and adapters clipped
- :python:`short_reads_assembly`
- :python:`sequence_alignment_map`: short reads mapped on a draft assembly built with long reads
- :python:`binary_alignment_map`: binary version of `sequence_alignment_map`
- :python:`hybrid_assembly`: long read draft assembly improved with short reads
