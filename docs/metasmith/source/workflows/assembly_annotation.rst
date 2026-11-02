Assembly & Annotation
############################################################

.. role:: python(code)
    :language: python

.. note::
    This is not a guide for getting started with Metasmith. See `the tutorial <../main/workflow.html>`_ instead.


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
        samples    = [inputs],
        resources  = [containers],
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
- :python:`assembly`: a generic type over `short_reads_assembly`, `long_reads_assembly`, or `hybrid_assembly`

Annotation
------------------------------------------------------------

Databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- :python:`bakta_database`: the BAKTA database, hosted `here <https://zenodo.org/records/14916843>`_

  - :python:`bakta_database_light`: the lightweight BAKTA database
  - :python:`bakta_database_full`: the default BAKTA database

- Kofamscan

  - :python:`kofamscan_profile`: the Kofamscan profile database, hosted `here <https://www.genome.jp/ftp/db/kofam/>`_
  - :python:`kofamscan_ko_list`: the Kofamscan KO identifier database, hosted `here <https://www.genome.jp/ftp/db/kofam/>`_

- :python:`cazy_ref`: the CAZy database, hosted `here <https://www.cazy.org/>`_

- BUSCO

  - All lineage databases are hosted `here <https://busco-data.ezlab.org/v5/data/lineages/>`_
  - :python:`busco_ref`: the :python:`refseq_db.faa` file from a lineage database
  - :python:`busco_map`: the :python:`species.info` file from a lineage database



Annotation outputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- :python:`coding_sequences`: a FASTA of all predicted coding sequences
- :python:`gene_features`: a GFF3 annotation file including genomic coordinates and other metadata
- :python:`bakta_annotations`: a genome annotated by BAKTA
- :python:`kofamscan_annotations`: a genome annotated by Kofamscan
- :python:`cazy_annotations`: a BLAST-style TSV of CDS hits against CAZy
- :python:`busco_annotations`: a BLAST-style TSV of CDS hits against BUSCO
- :python:`functional_annotations`: a compiled directory of all annotation outputs
