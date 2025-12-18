Representing data
############################################################

.. role:: python(code)
   :language: python

Data Types
============================================================

Metasmith describes data types using set of properties, which enables
comparions between data to leverage set operations. For example, if we have two types: A = {1, 2} and 
B = {1, 2, 3}, then B may replace A since B can provide all properties that A can provide. Formally,
if A is a subset of B (A ⊆ B), then A is substitutable by B.

Data types are called Endpoints to avoid conflicts with python types and because they
exist at either end of `transforms <transforms.html>`_.

.. code-block:: python
    :linenos:

    from metasmith.python_api import Endpoint

The following example shows how to create and compare endpoints.

.. code-block:: python
    :linenos:

    ball = Endpoint({"ball"})
    red_ball = Endpoint({"red", "ball"})
    red_ball.IsA(ball) # True, a red ball is a ball
    ball.IsA(red_ball) # False, a ball is not necessarily red

Data Type Library
------------------------------------------------------------

Endpoints can be gathered into a :python:`DataTypeLibrary` and given a name for convenience.

.. code-block:: python
    :linenos:

    from metasmith.python_api import DataTypeLibrary

A :python:`DataTypeLibrary` shares the same basic syntax as a python :python:`dict`

.. code-block:: python
    :linenos:

    dtypes = DataTypeLibrary()
    dtypes["red_ball"] = Endpoint({"red", "ball"})

.. _data type library yaml:

YAML
------------------------------------------------------------

Data type libraries can be persisted to disk and it may be more convenient to edit them as
yaml files.

.. code-block:: python
    :linenos:

    dtypes.Save("dtypes.yml")
    dtypes = DataTypeLibrary.Load("dtypes.yml")

.. note::
    To help with standardization across data type libraries, metadata can be included to
    describe the `ontology <https://en.wikipedia.org/wiki/Ontology>`_ of the data types.

In YAML form, properties of endpoints can be key-value pairs...

.. code-block:: python
    :linenos:

    from metasmith import examples
    dtypes = examples.DataTypeLibraries("template_keyval")

.. code-block:: yaml
    :linenos:

    # ...
    types:
        property_type_demo:
            properties:
                str: abc
                int: 1
                float: 0.3
                bool: True
                none: null
                list:
                    - item 1
                    - item 2
        contigs:
            properties:
                data: DNA sequence
                format: FASTA
        oci_image:
            properties:
                data: software container
                format: OCI
                provides:
                    - python==3.12
                    - some other tool

... or simple lists

.. code-block:: python
    :linenos:

    dtypes = examples.DataTypeLibraries("template_list")

.. code-block:: yaml
    :linenos:

    # ...
    types:
        property_type_demo:
            properties:
                - abc
                - 1
                - 0.3
                - True
                - null
                - list item 1
                - list item 2
        contigs:
            properties:
                - data=DNA sequence
                - format=FASTA
        oci_image:
            properties:
                - data=software container
                - format=OCI
                - provides=python 3.12
                - provides=some other tool

Below is a more realistic example themed after genomics.

.. code-block:: python
    :linenos:

    dtypes = examples.DataTypeLibraries("minimal_genomics")

.. code-block:: yaml
    :linenos:

    ontology:
        doi: https://doi.org/10.1093/bioinformatics/btt113
        name: EDAM
        strict: false
        version: 1.25
    schema: '1.0'
    types:
        aa_sequences:
            properties:
                data: Amino acid sequence
                format: FASTA
        contigs:
            properties:
                data: DNA sequence
                format: FASTA
        oci_image_blast:
            properties:
                data: software container
                format: OCI
                provides:
                    - blast
        oci_image_prodigal:
            properties:
                data: software container
                format: OCI
                provides:
                    - prodigal
        orf_annotations:
            properties:
                data: Protein features
                format: CSV
        protein_reference_fasta:
            properties:
                data: database reference
                format: .faa

Data Instances
============================================================

A :python:`DataInstance` refers to the piece of data that is described by an endpoint.
Data instances are created when a file or folder is registered to a :python:`DataInstanceLibrary`.
On the filesystem, an XGDB is just a folder.

.. code-block:: python
    :linenos:

    from metasmith.python_api import DataInstanceLibrary
    xgdb = DataInstanceLibrary("./example.xgdb")

.. note::

    For historical reasons, data instance libraries are shortened to "XGDB" (extended genome database) named
    after "PGDBs" (pathway genome database) from `Pathway Tools <https://bioinformatics.ai.sri.com/ptools/>`_
    and `Metapathways <https://bitbucket.org/BCB2/metapathways/src/dev/>`_. 

Data type libraries must be added as namespaces to an XGDB before data instances can be added. The namespace
:python:`"genomics"` is added below.

.. code-block:: python
    :linenos:

    from metasmith import examples
    dtypes = examples.DataTypeLibraries("minimal_genomics")
    xgdb.AddTypeLibrary("genomics", dtypes)

The following information is required when adding new data instances:

#. The path to the original file or folder
#. The type of the data instance in the form :python:`"namespace::type"`

.. code-block:: python
    :linenos:

    xgdb.AddItem("/path/to/original/contigs.fna", "genomics::contigs")
    xgdb.AddItem("/path/to/original/orfs.faa", "genomics::aa_sequences")
    xgdb.Save()

Once added, softlinks can by automatically generated for each input file within the XGDB.
A prefix is added to ensure that file names are unique.

.. code-block:: python
    :linenos:
    inputs.Consolidate()

This creates the following directory structure:

.. code-block::

    example.xgdb/
    ├── _metasmith/
    │   ├── index.yml
    │   └── types/
    │       └── genomics.yml
    ├── 1_contigs.fna           (symlink)
    └── 2_orfs.faa              (symlink)

_metasmith/index.yml:

.. code-block:: yaml
    :linenos:

    # ...
    manifest:
        /path/to/original/contigs.fna: genomics::contigs
        /path/to/original/orfs.faa: genomics::aa_sequences
    # ...

_metasmith/types/genomics.yml:

.. code-block:: yaml
    :linenos:

    # ...
    types:
        contigs:
            properties:
                data: DNA sequence
                format: FASTA
        aa_sequences:
            properties:
                data: Amino acid sequence
                format: FASTA
    # ...    
