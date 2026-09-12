Representing data
############################################################

.. role:: python(code)
    :language: python

Data Types
============================================================

Motivations
------------------------------------------------------------

The interoperability problem that Metasmith seeks to solve can be summarized as determining
if the output of one computational step can be immediately used as the input of another. The
base case to be considered consists of three components: an upstream step, which produces an
intermediate data product, that is then consumed by a downstream step. 
In bioinformatics, the conventional approach is to forego types and link the steps
using the literal files that are produced. We will call this the "topology-centric" approach
because interoperability is encoded by the connections between tools and intermediate files.
The alternative proposed by Metasmith is to attach types to the inputs and outputs of each step.
Outputs from the upstream step can be used as the inputs of the downstream step if the types match.
We will call this the "contract-centric" approach because interoperability in each step's contract,
which describe the required inputs and promised outputs.

There are three key features that motivate the use of a contract-centric approach over a
topology-centric approach.

1. The contract of each step can be defined in isolation of other steps. While not sufficient on its own, this sets up the foundation for modular systems.
2. Tools are means to an end. Obtaining correct results are often more important than the specific methods used. In this way, the contract-centric approach favours the user over the machine as it describes the data explicitly and leaves Metasmith to infer the topology. By contrast, the topology-centric approach describes the methods explicitly but leaves the no information about what is produced. For example,  "a file produced by flye, megahit, spades, and hifiasm" is harder to comprehend than "a nucleotide sequence encoded in FASTA format".

Definitions
------------------------------------------------------------

Metasmith describes data types using set of properties, which enables
comparions between data to leverage set operations. For example, if we have two types: A = {1, 2} and 
B = {1, 2, 3}, then B may replace A since B can provide all properties that A can provide. Formally,
if A is a subset of B (A ⊆ B), then A is substitutable by B. Practically, an output can be used where an
input is required when the *input's* properties are a subset of the *output's* — the output carries at
least everything the input demands, and may carry more. The direction is not symmetric, and reversing it
yields a planner that appears to work while building wrong chains.

Data type objects within the Metasmith API are called Endpoints to avoid conflicts with python types and because they
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

.. caution::

    A plan will not take these as givens while they are still only registered
    here. Registering mints an identity from the filesystem this process can
    see, so it moves when a file is touched and is invented outright when the
    file lives on the agent's host. The plan key is built from the givens'
    identities and the run directory is named after the key, so an invented
    identity throws away the previous run on every submission.

    Data reaches a plan by being imported into the agent's pool once, and cited
    by name from then on. The import assigns the identity and the pool records
    it, so citing one costs nothing and never moves:

    .. code-block:: bash

        metasmith data import /path/to/original/contigs.fna \
            --dtype genomics::contigs --name study/contigs \
            --agent-home /where/the/agent/lives

    .. code-block:: python
        :linenos:

        givens = smith.PoolGivens()
        givens.Add("/path/to/original/contigs.fna", "genomics::contigs",
                   name="study/contigs")
        inputs = givens.Build("./inputs.xgdb", type_library_paths=[...])

    ``Build`` imports whatever the pool does not already hold, so running it
    again is a citation rather than a second import. Importing the same path
    twice on purpose is how you say a file is a different thing from the one
    the pool already holds.

    The pool is authoritative state. An assigned identity cannot be rebuilt, so
    every cached result keyed on an import dies with the agent home that holds
    the pool, and reuse does not cross from one agent home to another.

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
