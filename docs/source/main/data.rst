Data and Logistics
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

Data instances are moved to the XGDB folder once added. The following information is required when adding new
data instances:

#. The path to the original file or folder
#. The desired new path relative to the XGDB once added
#. The type of the data instance in the form :python:`"namespace::type"`

.. code-block:: python
    :linenos:

    xgdb.Add(
        items = [
            ("/path/to/original/file", "contigs.fna", "genomics::contigs"),
            ("/path/to/original/file", "orfs.faa", "genomics::aa_sequences"),
        ],
    )
    xgdb.Save()

This creats the following directory structure:

.. code-block::

    example.xgdb/
    ├── _metasmith/
    │   ├── index.yml
    │   └── types/
    │       └── genomics.yml
    ├── contigs.fna
    └── orfs.faa

_metasmith/index.yml:

.. code-block:: yaml
    :linenos:

    # ...
    manifest:
        contigs.fna: genomics::contigs
        orfs.faa: genomics::aa_sequences
    # ...

_metasmith/types/genomics.yml:

.. code-block:: yaml
    :linenos:

    # ...
    types:
        aa_sequences:
            properties:
                data: Amino acid sequence
                format: FASTA
        contigs:
            properties:
                data: DNA sequence
                format: FASTA
    # ...    

Logistics
============================================================

Metasmith provides abstractions to move XGDBs between different compute platforms
using a variety of methods. The :python:`Logistics` class facilitates transfers between
locations specified using the :python:`Source` class. Files and folders not managed by
Metasmith can also be moved using :python:`Logistics`.

.. code-block:: python
    :linenos:

    from metasmith.python_api import Logistics, Source
    mover = Logistics()
    mover.QueueTransfer(
        src = Source.FromLocal("./example.xgdb"),
        dest = Source.FromLocal("path/to/destination/example.xgdb"),
    )
    result = mover.ExecuteTransfers()

A :python:`DataInstanceLibrary` can be moved directly. The above is equivalent to:

.. code-block:: python
    
    xgdb.SaveAs(Source.FromLocal("path/to/destination/example.xgdb"))

A :python:`Source` need not be local.

.. _logistics ssh:

SSH
------------------------------------------------------------

Transfers can be made to and from remote machines via SSH.

.. .. note::

..     `How to set up SSH </guides/ssh.html>`_

.. code-block:: python
    :linenos:
    :emphasize-lines: 3-6

    from metasmith.python_api import SshSource

    remote = SshSource(
        host = "remote_name",
        path = "/path/on/remote/machine/example.xgdb",
    ).AsSource()
    local = Source.FromLocal("path/to/local/example.xgdb")

    mover = Logistics()
    mover.QueueTransfer(remote, local)
    mover.QueueTransfer(local, remote)
    mover.ExecuteTransfers()

.. _logistics web:

HTTP(S)/FTP
------------------------------------------------------------

Files accessible via a url can be downloaded. Upload is currently not supported.

.. code-block:: python
    :linenos:

    from metasmith.python_api import HttpSource
    remote = HttpSource.Parse("https://www.website.com/path/to/example.xgdb").AsSource()

.. _logistics globus:

Globus
------------------------------------------------------------

Metasmith can interface with `Globus <https://www.globus.org/globus-connect-personal>`_
using the `globus-cli <https://docs.globus.org/cli/>`_ to queue transfer tasks between endpoints.


.. code-block:: python
    :linenos:

    from metasmith.python_api import GlobusSource
    remote = GlobusSource(
        endpoint = "01234567-89ab-cdef-0123-456789abcdef", # endpoint ID
        path = "/path/on/endpoint/example.xgdb",
    ).AsSource()

If using the `globus file manager <https://app.globus.org/file-manager>`_, links to folders and files
can be parsed directly.

.. code-block:: python
    :linenos:

    remote_folder = GlobusSource.Parse(
        "https://app.globus.org/file-manager?origin_id=01234567-89ab-cdef-0123-456789abcdef&origin_path=%2Fpath%2Fon%2Fendpoint",
    ).AsSource()


    remote_file = GlobusSource.Parse(
        "https://g-012345.6789a.bcde.data.globus.org/path/on/endpoint/example.file",
    ).AsSource()
