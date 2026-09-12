.. role:: python(code)
    :language: python

Logistics
############################################################

Representing locations
===========================================================

.. TODO: talk about Source, refactor Sources to the same class as Source.FromSsh, etc.

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
    :linenos:
    
    xgdb.SaveAs(Source.FromLocal("path/to/destination/example.xgdb"))

A :python:`Source` need not be local.

Locations by method of access
===========================================================

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
