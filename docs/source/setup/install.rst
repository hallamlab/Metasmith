Getting Started
############################################################

.. role:: bash(code)
   :language: bash

Prerequisites
============================================================

Operating System
------------------------------------------------------------

- Linux is the recommended operating system.
- MacOS is supported, but Apptainer can not be installed on Mac. See :ref:`Container Runtime`.
- Windows is only supported through `WSL <https://learn.microsoft.com/en-us/windows/wsl/>`_.

.. _Container Runtime:
Container Runtime
------------------------------------------------------------

To maximize reproducibility and portability, Metasmith relies on `OCI <https://en.wikipedia.org/wiki/Open_Container_Initiative>`_
compliant containers to standardize the compute environment for itself and the tools that it runs. One of the following must be installed:

- `Apptainer <https://apptainer.org/>`_ is recommended due to its compatibility with research compute infrastructure.
- `Docker <https://docs.docker.com/get-docker/>`_ (experimental). On Linux, please also `do this to enable docker without "sudo" <https://docs.docker.com/engine/install/linux-postinstall/>`_


Conda
------------------------------------------------------------

:bash:`Conda` is a package manager that both provides a distribution channel and automates the install of Metasmith. Alternatively,
we recommend :bash:`mamba`, which is a more performant drop-in replacement for :bash:`conda`.

- `Conda <https://docs.conda.io/en/latest/>`_
- `Mamba <https://github.com/conda-forge/miniforge>`_ (recommended)

Installation
============================================================

Using conda/mamba:

.. code-block:: bash

    conda install -c hallamlab -c bioconda metasmith

This then provides access to metasmith through python

.. code-block:: python
    :linenos:

    from metasmith.python_api import *
    print(METASMITH_VERSION)

Next Steps
============================================================

It is now possible try the `quickstart example <workflow.html#_quickstart>`_. Or continue with the rest of the documentation at a slower pace. 
