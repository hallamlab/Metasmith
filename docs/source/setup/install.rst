.. role:: bash(code)
   :language: bash

Installing Metasmith
############################################################

.. important::

    Metasmith does not need to be installed again on remote machines.
    To execute a workflow remotely, `deploy an agent <deployment.html>`_ using your local installation.

Prerequisites
============================================================

Operating System
------------------------------------------------------------

Linux and MacOS running on x86_64 (Intel/AMD CPUs) or arm64 (Apple silicon) architectures are supported.

Windows users can use either `WSL <https://learn.microsoft.com/en-us/windows/wsl/>`_, or Docker (see installation instructions below).

.. tip::

    You can check your OS and CPU architecture with:
    
    .. code-block:: console
        :caption: UNIX Terminal

        $ uname -s -m

    Which should print "Linux"/"Darwin" and "amd64"/"arm64" if your system is supported.

Installation
============================================================

Metasmith is available through multiple channels. Pick one of the following. 

Conda
------------------------------------------------------------

:bash:`Conda` is a package manager that both provides a distribution channel and automates the install of Metasmith. Alternatively,
we recommend :bash:`mamba`, which is a significantly more performant drop-in replacement for :bash:`conda`.

.. button-link:: https://github.com/conda-forge/miniforge
    :color: primary

    **Install Mamba**

.. button-link:: https://docs.conda.io/en/latest/
    :color: primary

    **Install Conda**
    
.. code-block:: console
    :caption: Terminal

    $ mamba create -n msm_env -c hallamlab -c bioconda metasmith
    $ mamba activate msm_env
    (msm_env)$ msm

.. code-block:: console
    :caption: expected output:

    metasmith v#.#.#
    https://github.com/hallamlab/metasmith
    ...

Docker
------------------------------------------------------------

.. Warning::

    Experimental feature

`Docker <https://www.docker.com/>`_ is a runtime for software packaged into `containers <https://opencontainers.org/>`_
along with nearly all of their dependencies. Containers are "pulled" as "images" which can then be executed.

.. button-link:: https://docs.docker.com/get-docker
    :color: primary
    
    **Install Docker Desktop**

.. code-block:: console
    :caption: Terminal

    $ docker run \
        --platform=linux/amd64 \
        -it --rm \
        -u $(id -u):$(id -g) \
        --mount type=bind,source="${TMPDIR-/tmp}",target="/tmp" \
        --mount type=bind,source="$(pwd -P)",target="/ws" \
        --workdir="/ws" \
        quay.io/hallamlab/metasmith \
        msm

.. code-block:: console
    :caption: expected output:

    ...
    metasmith v#.#.#
    https://github.com/hallamlab/metasmith
    ...

.. _Apptainer Install:

Apptainer
------------------------------------------------------------

`Apptainer <https://apptainer.org/>`_ is an alternative to Docker designd for research computing on grid infrastructure.
It is only available for **Linux machines**.

We recommend installing via your system package manager (e.g. :bash:`sudo apt install apptainer` on Debian/Ubuntu)
or by following Apptainer's `official install docs <https://apptainer.org/docs/admin/main/installation.html>`_.
The conda-forge build is supported but lacks setuid privileges; metasmith will unpack each container image to
a sandbox directory at deploy time, roughly doubling on-disk footprint per cached image.

.. button-link:: https://apptainer.org/docs/admin/main/installation.html
    :color: primary

    **Install Apptainer**

.. code-block:: console
    :caption: Terminal

    $ apptainer run \
        --bind "${TMPDIR-/tmp}":/tmp,"$(pwd -P)":/ws \
        --workdir /ws \
        docker://quay.io/hallamlab/metasmith \
        msm

.. code-block:: console
    :caption: expected output:

    ...
    metasmith v#.#.#
    https://github.com/hallamlab/metasmith
    ...


Next Steps
============================================================

.. button-link:: tutorials.html
    :color: primary

    **Try a tutorial**
