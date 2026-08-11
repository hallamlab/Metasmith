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
This gives you a *setuid* Apptainer, which metasmith deploys as a single ``.sif`` image.

.. warning::

    The conda-forge / :bash:`mamba` Apptainer build is **not reliable for deployment**.
    Because it lacks the setuid ``starter-suid`` helper, it can only build and run the
    container rootfs through *unprivileged user namespaces*. Many recent Linux
    distributions restrict these by default — e.g. Ubuntu 23.10 and later ship
    ``kernel.apparmor_restrict_unprivileged_userns=1``, under which :python:`smith.Deploy()`
    fails while unpacking the sandbox with
    ``FATAL: ... Failed to create container process: Operation not permitted``.

    On a host where unprivileged user namespaces are blocked, choose one of:

    - install a **system (setuid) Apptainer** via your package manager (preferred), or
    - enable unprivileged user namespaces:
      :bash:`sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`
      (persist under ``/etc/sysctl.d/``), or
    - use the **Docker** runtime instead.

    HPC clusters that provide a setuid Apptainer module (e.g. via ``module load``) are
    unaffected. Where the conda-forge build *does* work, metasmith unpacks each image to a
    sandbox directory at deploy time, roughly doubling the on-disk footprint per cached
    image.

.. warning::

    **Apptainer 1.5.x bundles a** ``mksquashfs`` **(squashfs-tools 4.7.x) that can crash
    while building the** ``.sif``. The 4.7 series rewrote ``mksquashfs`` around
    multithreaded parallel readers and intermittently aborts with
    ``mksquashfs ... command failed: ... malloc(): corrupted top size`` (SIGABRT) when
    packing larger images — a non-deterministic heap-corruption race (observed failing
    ~80% of builds on Apptainer 1.5.1 / squashfs-tools 4.7.5 / glibc 2.35). It is
    unrelated to disk space or ``ulimit``, and a *setuid* Apptainer does not avoid it.

    Workarounds, in order of preference:

    - **Disable the regressed fragment path** when building by hand:
      :bash:`apptainer build --mksquashfs-args "-no-fragments" metasmith.sif docker://quay.io/hallamlab/metasmith`
      (reliable in testing; the ``.sif`` is marginally larger).
    - **Repoint Apptainer at a working** ``mksquashfs`` (admin): set
      ``mksquashfs path = /usr/bin/mksquashfs`` in ``/etc/apptainer/apptainer.conf`` when the
      system ``squashfs-tools`` predates 4.7, or downgrade Apptainer to the 1.4.x line.
    - **Skip squashfs entirely**: deploy with :python:`smith.Deploy(rootfs="sandbox")`, which
      builds the unpacked rootfs straight from the registry and never calls ``mksquashfs``;
      or build the ``.sif`` on another host and copy it over.

    :python:`smith.Deploy()` builds the image on the target host and does not expose
    ``--mksquashfs-args`` directly, but it *does* retry a failed pull with ``-no-fragments``
    before falling back to the unpacked rootfs — so an affected host usually needs no
    intervention at all. ``rootfs="sandbox"`` skips straight to the fallback when it does.

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
