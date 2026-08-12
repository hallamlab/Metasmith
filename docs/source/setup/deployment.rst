.. role:: python(code)
    :language: python

Deploying agents
############################################################

Agents are deployed to their specified home:

.. code-block:: python
    :linenos:

    agent_home = Source.FromLocal("/local/path/")
    smith = Agent(
        home = agent_home,
        runtime=Runtime.DOCKER,
    )
    smith.Deploy()

Previous deployments, such as those from an earlier version of Metasmith, can up overwritten by setting :python:`assertive=true`.

.. code-block:: python
    :linenos:

    smith.Deploy(assertive=True)

Agents can also be deployed to remote machines through SSH by changing their home.

.. code-block:: python
    :linenos:

    agent_home = Source.FromSsh(
        host="<host_name>",
        path="/remote/path/",
    )

.. tip::

    `SSH setup <../setup/ssh.html>`_

Additional setup commands, to be run before the excution of each workflow, can be specifed:

.. code-block:: python
    :linenos:

    smith = Agent(
        # ...
        setup_commands=[
            'export TMPDIR="/home/$USER/tmp"',
            'mkdir -p $TMPDIR',
            'export APPTAINER_CACHEDIR="$TMPDIR"',
            'export APPTAINER_TMPDIR="$TMPDIR"',
        ]
    )

.. note::

    For the Apptainer runtime, ``APPTAINER_CACHEDIR`` also selects where
    Metasmith stores its built container images (the ``.sif`` files and any
    unpacked ``.sandbox`` directories) — this is the store ``apptainer exec``
    reads from at run time. When it is unset, Metasmith falls back to
    ``<agent_home>/container_images``. If you export it (as above), point it at
    **persistent** storage with room for the images; a location that is wiped
    between sessions (e.g. a node-local ``/tmp``) forces a re-pull on every run.
    The example above uses ``/home/$USER/tmp``, which persists, so it is safe.

.. note::

    On clusters that use environment modules, the runtime must be loaded in
    ``setup_commands`` — and some sites need more than the runtime's own module.
    On UBC's **Sockeye**, ``module load apptainer`` alone fails; the working form
    names the compiler it was built against:

    .. code-block:: python
        :linenos:

        setup_commands=[
            "module load gcc/9.4.0 apptainer/1.3.1",
        ]

    Which modules a site needs is a fact about that site. If ``apptainer
    --version`` works interactively but a deploy or a run cannot find it, the
    login shell is loading something your ``setup_commands`` are not.

.. _Offline Compute Nodes:

Compute nodes with no internet
------------------------------------------------------------

Tool images are fetched lazily, by the first task that needs each one. On a
cluster whose compute nodes can reach a registry that is the right behaviour —
nothing is downloaded that no step turned out to need. Where the compute nodes
have **no route out**, it is the wrong place for the fetch to happen, and the
run fails inside its first task rather than at submit time.

Fill the store ahead of the run, from a host that *can* fetch — a login node:

.. code-block:: bash
    :caption: Terminal (login node)

    metasmith workflow stage <agent> <task>
    metasmith workflow materialise <agent> <task>
    metasmith workflow run <agent> <task>

``materialise`` fetches every image the staged workflow needs into the agent's
image store, on the agent's own host, and exits non-zero if any of them could
not be fetched — so a failure is visible before you submit rather than hours
into a run. It is idempotent: running it again does nothing, because each image
is skipped on the same check every task performs. Pass ``--force`` to re-fetch
regardless, which is what to reach for when a store is suspect rather than
incomplete.

``workflow run`` reports on the store without touching it: any image the agent
does not already hold is named before anything is transferred. That warning is
the cue to run ``materialise`` first.

.. note::

    The store must be somewhere the compute nodes can *read*, which on most
    clusters means shared storage rather than node-local scratch. This is the
    same choice as the ``APPTAINER_CACHEDIR`` note above, seen from the other
    side: a store on a node-local path is invisible to every other node.

.. _Container Runtime:

Container Runtime
------------------------------------------------------------

To maximize reproducibility and portability, Metasmith relies on `OCI <https://en.wikipedia.org/wiki/Open_Container_Initiative>`_
compliant containers to standardize the compute environment for itself and the tools that it runs. One of the following must be installed
on the machine that the agent is deployed to:

- `Apptainer <https://apptainer.org/>`_ is typically used by research compute infrastructure. See the :ref:`apptainer install notes <Apptainer Install>` for the recommended installation channel.
- `Docker <https://docs.docker.com/get-docker/>`_ is the de facto industry standard for containerization.

.. important::
    
    Docker must be invocable without explicit use of :python:`sudo`. Check to see that the following works.
    
    :python:`--rm` *just tells docker to clean up after itself.*

    .. code-block:: bash
        :caption: Terminal

        docker run --rm hello-world

    `There is this guide for linux systems <https://docs.docker.com/engine/install/linux-postinstall/>`_

The agent must be configured to use the installed container runtime.

.. code-block:: python
    :linenos:

    smith = Agent(
        ...
        runtime=Runtime.APPTAINER,
    )

    # or
    smith = Agent(
        ...
        runtime=Runtime.DOCKER,
    )

SLURM
------------------------------------------------------------

Provided Nextflow config presets are retrievable by name, including one for HPC
platforms using SLURM.

.. code-block:: python
    :linenos:

    with open("../secrets/slurm_account") as f:
        SLURM_ACCOUNT = f.readline()

    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params=dict(
            slurmAccount=SLURM_ACCOUNT,
        )
    )

.. tip::

    Here is the Nextflow documentation on

    - `config <https://www.nextflow.io/docs/latest/config.html>`
    - `parameters <https://www.nextflow.io/docs/latest/cli.html#pipeline-parameters>`

These are just paths to Nextflow config files, so feel free to point to your own config as well.
Metasmith doesn't require a specific Nextflow configuration.

.. code-block:: python
    :linenos:

    smith.RunWorkflow(
        task,
        config_file="path/to/my/config.nf",
    )

.. tip::

    :python:`task` is the result of :python:`smith.GenerateWorkflow(...)`