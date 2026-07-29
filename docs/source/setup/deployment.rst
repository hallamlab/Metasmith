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