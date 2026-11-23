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

Additional setup commands, to be run before the excution of each workflow, can be specifed.
On a cluster, load the container runtime here:

.. code-block:: python
    :linenos:

    smith = Agent(
        # ...
        setup_commands=[
            'module load apptainer',
        ]
    )

.. note::

    For the Apptainer runtime, Metasmith stores the built container images and
    Apptainer's own download cache in ``<agent_home>/container_images``. Put the
    agent home on storage with room for the images, such as a cluster's scratch.
    To store the images elsewhere, export ``APPTAINER_CACHEDIR`` in
    ``setup_commands`` and point it at **persistent** storage. A location wiped
    between sessions forces a new download on every run.

.. warning::

    Do not point ``TMPDIR`` or ``APPTAINER_TMPDIR`` at a network filesystem,
    such as ``/home`` or ``/scratch`` on most clusters. Apptainer unpacks each
    image layer in that directory. On a network filesystem the unpack takes over
    an hour, against a few minutes in the login node's local ``/tmp``. Leave
    both unset.

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

Run setup first, from the login node, where the driver runs:

.. code-block:: bash
    :caption: Terminal (login node)

    metasmith workflow setup <agent> <task>
    metasmith workflow run <agent> <task>

The setup run pulls every tool image the workflow needs, as steps on the
driver's host, three at a time. Each step that runs a tool waits for its image,
so a compute node never starts before its image is in the store. A pull that
fails stops the setup run. The same run fetches the reference databases, and its
first line says how much it will download, for example
``setup: ~439 GB of references, 14 images``.

``workflow run`` reports on the store without touching it. It names any image
the agent does not already hold before anything is transferred. That warning is
the cue to run setup first.

.. note::

    The store must be somewhere the compute nodes can *read*, which on most
    clusters means shared storage rather than node-local scratch. This is the
    same choice as the ``APPTAINER_CACHEDIR`` note above, seen from the other
    side: a store on a node-local path is invisible to every other node.

.. _Setup Run:

Set up a workflow before its first run
------------------------------------------------------------

A workflow's reference databases are downloaded by its own steps, and some take
hours. A failed download in the real run is discovered only when that step
fails. A **setup run** does that work first, so the real run starts with every
reference already in place.

.. code-block:: bash
    :caption: Terminal

    metasmith workflow setup <agent> <task> --preset slurm --params '{"slurmAccount": "<account>"}'
    metasmith workflow wait <agent> <setup task>
    metasmith workflow run <agent> <task> --preset slurm --params '{"slurmAccount": "<account>"}'

``workflow setup`` launches a run of the steps that read no sample: the
downloads, and the database builds that use only them. On a container agent the
run first pulls every tool image the task needs, as steps of its own. Before it
starts, it prints how much it will download and how many images it pulls. It
prints the setup run's own task key as ``setup_task_key``, which is the one to
``wait`` on. In the web GUI the same action is the workflow's **set up**
button. The plan shows the same size, and the button asks again when the size
is large. The run publishes the references under ``results/references/`` and a
record of each image under ``results/envs/``.

When the setup run completes, the real run serves those steps from the agent's
cache instead of running them again. Running setup a second time downloads
nothing that is already done. It checks each image in the store again, which
costs one test per image.

.. important::

    The real run must use the **same agent** and the **same task** as the setup
    run. A different agent home has its own empty cache. A task planned again
    from a rebuilt resource library cites different references, and so does not
    find them.

Downloads run on the login node
------------------------------------------------------------

On a cluster, the large downloads run on the node that runs the workflow's
driver, which is normally a login node, not on a compute node. Compute nodes on
many clusters download slowly or not at all. A separate build step then
unpacks, indexes and checks each large download on a compute node. A download
that stops partway continues from where it stopped when the step runs again,
in the same run or a later one on the same agent.

Each large download takes its source address from a run parameter, for example
``gtdb_release_url``, ``uniref50_url`` and ``metabuli_db_url``. Point one at a
local mirror to skip the internet download:

.. code-block:: bash
    :caption: Terminal

    metasmith workflow setup <agent> <task> --preset slurm \
        --params '{"slurmAccount": "<account>", "gtdb_release_url": "https://mirror.example.org/gtdb/release232/232.0"}'

.. note::

    GTDB-Tk uses GTDB release 232, whatever newer release exists. A GTDB
    mirror must serve release 232 in the layout of its ``232.0`` directory,
    ``MD5SUM.txt`` included.

.. caution::

    A download's parameters are not part of its cache key. A run that changes
    what a download fetches, such as another release, a subset or a test
    database, caches the result as the default database, and every later run
    on that agent is served it. Run such a test on its own agent home.

.. _Container Runtime:

File quotas
------------------------------------------------------------

A cluster limits how many files each user or project may hold, and a run
creates many: each step's work directories and each cache hit's link tree
count. Before a launch, metasmith creates a file and a directory in the run's
workspace. When it cannot, it refuses the launch and names the path and the
reason, such as ``Disk quota exceeded (EDQUOT)``. On Lustre it also reads the
agent home's file and space quotas, and warns when the files left look too few
for the run. To clear either message, free files under the agent home, or ask
the cluster admins for a higher file limit.

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

Running tools without a container
------------------------------------------------------------

An agent with :python:`runtime=Runtime.MAMBA` runs each tool in a conda
environment on its host instead of in a container. ``workflow setup`` and
``workflow setup-env`` build the environments a workflow needs. The GUI's
**environments only** button does the same.

Some tools run only in a container:

- A tool whose environment declares no conda package. The run refuses to launch
  and names the environment.
- A tool whose database records the path it was built at, such as VirSorter2.
  Its step fails and says it needs a container runtime.

Run a workflow that needs one of them on a Docker or Apptainer agent.

Running a development engine
------------------------------------------------------------

To run an engine from a source tree inside the agent's image, send the tree to
the agent:

.. code-block:: bash
    :caption: Terminal

    metasmith agent dev-overlay <agent> --src /path/to/src/metasmith

Without ``--src`` it sends the engine the command itself runs from. Every task
then runs that tree in place of the image's own engine. Send it again after
each change to the tree. To return to the image's engine, move
``dev/metasmith`` in the agent home aside.

.. caution::

    The overlay carries code, not dependencies. A tree that imports a package
    the image lacks fails in every task, after staging succeeds.

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