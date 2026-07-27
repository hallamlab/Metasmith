.. role:: python(code)
    :language: python

.. role:: bash(code)
    :language: bash

Remote agent
############################################################

This tutorial walks through deploying a Metasmith agent to a remote Linux
host over SSH, using Apptainer as the container runtime, driven from a
plain Python script on your laptop. It stops once :python:`smith.Deploy()`
succeeds — running a workflow on the deployed agent is covered by the
existing tutorials (just swap the agent's home).

Metasmith itself does **not** need to be installed on the remote machine.
The deploy step pushes a self-contained Metasmith container and a small
relay folder, and from then on the remote acts as an agent home.

Prerequisites
============================================================

- `Metasmith is installed locally <../setup/install.html>`_
- SSH access to a Linux host. The :ref:`SSH section below <Remote SSH>`
  covers the config in detail; for a refresher on the syntax itself see
  the `SSH setup page <../setup/ssh.html>`_.
- :ref:`Apptainer <Apptainer Install>` is installed on the remote, *or*
  someone with administrative access on the remote is willing to install
  it. The conda-forge build works but trades a slightly larger on-disk
  footprint per container (see :ref:`apptainer-sandbox-note`).
- An absolute path on the remote that you can write to and that has
  enough free space for the Metasmith container plus any workflow
  container caches. Scratch on HPC, :bash:`$HOME` on a plain box.

You do **not** need :python:`msm lab`, a clone of
:python:`MetasmithLibraries`, Jupyter, or a notebook for this tutorial.

.. _Remote SSH:

Why SSH is the tricky part
============================================================

Metasmith drives the remote agent by shelling out to :bash:`ssh` with
:bash:`-o BatchMode=yes`. :bash:`BatchMode=yes` disables every interactive
prompt: passwords, key passphrases, 2FA challenges. If a plain
:bash:`ssh <host>` from your terminal asks you for *anything*, then the
same connection from Metasmith will fail immediately — typically with an
opaque "Permission denied" or "Connection closed by remote host".

The fix is **ControlMaster multiplexing**: authenticate once in a
foreground shell, leaving an open multiplexed socket; every subsequent
:bash:`ssh <host>` (including Metasmith's) reuses that socket and never
prompts.

Minimal config
------------------------------------------------------------

Pick an :bash:`Host` alias that makes sense (we'll use :bash:`my_remote`
below). Add a block like this to :bash:`~/.ssh/config`:

.. code-block:: text
    :caption: ~/.ssh/config

    host my_remote
        HostName remote.example.org
        User myusername
        PreferredAuthentications publickey
        IdentityFile ~/.ssh/my_key
        ControlMaster auto
        ControlPath ~/.ssh/live_connections/%h_%p_%r

Then create the directory the ControlMaster socket lives in (the path
must exist before the first connection):

.. code-block:: bash
    :caption: Terminal

    $ mkdir -p ~/.ssh/live_connections

.. tip::

    The `SSH setup page <../setup/ssh.html>`_ has the full walkthrough,
    including how to generate the keypair, copy the public half to the
    remote, and lock down file permissions.

Verify SSH works non-interactively
------------------------------------------------------------

Run these three commands in order. They are exactly what Metasmith does,
in escalating strictness. Each must succeed before moving to the next.

.. code-block:: bash
    :caption: Terminal

    # 1. Authenticate interactively. Answer 2FA / key passphrase here.
    #    Leave this shell open OR exit cleanly — the ControlMaster
    #    socket stays alive either way.
    $ ssh my_remote

    # 2. From a second terminal, confirm a fresh ssh reuses the socket.
    #    Should return immediately with no prompts.
    $ ssh my_remote hostname

    # 3. Confirm BatchMode=yes also succeeds — this is what Metasmith
    #    will do under the hood. Should print "ok" and an Apptainer
    #    version, no prompts.
    $ ssh -o BatchMode=yes my_remote 'echo ok && apptainer --version'

If step 3 fails — for example with :bash:`Permission denied` or
:bash:`Connection closed by remote host` — fix it **here**, not after
:python:`smith.Deploy()`. The most common causes are: the ControlMaster
socket has expired (re-run step 1), the public key is not in the remote's
:bash:`~/.ssh/authorized_keys`, or the SSH config lacks
:bash:`PreferredAuthentications publickey` and the server is silently
falling back to a method that requires a prompt.

Remote-side prerequisites
============================================================

Apptainer must be invocable as :bash:`apptainer` on the remote (step 3 of
the SSH preflight confirms this). For install channels see the
:ref:`Apptainer Install` section of the install page.

.. _apptainer-sandbox-note:

.. note::

    Apptainer's setuid helper (:bash:`starter-suid`) is required to run
    :bash:`.sif` images directly. System packages (apt, dnf, the
    official installer) ship it; the conda-forge build does not. When
    Metasmith deploys to a host without :bash:`starter-suid`, it
    additionally unpacks each :bash:`.sif` to a sibling
    :bash:`.sandbox/` directory, which roughly doubles the on-disk
    footprint per cached container. The agent transparently prefers the
    sandbox at run time. No action is required from you — this is just
    so the disk-usage figures are not a surprise.

If the remote is space-constrained on :bash:`$HOME` (common on HPC
logins), point Apptainer's cache and temp at scratch via
:python:`setup_commands` — Metasmith will run these in the shell that
hosts the agent on every invocation:

.. code-block:: python
    :linenos:

    setup_commands=[
        'export TMPDIR="/home/$USER/tmp"',
        'mkdir -p $TMPDIR',
        'export APPTAINER_CACHEDIR="$TMPDIR"',
        'export APPTAINER_TMPDIR="$TMPDIR"',
    ]

Write the deploy script
============================================================

Save the following as :bash:`deploy_remote.py` on your laptop. Adjust the
:python:`host` alias, the remote :python:`path`, and the
:python:`setup_commands` to your environment.

.. code-block:: python
    :caption: deploy_remote.py
    :linenos:

    from metasmith.python_api import Agent, ContainerRuntime, Source

    agent_home = Source.FromSsh(
        host="my_remote",                # must match an SSH config Host entry
        path="/home/myusername/msm_home",  # absolute path on the remote
    )

    smith = Agent(
        home=agent_home,
        runtime=ContainerRuntime.APPTAINER,
        setup_commands=[
            'export TMPDIR="/home/$USER/tmp"',
            'mkdir -p $TMPDIR',
            'export APPTAINER_CACHEDIR="$TMPDIR"',
            'export APPTAINER_TMPDIR="$TMPDIR"',
        ],
    )

    smith.Deploy()
    print("deployed")

Run it:

.. code-block:: bash
    :caption: Terminal

    $ python deploy_remote.py

The first deploy pulls the Metasmith container onto the remote (a few
minutes on a fast link, longer on HPC logins with throttled egress). On a
host without :bash:`starter-suid` the deploy also builds the
:bash:`.sandbox/` directory described above, which adds another minute or
so per container.

Verify the deploy
============================================================

The agent home should now contain Metasmith's bootstrap files. Check
from your laptop without leaving the SSH alias:

.. code-block:: bash
    :caption: Terminal

    $ ssh my_remote ls /home/myusername/msm_home
    container_images  lib  relay  runs  msm

    $ ssh my_remote ls /home/myusername/msm_home/container_images
    metasmith.sif
    # or, on hosts without starter-suid:
    # metasmith.sandbox  metasmith.sif

    $ ssh my_remote /home/myusername/msm_home/msm --version
    metasmith v0.18.3
    https://github.com/hallamlab/metasmith

If the :bash:`.sandbox/` directory is present, the host's apptainer
lacks :bash:`starter-suid` and Metasmith has already adapted; you do not
need to do anything. If it is absent, the host has the setuid helper and
:bash:`.sif` images will run directly.

Re-deploying
============================================================

:python:`smith.Deploy()` is idempotent — calling it again against an
existing home is a no-op. To overwrite an older deploy (for example after
upgrading Metasmith locally and wanting the remote to match, or after
manually corrupting a sandbox), pass :python:`assertive=True`:

.. code-block:: python
    :linenos:

    smith.Deploy(assertive=True)

This re-pushes the bootstrap files and rebuilds any sandbox directories
without re-pulling the cached :bash:`.sif`.

Next steps
============================================================

You now have a working remote agent. To actually run something on it,
the change is one line: open any of the existing tutorials and replace

.. code-block:: python

    agent_home = Source.FromLocal(WORKSPACE/"msm_home")

with the :python:`Source.FromSsh(...)` block from this tutorial. The rest
of the tutorial — registering inputs, generating a workflow, staging,
running, collecting results — works identically against a remote home.

- `My first agent <my_first_agent.html>`_ — the canonical end-to-end
  pangenome example. Swap the home and run it remotely.
- `Custom transforms <custom_transforms.html>`_ — add your own tool to
  the type graph.
- `Python basics <python.html>`_ — when driving Metasmith from a plain
  :bash:`.py` script (rather than a notebook), :python:`RunWorkflow` is
  fire-and-forget. Use :python:`smith.WaitForWorkflow(task)` to block on
  completion; it works against remote homes without any local polling.
