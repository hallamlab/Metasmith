Setup
############################################################

Installing the CLI
============================================================

``pip install -e .`` (from the repo root) installs the ``metasmith``
entry point (with ``msm`` as a shorter alias). There is no daemon to
launch — each invocation is a self-contained shell call.

.. code-block:: bash
    :caption: Terminal

    pip install -e .
    metasmith --version
    metasmith --help

Workspace and JSON output
============================================================

Two global flags shape every call:

.. code-block:: bash
    :caption: Terminal

    metasmith --json --workspace ~/.metasmith/workspace COMMAND ...

- ``--workspace PATH`` — caches planned tasks and rendered DAGs.
  Shared across calls so that an agent that disconnects and reconnects
  can resume from a ``task_key``. Defaults to
  ``~/.metasmith/workspace``; also reads the ``METASMITH_WORKSPACE``
  environment variable.
- ``--json`` — emit machine-readable JSON on stdout. Progress logs are
  routed to stderr so the stdout stream stays clean for ``jq`` or
  similar.
- ``--quiet`` — suppress non-essential output.
- ``-V`` / ``--version`` — print version and exit.

Errors exit non-zero with the message on stderr; they are NOT
swallowed into ``{"error": ...}`` dicts.

Configuration in a wrapper client
============================================================

LLM agents can be given a small wrapper that just shells out:

.. code-block:: bash
    :caption: Example agent wrapper

    metasmith --json --workspace "$METASMITH_WORKSPACE" "$@"

Or for tools that take a process command (Claude Code, etc.), point
them directly at ``metasmith`` with ``--json`` and a workspace
argument; the agent then runs subcommands as normal shell invocations
and parses the JSON results.

Loading state on each call
============================================================

Because there is no persistent server, every call loads the libraries
it touches from disk. Cold-load cost for a single ``.xgdb`` or
type/transform library is small. There is no separate
``register_*`` step — pass paths directly to the subcommands that
need them (``--data-library``, ``--transform-library``,
``--resource-library``, etc.).

If a fresh-load is undesirable in scripts that touch the same library
many times in quick succession, hold the path in a shell variable and
reuse it; the on-disk manifest is the source of truth.
