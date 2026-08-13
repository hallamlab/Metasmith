.. role:: json(code)
    :language: json

Agentic Use
############################################################

Metasmith exposes its full Python API as a CLI under ``metasmith``
(alias ``msm``). The same surface is used by humans typing into a
shell and by LLM agents shelling out with ``--json`` for
machine-readable output. There is no server process — each invocation
loads what it needs from disk and exits.

An LLM agent can drive Metasmith end-to-end — register inputs, author
transforms, plan workflows, run, wait, tail logs, and collect
results — entirely through shell commands.

This section is the canonical reference for agent-driven use.

Who this is for
============================================================

- **LLM agents** that shell out to ``metasmith ... --json``.
- **Engineers** who want one stable, typed, scriptable surface that
  mirrors the Python API.

If you are writing a Jupyter notebook by hand, prefer the
`Python tutorials <../tutorials/_index.html>`_.

What a CLI-driven session looks like
============================================================

The canonical end-to-end shape is::

    metasmith data create + data add-*       ──► typed inputs registered
    metasmith agent save + agent deploy      ──► execution target ready
    metasmith plan                           ──► task_key (cached to workspace)
    metasmith workflow stage                 ──► nextflow scripts on the agent
    metasmith workflow run                   ──► detached launch
    metasmith workflow wait                  ──► blocks on `run completed at` sentinel
    metasmith workflow tail                  ──► last N lines for inspection
    metasmith workflow result-source         ──► where the results live
    metasmith workflow collect               ──► copy back to a local/Globus dest
    metasmith data trace                     ──► map outputs to their inputs

Every step above is a single CLI call returning JSON when invoked with
``--json``. No state is held between invocations; the workspace
directory (``--workspace``, default ``~/.metasmith/workspace``) caches
planned tasks so they can be re-fetched by ``task_key``.

.. toctree::
    :maxdepth: 2

    setup
    tool_reference
    lifecycle_recipes
    diagnosing_failures
