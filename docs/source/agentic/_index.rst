.. role:: json(code)
    :language: json

Agentic Use
############################################################

Metasmith exposes its full Python API over the `Model Context Protocol
<https://modelcontextprotocol.io>`_. An LLM agent (Claude, etc.) can
drive Metasmith end-to-end — register inputs, author transforms, plan
workflows, run, wait, tail logs, and collect results — without ever
calling Python.

This section is the canonical reference for agent-driven use.

Who this is for
============================================================

- **LLM agents** running the ``metasmith-mcp`` server as a tool source.
- **Engineers** building automations that need a stable, typed,
  introspectable surface that mirrors the Python API.

If you are writing a Jupyter notebook by hand, prefer the
`Python tutorials <../tutorials/_index.html>`_.

What an MCP-driven session looks like
============================================================

The canonical end-to-end shape is::

    register_type_library          ──► types known to the server
    create_data_library + add_*    ──► typed inputs registered
    register_transform_library     ──► tools known to the server
    save_agent + deploy_agent      ──► execution target ready
    plan_workflow                  ──► task_key (cached on disk)
    stage_workflow                 ──► nextflow scripts on the agent
    run_workflow                   ──► detached launch, returns immediately
    wait_for_workflow              ──► blocks on `run completed at` sentinel
    tail_workflow_log              ──► last N lines for inspection
    get_result_source              ──► where the results live
    collect_results                ──► copy back to a local/Globus dest
    trace_lineage                  ──► map outputs to their inputs

Every step above is a single MCP tool call. The server caches loaded
libraries, tasks, and agents across calls, so an agent does not need
to re-load between operations.

.. toctree::
    :maxdepth: 2

    setup
    tool_reference
    tutorials/my_first_agent
    tutorials/custom_transforms
    lifecycle_recipes
    diagnosing_failures
