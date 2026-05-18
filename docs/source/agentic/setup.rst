Setup
############################################################

Launching the server
============================================================

The ``metasmith-mcp`` entry point starts a FastMCP server that listens
on stdio (default) or SSE.

.. code-block:: bash
    :caption: Terminal

    metasmith-mcp \
      --types data_types/ncbi.yml data_types/sequences.yml \
      --data inputs.xgdb \
      --transforms transforms/amplicon transforms/pangenome \
      --agents agents/local.yml \
      --workspace ~/.metasmith/mcp_workspace

Flags can also be supplied as colon-separated environment variables:
``METASMITH_TYPE_LIBS``, ``METASMITH_DATA_LIBS``,
``METASMITH_TRANSFORM_LIBS``, ``METASMITH_AGENTS``,
``METASMITH_WORKSPACE``.

The ``--workspace`` directory caches planned tasks and rendered DAGs;
it is shared across runs so an agent that disconnects and reconnects
can resume from a ``task_key``.

Runtime registration
============================================================

Inputs that were not passed at launch can be added after the fact
without a restart. The cache for that kind is invalidated and re-read
on next access.

==================================  =================================================
Tool                                Purpose
==================================  =================================================
``register_type_library(path)``     Add a types YAML at runtime
``register_data_library(path)``     Add a ``.xgdb`` data library at runtime
``register_transform_library(path)`` Add a transform library directory at runtime
``register_agent(path)``            Add an agent YAML at runtime
``reload_libraries(kinds)``         Drop caches and re-read on next access
``server_status()``                 Show all registered paths and counts
==================================  =================================================

Configuration in a wrapper client
============================================================

Claude Code, for example, points at the server via ``~/.claude.json``:

.. code-block:: json

    {
      "mcpServers": {
        "metasmith": {
          "command": "metasmith-mcp",
          "args": ["--workspace", "/home/me/.metasmith/mcp_workspace"]
        }
      }
    }

Once the server is up, an agent's first call should be ``server_status``
to confirm what is already loaded, then ``register_*`` for anything
needed by the task.
