.. role:: json(code)
    :language: json

My first agent (MCP)
############################################################

This tutorial mirrors the Python tutorial of the same name, but every
step is a single MCP tool call. We will perform pangenome analysis
on three E. coli genomes identified by their NCBI assembly accessions.

The endpoint of the workflow is a pangenome heatmap SVG.

Prerequisites
============================================================

- ``metasmith-mcp`` is installed and running (see `Setup <../setup.html>`_)
- Docker or Apptainer is available on the host that will be your agent
- A working directory ``./workspace`` where the agent will live

1 — Confirm what the server already knows
============================================================

.. code-block:: json
    :caption: server_status

    {}

The server returns its loaded type/data/transform/agent paths and the
workspace directory. If nothing is loaded, the next calls register the
inputs.

2 — Register types and tools
============================================================

.. code-block:: json
    :caption: register_type_library

    {"path": "data_types/ncbi.yml"}

.. code-block:: json
    :caption: register_type_library

    {"path": "data_types/sequences.yml"}

.. code-block:: json
    :caption: register_type_library

    {"path": "data_types/pangenome.yml"}

.. code-block:: json
    :caption: register_transform_library

    {"path": "transforms/logistics"}

.. code-block:: json
    :caption: register_transform_library

    {"path": "transforms/pangenome"}

3 — Build the input library
============================================================

Create a fresh ``.xgdb`` and attach the type libraries.

.. code-block:: json
    :caption: create_data_library

    {
      "path": "./workspace/3pangenome.xgdb",
      "type_library_paths": [
        "data_types/ncbi.yml",
        "data_types/sequences.yml",
        "data_types/pangenome.yml"
      ],
      "purge": true
    }

Register a pangenome group, then add three accessions under it.

.. code-block:: json
    :caption: add_data_value

    {
      "library_path": "./workspace/3pangenome.xgdb",
      "name": "ecoli_panel",
      "value": "e coli",
      "dtype": "pangenome::pangenome"
    }

.. code-block:: json
    :caption: add_data_value

    {
      "library_path": "./workspace/3pangenome.xgdb",
      "name": "K12",
      "value": "GCF_000005845.2",
      "dtype": "ncbi::assembly_accession",
      "parents": ["ecoli_panel"]
    }

.. code-block:: json
    :caption: add_data_value

    {
      "library_path": "./workspace/3pangenome.xgdb",
      "name": "Sakai",
      "value": "GCF_000008865.2",
      "dtype": "ncbi::assembly_accession",
      "parents": ["ecoli_panel"]
    }

.. code-block:: json
    :caption: add_data_value

    {
      "library_path": "./workspace/3pangenome.xgdb",
      "name": "O157",
      "value": "GCA_000732965.1",
      "dtype": "ncbi::assembly_accession",
      "parents": ["ecoli_panel"]
    }

4 — Create and deploy the agent
============================================================

.. code-block:: json
    :caption: save_agent

    {
      "path": "./workspace/agents/local.yml",
      "home_uri": "./workspace/msm_home",
      "runtime": "DOCKER"
    }

.. code-block:: json
    :caption: load_agent

    {"agent_path": "./workspace/agents/local.yml", "name": "smith"}

.. code-block:: json
    :caption: deploy_agent

    {"agent_name": "smith"}

5 — Plan the workflow
============================================================

.. code-block:: json
    :caption: plan_workflow

    {
      "data_library": "./workspace/3pangenome.xgdb",
      "sample_type": "ncbi::assembly_accession",
      "target_types": ["pangenome::heatmap"],
      "transform_libraries": ["transforms/logistics", "transforms/pangenome"]
    }

Returns a ``task_key`` (and a ``hints`` array if the solver could
not find a complete plan — see `Diagnosing failures
<../diagnosing_failures.html>`_).

You can render the planned DAG to confirm shape:

.. code-block:: json
    :caption: render_plan_dag

    {"task_key": "<task_key from previous step>"}

6 — Stage, run, wait
============================================================

.. code-block:: json
    :caption: stage_workflow

    {"agent_name": "smith", "task_key": "<task_key>"}

.. code-block:: json
    :caption: run_workflow

    {"agent_name": "smith", "task_key": "<task_key>", "config_preset": "local"}

``run_workflow`` returns immediately (the launcher detaches under
``nohup``). Block on the sentinel:

.. code-block:: json
    :caption: wait_for_workflow

    {"agent_name": "smith", "task_key": "<task_key>", "timeout_s": 3600}

Tail the Nextflow log while waiting (in a separate call):

.. code-block:: json
    :caption: tail_workflow_log

    {"agent_name": "smith", "task_key": "<task_key>", "source": "main", "lines": 50}

7 — Collect results and trace lineage
============================================================

.. code-block:: json
    :caption: get_result_source

    {"agent_name": "smith", "task_key": "<task_key>"}

.. code-block:: json
    :caption: collect_results

    {"agent_name": "smith", "task_key": "<task_key>", "dest_uri": "./workspace/results"}

To map outputs back to inputs, load the result library and trace:

.. code-block:: json
    :caption: load_remote_library

    {"src_uri": "./workspace/results", "dest_path": "./workspace/results.xgdb", "as_image": false}

.. code-block:: json
    :caption: trace_lineage

    {
      "library_path": "./workspace/results.xgdb",
      "from_type": "ncbi::assembly_accession",
      "to_type": "pangenome::heatmap"
    }
