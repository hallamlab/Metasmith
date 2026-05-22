.. role:: json(code)
    :language: json

Custom transforms (MCP)
############################################################

This tutorial mirrors the Python ``custom_transforms`` tutorial.
We add a new output type (``sequences::ani_matrix``), scaffold a
``fastani`` transform that produces it, validate the contract,
propagate types to the transform library, then plan a workflow
that uses the new transform.

The agent never touches Python directly — all steps are MCP calls.

Prerequisites
============================================================

Complete the input-library and agent setup from `My first agent
<my_first_agent.html>`_. We reuse ``./workspace/3pangenome.xgdb``
and the loaded transform libraries.

1 — Define the new output type
============================================================

.. code-block:: json
    :caption: add_type

    {
      "library_path": "data_types/sequences.yml",
      "name": "ani_matrix",
      "properties": {
        "_": "average nucleotide identity matrix",
        "ext": "tsv"
      }
    }

Reload the type cache so the next call sees the new type:

.. code-block:: json
    :caption: reload_libraries

    {"kinds": ["types"]}

.. code-block:: json
    :caption: register_type_library

    {"path": "data_types/sequences.yml"}

2 — Scaffold the transform
============================================================

.. code-block:: json
    :caption: scaffold_transform

    {
      "library_path": "transforms/pangenome",
      "name": "fastani",
      "inputs": ["sequences::gbk"],
      "outputs": ["sequences::ani_matrix"],
      "group_by": "sequences::gbk",
      "container_type": "containers::fastani.oci",
      "resources": {"cpus": 4, "memory_gb": 8, "duration_h": 2}
    }

The server returns the path to the newly created ``.py`` and its
generated source.

3 — Fill in the protocol body
============================================================

Read it, edit it, write it back:

.. code-block:: json
    :caption: read_transform_source

    {"library_path": "transforms/pangenome", "transform_path": "fastani"}

.. code-block:: json
    :caption: write_transform

    {
      "library_path": "transforms/pangenome",
      "transform_path": "fastani",
      "source": "<filled-in source code>"
    }

4 — Validate the contract
============================================================

This runs the transform module through a harness that checks
inputs/outputs/group_by resolve to known types. No container is
pulled.

.. code-block:: json
    :caption: validate_transform_contract

    {"library_path": "transforms/pangenome", "transform_path": "fastani"}

Expected: ``{"ok": true, "inputs": [...], "outputs": [[...]]}``.

5 — Propagate types
============================================================

Every transform library carries its own compiled
``_metadata/types/``. Push the registered type libraries into it:

.. code-block:: json
    :caption: propagate_types

    {"transform_library": "transforms/pangenome"}

.. code-block:: json
    :caption: reload_libraries

    {"kinds": ["transforms"]}

6 — Plan a workflow targeting the new type
============================================================

.. code-block:: json
    :caption: plan_workflow

    {
      "data_library": "./workspace/3pangenome.xgdb",
      "sample_type": "ncbi::assembly_accession",
      "target_types": ["sequences::ani_matrix"],
      "transform_libraries": ["transforms/logistics", "transforms/pangenome"]
    }

Inspect the rendered DAG to confirm the new transform fires:

.. code-block:: json
    :caption: render_plan_dag

    {"task_key": "<task_key>"}

7 — Stage, run, wait, collect
============================================================

The lifecycle calls are identical to `My first agent
<my_first_agent.html>`_: ``stage_workflow`` → ``run_workflow`` →
``wait_for_workflow`` → ``tail_workflow_log`` → ``collect_results``.
