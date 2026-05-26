.. role:: bash(code)
    :language: bash

My first agent (CLI)
############################################################

This tutorial mirrors the Python tutorial of the same name, but every
step is a single ``metasmith`` shell call. We will perform pangenome
analysis on three E. coli genomes identified by their NCBI assembly
accessions.

The endpoint of the workflow is a pangenome heatmap SVG.

Prerequisites
============================================================

- ``metasmith`` is installed (see `Setup <../setup.html>`_)
- Docker or Apptainer is available on the host that will be your agent
- A working directory ``./workspace`` where the agent will live

For brevity, the examples below omit ``--json`` — add it whenever you
want machine-readable output instead of pretty-printed tables.

1 — Build the input library
============================================================

Create a fresh ``.xgdb`` and attach the type libraries:

.. code-block:: bash

    metasmith data create ./workspace/3pangenome.xgdb \
      --type-lib data_types/ncbi.yml \
      --type-lib data_types/sequences.yml \
      --type-lib data_types/pangenome.yml \
      --purge

Register a pangenome group, then add three accessions under it:

.. code-block:: bash

    metasmith data add-value ./workspace/3pangenome.xgdb \
      --name ecoli_panel --value "e coli" \
      --dtype pangenome::pangenome

    metasmith data add-value ./workspace/3pangenome.xgdb \
      --name K12 --value GCF_000005845.2 \
      --dtype ncbi::assembly_accession --parent ecoli_panel

    metasmith data add-value ./workspace/3pangenome.xgdb \
      --name Sakai --value GCF_000008865.2 \
      --dtype ncbi::assembly_accession --parent ecoli_panel

    metasmith data add-value ./workspace/3pangenome.xgdb \
      --name O157 --value GCA_000732965.1 \
      --dtype ncbi::assembly_accession --parent ecoli_panel

2 — Create and deploy the agent
============================================================

.. code-block:: bash

    metasmith agent save ./workspace/agents/local.yml \
      --home ./workspace/msm_home \
      --runtime DOCKER

    metasmith agent deploy ./workspace/agents/local.yml

3 — Plan the workflow
============================================================

.. code-block:: bash

    metasmith plan \
      --data-library ./workspace/3pangenome.xgdb \
      --sample-type ncbi::assembly_accession \
      --target-type pangenome::heatmap \
      --transform-library transforms/logistics \
      --transform-library transforms/pangenome

Returns a ``task_key`` (and a ``hints`` array if the solver could not
find a complete plan — see `Diagnosing failures
<../diagnosing_failures.html>`_). The plan is cached under
``--workspace`` (default ``~/.metasmith/workspace``).

You can render the planned DAG to confirm shape:

.. code-block:: bash

    metasmith task dag <task_key>

4 — Stage, run, wait
============================================================

.. code-block:: bash

    metasmith workflow stage ./workspace/agents/local.yml <task_key>

    metasmith workflow run ./workspace/agents/local.yml <task_key> --preset local

``workflow run`` returns immediately (the launcher detaches under
``nohup``). Block on the sentinel:

.. code-block:: bash

    metasmith workflow wait ./workspace/agents/local.yml <task_key> --timeout 3600

Tail the Nextflow log while waiting (in a separate terminal):

.. code-block:: bash

    metasmith workflow tail ./workspace/agents/local.yml <task_key> --source main --lines 50

5 — Collect results and trace lineage
============================================================

.. code-block:: bash

    metasmith workflow result-source ./workspace/agents/local.yml <task_key>

    metasmith workflow collect ./workspace/agents/local.yml <task_key> \
      --dest ./workspace/results

To map outputs back to inputs, load the result library and trace:

.. code-block:: bash

    metasmith data load-remote ./workspace/results ./workspace/results.xgdb

    metasmith data trace ./workspace/results.xgdb \
      ncbi::assembly_accession pangenome::heatmap
