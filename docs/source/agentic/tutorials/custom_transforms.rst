.. role:: bash(code)
    :language: bash

Custom transforms (CLI)
############################################################

This tutorial mirrors the Python ``custom_transforms`` tutorial. We
add a new output type (``sequences::ani_matrix``), scaffold a
``fastani`` transform that produces it, validate the contract,
propagate types to the transform library, then plan a workflow that
uses the new transform.

The agent never touches Python directly — all steps are ``metasmith``
shell calls.

Prerequisites
============================================================

Complete the input-library and agent setup from `My first agent
<my_first_agent.html>`_. We reuse ``./workspace/3pangenome.xgdb`` and
the transform libraries on disk.

1 — Define the new output type
============================================================

.. code-block:: bash

    metasmith type add data_types/sequences.yml ani_matrix \
      --properties '{"_": "average nucleotide identity matrix", "ext": "tsv"}'

There is no cache to invalidate — each subsequent call re-reads the
type YAML from disk.

2 — Scaffold the transform
============================================================

.. code-block:: bash

    metasmith transform scaffold transforms/pangenome fastani \
      --in sequences::gbk \
      --out sequences::ani_matrix \
      --group-by sequences::gbk \
      --container containers::fastani.oci \
      --cpus 4 --memory-gb 8

The CLI returns the path to the newly created ``.py`` and its
generated source.

3 — Fill in the protocol body
============================================================

Read it, edit it, write it back:

.. code-block:: bash

    metasmith transform read transforms/pangenome fastani > /tmp/fastani.py
    # edit /tmp/fastani.py in your editor
    metasmith transform write transforms/pangenome fastani --source /tmp/fastani.py

4 — Validate the contract
============================================================

This runs the transform module through a harness that checks
inputs/outputs/group_by resolve to known types. No container is
pulled.

.. code-block:: bash

    metasmith transform validate transforms/pangenome fastani

Expected (with ``--json``): ``{"ok": true, "inputs": [...], "outputs": [[...]]}``.

5 — Propagate types
============================================================

Every transform library carries its own compiled
``_metadata/types/``. Push the registered type libraries into it:

.. code-block:: bash

    metasmith transform propagate-types transforms/pangenome \
      --type-lib data_types

6 — Plan a workflow targeting the new type
============================================================

.. code-block:: bash

    metasmith plan \
      --data-library ./workspace/3pangenome.xgdb \
      --sample-type ncbi::assembly_accession \
      --target-type sequences::ani_matrix \
      --transform-library transforms/logistics \
      --transform-library transforms/pangenome

Inspect the rendered DAG to confirm the new transform fires:

.. code-block:: bash

    metasmith task dag <task_key>

7 — Stage, run, wait, collect
============================================================

The lifecycle calls are identical to `My first agent
<my_first_agent.html>`_: ``metasmith workflow stage`` →
``metasmith workflow run`` → ``metasmith workflow wait`` →
``metasmith workflow tail`` → ``metasmith workflow collect``.
