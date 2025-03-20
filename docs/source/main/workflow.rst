============================================================
Executing Workflows
============================================================

Tutorial: Genomics Annotation
------------------------------------------------------------

.. code-block:: python
    :linenos:
    
    from pathlib import Path
    from metasmith.python_api import *
    from metasmith import examples
    dtypes, contigs, references, transforms = examples.GenomicsAnnotationExample()

.. code-block:: python
    :linenos:

    path_to_agent_home = Path("../workspace/metasmith_home").resolve()
    smith = Agent(
        home = Source.FromLocal(path_to_agent_home),
    )
    smith.Deploy()

.. code-block:: python
    :linenos:
    
    task = smith.GenerateWorkflow(
        given=[contigs, references],
        transforms=[transforms],
        targets=[
            dtypes["orf_annotations"].WithLineage([dtypes["contigs"]]),
        ]
    )

.. code-block:: python
    :linenos:

    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(task)
    smith.CheckWorkflow(task)
