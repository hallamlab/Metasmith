# tests/unit

Unit and contract tests on a small surface — no DAG planning, no container exec, no Nextflow.

Lives here: version-chain pins (`test_container_tag.py`, `test_dev_sh_tag.py`, `test_build_pip_version_split.py`), data model contracts (`test_paths.py`, `test_source_parse.py`, `test_lineage_schema.py`), CLI surface (`test_cli.py`), build decomposition (`test_build_decomposition.py`), Nextflow metadata extraction (`test_nxf_task_metadata.py`), LiveShell unit behavior (`test_live_shell.py`).

Default marker: `fast`. Should finish in <100ms each.
