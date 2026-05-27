Follow the tutorial at **`{SANDBOX}/docs/{TUTORIAL_REL}`** step by step,
working inside `{SANDBOX}/workspace/`.

The tutorial is written for a Jupyter notebook. Treat each
`.. code-block:: python` block as a script you can save under
`{SANDBOX}/workspace/` (or append to a single growing script) and run
with `python <name>.py` in the activated `msm_env` conda environment.
At the top of your script, set:

```python
from pathlib import Path
WORKSPACE = Path("{SANDBOX}/workspace").resolve()
MLIB      = Path("{SANDBOX}/MetasmithLibraries").resolve()
```

`MLIB` points at a real clone of the
[MetasmithLibraries](https://github.com/hallamlab/MetasmithLibraries) repo
that has been pre-staged in this sandbox. It contains the canonical
`data_types/`, `resources/containers/`, `resources/lib/`, and `transforms/*`
trees the tutorials reference. Notebook helpers like `ipynbButtonLink(...)`
are illustrative and can be skipped.

If the tutorial requires creating an agent, use:

* runtime: **`{RUNTIME}`**
* agent home: **`{SANDBOX}/agent_home`**

When the tutorial finishes and results are collected, run:

```bash
metasmith data load-remote ./results ./results.xgdb
```

so lineage can be traced.

If any tutorial command produces an error or unexpected output, stop immediately and run:

```bash
metasmith e2e report_issue --cwd "{SANDBOX}" --reason "<one line describing what you saw>"
```

When the tutorial has completed end-to-end, run:

```bash
metasmith e2e checkpoint done --cwd "{SANDBOX}" --key {TASK_KEY}
```

The `--cwd "{SANDBOX}"` flag writes `CONTROL.json` to the sandbox root,
where the harness loop watches for it. Without it, the file would land
in your current working directory and the harness would not see it.
