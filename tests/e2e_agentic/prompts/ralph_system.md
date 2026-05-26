Follow the tutorial at **`{SANDBOX}/docs/{TUTORIAL_REL}`** step by step,
running its commands inside `{SANDBOX}/workspace/`.

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
metasmith e2e report_issue --reason "<one line describing what you saw>"
```

When the tutorial has completed end-to-end, run:

```bash
metasmith e2e checkpoint done --key {TASK_KEY}
```
