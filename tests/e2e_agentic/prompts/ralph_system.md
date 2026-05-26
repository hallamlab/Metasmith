{PRELUDE}

# Your task

Complete the metasmith tutorial at **`docs/{TUTORIAL_REL}`**, step by
step, inside `workspace/`. The `docs/`, `data_types/`, and `transforms/`
directories under the sandbox root are real (in-sandbox) copies — read
from them freely.

If the tutorial requires creating an agent, the runtime for
`metasmith agent save` MUST be **`{RUNTIME}`** and the agent home MUST
be **`<SANDBOX>/agent_home`** (the harness has pre-staged the local
container there).

When you finish (or determine that you cannot finish), declare the
terminal state by calling exactly one of:

```bash
metasmith e2e checkpoint done --key <TASK_KEY>          # tutorial complete
metasmith e2e checkpoint give_up --reason "<text>"      # cannot proceed
```

# Rules

1. Use only the shell — `metasmith ...` and standard POSIX commands.
2. Stay inside the sandbox.
3. Follow the docs verbatim, including any install/setup steps the
   tutorial directs you to. The harness has spoofed `-c hallamlab` to a
   local file:// channel, so the docs' install command runs offline.
4. After each meaningful step, append a short one-line note to
   `../PROGRESS.md` so future iterations of this loop know where you
   left off. Read that file at the start of each iteration.
5. When the workflow has finished and results are collected, run
   `metasmith data load-remote ./results ./results.xgdb` so the harness
   can trace lineage.

# Reading state

Before doing anything else this iteration:

```bash
ls -la .
cat ../PROGRESS.md 2>/dev/null
```

Check whether metasmith is already installed for you:

```bash
mamba env list | grep -q msm_env && \
    source $(conda info --base)/etc/profile.d/conda.sh && conda activate msm_env
```

If `msm_env` does not exist, install it per the docs:
`mamba create -y -n msm_env -c hallamlab -c bioconda metasmith` and then
activate it.

If `PROGRESS.md` shows the previous iteration was waiting on a long
workflow, run `metasmith workflow wait ...` and then proceed.

# Failure surface

If a command fails, capture the error in `../PROGRESS.md` and try to
make forward progress. Only `checkpoint give_up` if you have a concrete
reason you cannot continue (e.g. a tool exits with a clearly fatal error
that you have already tried to work around).
