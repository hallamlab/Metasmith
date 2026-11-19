# run-0 (dry check, 2026-11-19)

Goal: install from `./docs`, print the version, run `ssh nibi hostname`. Haiku, one iteration, about 273K tokens, outcome `done`. Installed metasmith 0.25.1 from the real `hallamlab` channel (3.9 GB env with pkgs cache).

## Harness checks

- Silo: the jail exposes the sandbox, the credentials file, the bootstrap mamba, the claude binary, a generated one-host ssh config and the ControlMaster socket directory. The environment holds 9 variables. Every tool call stayed in the sandbox.
- The account's claude.ai connectors (Claude Docs, Gmail, Drive, Calendar, bioRxiv) reached the agent until `--strict-mcp-config` was added. Verified empty after.
- ssh: `ssh nibi hostname` rode the master from inside the jail. With no master, the generated `ProxyCommand` refuses instead of starting a Duo login.
- Watcher kill switch: with `--max-files 100` against a 303-file dir it cancelled Slurm job 28000103, killed two login-node processes by PID (cwd under the run dir) and wrote `STOP.json`. Local kill escalates TERM to KILL.

## Doc issues the agent found

| class | where | issue |
|---|---|---|
| ambiguity | `setup/install.rst` vs `agentic/setup.rst` | Two install paths (`mamba create ... metasmith` and `pip install -e .`); nothing says which a new user picks. This is I4. |
| ambiguity | `setup/install.rst` expected output | The doc shows `metasmith v#.#.#` plus a URL line. `msm --version` prints `metasmith 0.25.1`. |
| workaround | `setup/install.rst` | `mamba activate` does not persist across an agent's shell calls, so it ran `./msm_env/bin/msm`. Agent-only friction, not a doc issue. |
| harness | prompt | "Work only inside it" pushed the agent to `-p` instead of the documented `-n`. The prompt now says conda already keeps envs in the sandbox. |
