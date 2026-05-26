# Sandbox prelude (do not skip)

You are simulating a real user running an **end-to-end test** of metasmith
**{VERSION}** from the public docs. The environment has been seeded so
every reference to the public release transparently resolves to a
locally-built artifact — you should **follow the docs verbatim** and the
spoof handles the rest.

The sandbox root is `{SANDBOX}`. Inside it:

- `home/.condarc` is pre-configured. The `-c hallamlab` channel resolves
  to a local file:// channel; conda envs land under `<SANDBOX>/envs/`.
  Do NOT edit `.condarc` and do NOT pass `--override-channels` — the
  spoof depends on it.
- `mamba` and `apptainer` are already on PATH (real users have these
  installed before running metasmith's install command).
- The container runtime for this scenario is **{RUNTIME}**. The metasmith
  container `{IMAGE_TAG}` is reachable locally (host docker daemon for
  DOCKER; pre-placed `.sif` under `<SANDBOX>/agent_home/container_images/`
  for APPTAINER). `metasmith agent deploy` will reuse the local copy.
- `docs/`, `data_types/`, `transforms/` are real (in-sandbox) copies of
  the project tree. Stay inside the sandbox — anything outside the
  sandbox is off-limits.
- Network is allowed for conda-forge / bioconda (metasmith's transitive
  deps come from there, same as a real install). The only spoofed
  channel is `hallamlab` — `-c hallamlab` always resolves locally.

## Checkpointing

When you have completed the task, declare the outcome by running **exactly
one** of:

```bash
metasmith e2e checkpoint done --key <TASK_KEY>           # success
metasmith e2e checkpoint give_up --reason "<why>"        # i cannot proceed
metasmith e2e checkpoint continue --notes "<status>"     # need another iteration
```

The harness watches `<SANDBOX>/CONTROL.json` between iterations. If you
emit no checkpoint, the loop runs again with the same prompt next
iteration. Anything you want preserved across iterations should be
appended to `<SANDBOX>/PROGRESS.md`.

If `metasmith` is not yet on PATH because you haven't run the install
step yet, write the CONTROL.json file directly:

```bash
cat > <SANDBOX>/CONTROL.json << 'EOF'
{"action":"done","task_key":"<TASK_KEY>"}
EOF
```
