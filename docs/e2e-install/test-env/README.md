# test-env

> I actually want this to run on cloud agents, but I guess what I really want is an environment that can be set up and torn down for each test. So I don't know where we could set that up. On an HPC cluster like Sockeye.

> Once we know that, then we'll know if we can run a full actual metagenomics workflow, or we may have to use something like pan-genome analysis, since it doesn't require heavy compute. Are we mocking compute?

## Status

Open. This topic sits ahead of software-suite: the environment decides whether the full metagenomics template fits, or a light one such as `pangenome_heatmap_from_assembly`.

## Proposal on the table (not yet answered)

- The tester is an isolated local session on altair: Haiku, with restricted tools and no workspace memory or journals. Claude cloud sessions cannot reach Sockeye, which needs a CCDB key plus Duo, through the ssh-arbiter.
- The client side is a fresh conda env, standing in for a user's laptop.
- On Sockeye, each test gets a fresh directory under `/scratch/st-shallam-1/<user>/msm_e2e/<run-id>`. The harness creates it, refuses to start if it exists, and deletes it at teardown.
- The reference databases are one shared, persistent, pre-staged copy.

## Survey facts

- Metasmith already has a virtual runtime (`tests/metasmith/e2e/virtual/`) and mock transforms (`tests/metasmith/e2e/docker/test_direct_run.py`). Both test the engine, not an install.
- Every Rokaiya failure was a real-environment failure: ssh key and Duo, a missing `module load apptainer`, a cold pull, Slurm presets, file quota.
