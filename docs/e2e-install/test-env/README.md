# test-env

> I actually want this to run on cloud agents, but I guess what I really want is an environment that can be set up and torn down for each test. So I don't know where we could set that up. On an HPC cluster like Sockeye.

> Once we know that, then we'll know if we can run a full actual metagenomics workflow, or we may have to use something like pan-genome analysis, since it doesn't require heavy compute. Are we mocking compute?

> fir is down rn, but can we try on nibi?

> lets try from scratch setup then -> need clean env somewhere (altair?) that installs locally and tries a run on nibi. Watch another naive agent try from only docs.

## Design

- **Client.** A fresh, empty conda env on altair stands in for a new user's machine. The naive agent installs metasmith into it, following only the user docs.
- **Cluster.** nibi, as user `phyberos`, account `def-shallam_cpu`. fir is down.
- **Compute is real, not mocked.** Every Rokaiya failure was in the real environment, and only a real run reaches them.
- **Teardown.** The test owns one fresh directory on nibi scratch, plus the client conda env. The ws-bloat topic decides the details.
- **The awm ssh master carries the ssh login.** Open it with `ssh(verb=connect, host=nibi)` before the test starts, which fires one Duo push. The naive agent's `ssh nibi` then rides the master. The test does not cover first-login Duo setup.

## nibi facts (2026-11-17)

- `/scratch/phyberos`: 174 GiB of 1024 GiB, 57K of 1,000K files. The file cap is per user, so a run that floods it also blocks every other phyberos job on nibi.
- `/home`: 50 GiB and 500K files. An apptainer cache left at its default `~/.apptainer` lands here.
- `$SCRATCH` already holds `refs/`, `bench/` and others from earlier work. The naive run must not reuse them.
- Metabuli's 1.4 TB peak does not fit the 1 TB scratch quota.
