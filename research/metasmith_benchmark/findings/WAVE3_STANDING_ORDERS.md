# Wave 3 standing orders — paste verbatim into every launch brief

Every one of these was paid for by a real failure in waves 1 and 2. None is hypothetical.

## The one rule that has been broken most

**NEVER END A TURN WAITING ON A BACKGROUND JOB.** A background-job completion notification
CANNOT reach a subagent after its turn ends. Two agents stalled this way, one for forty minutes,
and the second left four orphaned poll loops consuming the resource its own lane needed.
Poll inside the same turn, or launch detached on the cluster and read the log in that same turn:

    nohup setsid timeout -k 30 <secs> ./cmd > log 2>&1 < /dev/null & disown

## Record your process tree and your agent-to-worktree assignment

Three separate incidents in wave 2 came from nobody tracking who owned what: orphaned worker
pools, a duplicated integrity check, and two agents editing one worktree.

- Write your PIDs, your log paths and your working directory into a file a later reader finds.
- On teardown, consult that file. **Do NOT pattern-match on command names.**
- **`kill` on a shell driver does NOT kill an `xargs -P` pool** — those children reparent to init
  and keep running. Kill the leaf commands, repeatedly, because the pool refills as fast as you
  kill workers.
- `pgrep -f <pattern>` and `ps -ef | grep <pattern>` BOTH match your own ssh wrapper's argv. Match
  on the process, or on a PID file the job itself wrote.

## The 512-process cap is the limit that actually bites, and it wears three faces

The login node caps each USER at 512 processes and 16 GiB across EVERY ssh session. The memory
number looks alarming and is mostly reclaimable page and dentry cache; there have been zero OOM
kills. The process cap does not degrade gracefully. Its three observed symptoms:

| symptom | where |
|---|---|
| `GC(0) Failed to create worker thread` / `unable to create native thread` | a Nextflow driver JVM at boot |
| `pthread_create failed (EAGAIN) ... stacksize: 1024k` | inside a pipeline task |
| `FATAL ERROR: Failed to create thread` from squashfs | container image conversion |

**One limit, three unrecognisable faces.** Also: an intermittent Lmod error about
`index local 'out' (a nil value)`, plus `fork: retry: Resource temporarily unavailable`, is the
same pressure — and it appears on COMPUTE nodes too, printed ABOVE output that is perfectly
correct. Never discard a result because of it.

Mitigations: `-XX:ActiveProcessorCount=4` in `NXF_OPTS`; container pulls at parallelism 1-2, never
6; kill your own stray loops before blaming the cluster.

## Stagger your launch, and do not raise the heap

The engine sets `-Xmx10g` with an `-Xms2g` floor. One driver is fine. **Three concurrent drivers
request 30 GiB against a 16 GiB cgroup** and claim 6 GiB immediately. So: reach your 30-minute
pilot before the next agent submits. Hosting drivers under Slurm is NOT available — the Slurm
driver has no relay and must be launched from a login node.

## Launch from the FROZEN checkout, and never touch it

A leaf instance id hashes the file's **path and mtime**, never its content. So a `git checkout`, a
`git pull`, an `rsync` without `-t`, a metadata rebuild, or `envs/fabfos/setup_agent_env.sh` all
move every plan key, every cached step is discarded, and **nothing warns you**. For the life of
your run the launch checkout gets none of those. Record the plan key immediately before submit and
re-check it before any resume.

## A fresh agent home needs the DEV OVERLAY, or its first real step dies

**This was missing from these orders and it cost a lane its whole pilot.** After deploying an
agent home, push the pinned engine source over the published image:

    MSM_AGENT_HOME=<that agent home> research/cami/ops/push_dev_overlay.sh

The published image predates the collapsed `ExecWithEnv(env=, cmd=)` that every transform in this
library now calls, so a task built against the pin dies at runtime with an unexpected-keyword
`TypeError` while **the solve, the stage and the launch all report success**. Version strings are
not code parity: the CLI and the image tag both say the same version and the code inside differs.

No rebuild and no redeploy are needed. The agent's own deploy bakes a CONDITIONAL bind of
`$AGENT_HOME/dev/metasmith` over the image's site-packages into the `msm` wrapper and the
bootstrap, so pushing the source is sufficient. Proof is in a task log, which prints a line about
staging the dev overlay, not in the script's own output.

## What actually moves a plan key, measured rather than assumed

Three of these were wrong in my own head earlier in this campaign.

| change | moves the plan key? |
|---|---|
| a registered input's PATH string | **yes** |
| a registered input file's mtime | **yes** — ids hash path plus mtime, never content |
| a new data type, or a type re-parented | **yes** |
| a target set change | **yes** |
| engine code or engine CONFIG (`src/metasmith/**`) | **no** |
| a driver under `research/**` | **no** |
| a TRANSFORM's source | **no** — but it DOES retire that transform's cache shards |
| a file's CONTENT inside a directory-registered library item | **no** |

That last row cuts both ways. It is why a library helper can be fixed mid-campaign without
discarding resume state, and it is a hazard: **a content change invisible to the key is also
invisible to `-resume`**, so a previously cached success would be reused against changed code.
After changing one, force the affected step to re-execute rather than trusting the cache.

## Verification standards, in order of how much they have caught

1. **Dump each step's `uses`.** Step counts and step POSITIONS hide re-routing. A binner went
   unscored through two gates with the right count and the right positions.
2. **Check the given-leaf counts**: `Counter(i.dtype_name for i in task.plan.given)`. A driver
   reported zero dropped targets while planning 48 of 491 runs out of existence.
3. **Check what the arm is POINTED AT.** A construction built to make a prediction testable
   outlives the test and reports true numbers about the wrong scope.
4. **Check products, never exit codes.** Several tools here exit 0 having produced nothing, and a
   pipeline reporting `completed` can have produced no output at all.
5. `as [K] unique case(s)` is necessary and NOT sufficient — it is a union across groups and
   imposes no per-case coverage.

## Cluster hygiene

- `Session open refused by peer` is a LOCAL ssh multiplexing session cap, not the cluster. Retry
  with a short backoff. **NEVER reconnect** — that fires an MFA attempt against a ten-strike
  account lockout. The retry wrapper is for SHORT queries only: against a long-running remote
  command a retry spawns a SECOND copy.
- `df` and `df -i` on this Lustre are untrustworthy — they have returned answers four orders of
  magnitude apart. Use `diskusage_report`, or
  `lfs quota -p $(lfs project -d /scratch/phyberos | awk '{print $1}') /scratch`.
- `def-shallam` project space is roughly DOUBLE its quota. `rpp-shallam` has bytes but sits near
  its file quota. Everything belongs under `/scratch/phyberos/`.
- `salloc ... <cmd>` WITHOUT `srun` runs on the LOGIN node. Check `hostname` explicitly.
- Do NOT set a Slurm partition — `sbatch` rejects the name and redirects automatically.
- Decompose a number before extrapolating from it. 262K files looked like an inode crisis; the
  part that scales with sample count was 598.

## The ramp IS the authorization

Pilot on a deliberately tiny input, capped at **30 minutes, hard**. A pilot that reaches the first
real tool invocation and runs out of time has SUCCEEDED — record what it proved and what it did not
reach. Then one sample, then ten, then the full set. Do not skip a rung to save wall clock.

## A run-control probe pointed at the wrong agent home reports the run as EMPTY

There are **three** agent homes, not one, and the driver's defaults do not name two of them:

    /scratch/phyberos/cami/metasmith        the default; CAMI short read and long read
    /scratch/phyberos/pratama2026/metasmith needs MSM_AGENT_HOME_PRATAMA
    /scratch/phyberos/metagem/metasmith     metaGEM's own

`MSM_AGENT_HOME_PRATAMA` **defaults to the CAMI home**. Run `InspectWorkflowProcesses` or
`CancelWorkflow` without it and the workspace resolves under `cami/`, where your run id does not
exist, and the answer is:

    {"processes": [], "survivors": [], "empty": true}

A live run with a token, a process group and a running JVM reported `empty: true`. **A cancel or a
reap against the wrong home does nothing and does not say so.** Print `agent.home.GetPath()` and
`agent._task_workspace(<key>)` and read them before you believe any absence.

Same class as every other false negative this campaign has produced — a `find` at the wrong depth,
a `pgrep` matching its own argv, an archive inspected before it was unpacked. **An instrument that
was pointed somewhere else reports absence, and absence is what a healthy teardown also looks
like.**

## Cancel a run through PID.lock, never with a kill

`Agent.CancelWorkflow(<run key>)` is a ladder and the first rung matters: removing `PID.lock` makes
the driver's supervisor terminate Nextflow's own process group, and **Nextflow's shutdown hook is
what `scancel`s the grid jobs**. Kill the JVM directly and you orphan every queued and running
Slurm job — the documented failure that left a run's jobs alive with no FAILED row.

It also lets the driver wind down, which is when the **task cache is promoted**. Completed steps
stay reusable; a group kill throws that away.

Verify by product: `sacct` the run's jobs and confirm `CANCELLED`, and confirm the other arms'
jobs are untouched. Process names are `nf-pNN__<tool>`, and **NN is the step index of that run's
own plan**, so the same tool carries different numbers in different arms and two arms can both
have a `p06`. Never identify a job's owner by its process name alone.

## A brand-new agent home needs its images MATERIALISED, not just pulled

`setup --run` pulls, but the images unique to one arm can arrive unverified and unmaterialised in
that home's own image store. The images shared with an arm that ran earlier are already verified,
**so the gap is invisible on every shared image and appears only on the arm-unique ones.**

Fix with `Agent.MaterialiseImages('<run key>')` through the python API. A driver that builds its
Agent inline has no standalone agent yaml, so the CLI route does not apply. The check is
per-agent-home, not per-image.

## `sacct -S` reads LOCAL time — do not pass `date -u`

These orders tell you `sacct -u <user> -S <time> | grep nf-` is the authoritative check that tasks
reached the queue, against `squeue`, which is a snapshot. That still holds. But `sacct` interprets
`-S` in **local** time while `date -u` renders UTC, and this host is seven hours behind — so
`-S $(date -u -d '3 hours ago' +%H:%M)` asks for a window seven hours in the future and returns
**zero rows while jobs are running**. That is indistinguishable from "nothing ever submitted",
which is the exact failure the recipe exists to prevent.

Use `date -d '3 hours ago' +%H:%M`, a full local timestamp, or `-S today`. Treat an empty `sacct`
result as suspect until the window is proven.

## An instrument that shares a contended resource with its subject measures the contention

A watcher polling fir over ssh, running underneath agents that also poll fir over ssh, exhausts the
shared ControlMaster's `MaxSessions` and then reports the empty result as a dead cluster. Mine did,
three times, for three different causes. A mux refusal names the PEER — `Session open refused by
peer` — so it reads as the cluster refusing a login when the cap is entirely local.

So: classify your own transport failures before reporting on the subject, retry a mux refusal with
backoff, and require several consecutive genuine failures before announcing anything. And never
answer a mux refusal with a reconnect — that fires an MFA attempt against a ten-strike lockout.

**Corollary: a long-running bash script does not pick up edits to its own file.** Restart a monitor
after editing the script it runs, or you are still measuring with the old instrument.

## GTDB-Tk 2.6.1 cannot be run against a tree with no skani database

`classify_wf --skip_ani_screen` gates only the PRE-screen. A second, post-placement ANI
verification is **not** gated: in `classify.py` the guard reads

    ##if not prescreening and len(skani_verification) > 0:
      if len(skani_verification) > 0:

so the version that once skipped it is commented out upstream. Without `skani/database/` it raises
`Reference genome missing from skani database` and exits — **after** MSA masking and both pplacer
placements, measured at 33m44s for a single genome, and **the enclosing job exits 0:0**. The only
escape is `--genes`, which changes the input contract to called genes and still pays pplacer.

So a GTDB-Tk step needs the full data package plus the ~179 GB / ~113 K-file representative-genome
set, or it must not be in the plan at all. Under retry-then-ignore it fails silently and starves
everything downstream of it.

## Dump the ALL-STEPS view before and after any change to a shared transform's requirements

`GB_ALLSTEPS=1 gate_bindings.py <arm>` prints every step with everything it consumes. The
step-family filter cannot show that a step LEFT the plan, because a step it does not recognise is
simply absent from its output — and `GB_UNFILTERED`'s `<- []` means "nothing matched the filter",
not "consumes nothing". Removing a requirement from a shared transform is exactly the change those
two views cannot see.

## THE TWO-DRIVER GATE IS BACK. Three concurrent drivers OOM-killed two of themselves.

**This reverses a withdrawal I made earlier in this campaign, and the withdrawal was wrong.** I had
measured non-reclaimable memory at 2.04 GB of 16, each driver under 1 GB resident, and `oom_kill`
at **0** across 23,600 reclaim events, and concluded the heap flags were never the constraint.
Every one of those numbers was accurate. The inference was not.

Measured 2026-09-12: two drivers coexisted for **two hours** with no trouble. A **third** started
and within 23 seconds the cgroup OOM killer took the new driver *and* one of the existing two.
`memory.events` went from `oom_kill 0` to `oom_kill 3`.

**Why anon does not predict it, and I named anon as the guard.** The cgroup's `memory.current` sits
at **99.5% of its 16 GiB max permanently** — ~15.9 GB of page cache on a node doing heavy staging.
So a starting JVM's `-Xms` floor is a demand for that much *instant* reclaim, and when reclaim
cannot keep up the kernel kills rather than waits. Steady-state `anon` read **1.01 GB** moments
after two OOM kills. **The trigger is the RATE of a new claim against a cache-saturated cgroup, not
the steady-state total**, and no steady-state number shows it.

So, before every submit:

    pgrep -c -x java        # on fir. REFUSE AT 2. Two is the stop condition, not three.

Count the JVM **by exact binary name**. `pgrep -f` and `ps -ef | grep` both match your own ssh
wrapper's argv and have produced confident wrong answers three times here.

The engine's driver heap floor is now `-Xms512m -Xmx6g` (was `-Xms2g -Xmx10g`), which cuts the
startup claim fourfold — drivers measure under 1 GB resident on plans of ~5,000 tasks, so the old
numbers were defensive rather than needed. That **reduces** the hazard and does not remove it: keep
the count. And never raise either number to "fix" a kill — a bigger heap makes your driver the most
attractive victim on a shared node. Only an in-JVM `OutOfMemoryError`, which is a *different*
failure from being `Killed`, justifies raising `-Xmx`.

## A metasmith run reports "run completed" after its driver is SIGKILLed

The worst shape in this campaign, observed live:

    13:09:57  Submitted process > p06__interleave_zipped_short_reads_cached (1)
    13:10:04 E| bash: line 54: 3972426 Killed    nextflow -config ./workflow.resources.nf ...
    13:10:05  cache: 0 member(s) promoted, 5 served from shards
    13:10:06  run completed at [2026-09-12_13-10-06]

A driver killed 23 seconds in, an output count of 5 — all cache-served shims — and a completion
line. `results/` held `given.csv` and `_metadata` and nothing else. **An output count is not a
product count, and "run completed" survives the driver's death.**

So after any run that claims completion:

    grep -c Killed <run>/_metasmith/logs.latest/agent.log     # expect 0
    ls -1 <run>/results                                       # expect real per-target files

Nextflow's own wrapper is more honest about this than ours: on the same event a plain `nextflow`
invocation returned `NXF_EXIT=137` (128+9, SIGKILL), which is unambiguous. If you read a metasmith
run's status, grep for `Killed` before believing the completion line.

## fir HAS THREE LOGIN NODES AND THE MEMORY CAP IS PER USER **PER NODE**

Every driver in this campaign has been launched onto **login1**, and login1 is now unusable. This
was the missing lever the whole time.

Measured 2026-09-12 with a JVM that actually faults in its heap floor
(`java -Xms… -XX:+AlwaysPreTouch -version`; **plain `java -version` only RESERVES address space and
gives a false pass — it "STARTED ok" on a node that kills every real driver**):

| node | memory.current | slack | oom_kill | -Xms512m pretouch | -Xms2g pretouch |
|---|---|---|---|---|---|
| login1 | 15.93 / 16.00 GiB, **99.59%** | **68 MB** | 7 | **KILLED rc=137** | **KILLED rc=137** |
| login2 |  4.49 / 16.00 GiB, 28%    | 11.8 GB | 0 | STARTED | STARTED |
| login3 |  1.53 / 16.00 GiB, 9.6%   | 14.8 GB | 0 | STARTED | STARTED |

login1's cap is **essentially all page cache** — `file` 15.81 GB, `anon` 0.010 GB — it does not
drain (8 KB of movement over 20 s), and `memory.reclaim` is root-only (`--w------- root root`), so
it cannot be forced down. Lowering the heap floor does **not** rescue it: 512 MB is killed too.

**So the rule is one driver per login node, on a node with real slack — not a count on one node.**
Check before launching:

    ssh fir bash /scratch/phyberos/node_probe.sh          # runs locally, prints all the above
    ssh fir 'ssh login3 bash /scratch/phyberos/node_probe.sh'

### Launching a driver on login2 or login3

`login2`/`login3` are **internal** names (172.25.118.x) and are **not resolvable from the
workstation** — `fir.computecanada.ca` is the only external entry point, and this account carries a
hard MFA-lockout guard (`ControlMaster no`, a ProxyCommand that refuses to CREATE a master,
`NumberOfPasswordPrompts 1`, ticket #0317299). **Do not add ssh config entries or open new external
connections to reach them.** Hop internally instead: `ssh fir` then `ssh loginN`, which needs no
MFA and reuses the existing master for the first leg.

The route, both unknowns proven:

1. **`start.sh` is node-agnostic.** It `cd`s to its own directory and passes `host=$(hostname)`, so
   it adapts to whatever node runs it. Nothing in it names login1.
2. **The relay IS node-bound** and is the only piece needing setup. Its socket lives in node-local
   `/tmp` and is discovered by hostname, symlinked into the agent home as `relay/<hostname>`. So
   start one on the new node first:

       cd <agent home> && nohup ./relay/msm_relay start </dev/null >/tmp/relay.log 2>&1 &

   Confirm `relay/login3 -> /tmp/msm_login3_unknown_user` appears. Verified working.
3. Stage from the workstation as usual — that is file transfer over the existing master, not a
   driver — then run `bash start.sh` on the chosen node.

**CAUTION run-control from the workstation goes blind.** `CancelWorkflow`, `ReapWorkflow` and
`InspectWorkflowProcesses` resolve their host from the agent home's address (`ssh://fir:…` = login1)
while the workspace is a path on shared `/scratch`. So they will look for the process group on
login1, find nothing, and report `empty: true` — the same false negative as a wrong agent home, one
level out. Cancel a login3-hosted driver by hopping to login3 and removing `PID.lock` there.

### Do not run heavy filesystem sweeps on a login node hosting a driver

A `find` or `du` across hundreds of thousands of files pulls page cache and dentry cache into the
very cgroup that kills drivers. login1's 15.81 GB of page cache is what makes it unusable. Run
storage measurements from a compute node under `srun`.

### A grid job that outlives its driver preserves the COMPUTE, not the cache entry

Confirmed on L3's rung 1. MEGAHIT job 59472922 ran 2h28m58s and reported `COMPLETED 0:0` — a real
success — but it crossed the line *after* its driver was SIGKILLed. Nextflow marks a task cacheable
only once the driver observes its completion and hashes the outputs, so nothing was written to the
cache DB and `-resume` had nothing to find. The relaunch served five preprocessing steps as
`Cached process` and **re-submitted MEGAHIT**.

CAUTION this is the opposite of what a surviving job looks like. The work directory holds a complete,
valid product, and `sacct` says COMPLETED. Neither makes it reachable. There is no supported way to
inject a cache entry, so the output is dead weight on the inode budget — delete it by literal
absolute path once the replacement task has been collected.

So when a driver dies, the recoverable work is exactly what the cache had already promoted, which is
what it printed as `cache: N member(s) promoted`. Anything still running at the moment of death is
lost regardless of how it finishes.

### Two Nextflow drivers must never share a launch directory

Found on login2: an orphaned driver from a wave-2 test run, 8.5 hours old, PPID 1, zero grid jobs,
sharing `/scratch/phyberos/wave2_b3_nfcore/` with L3's live rung-1 run. Both write `.nextflow/history`
and the session cache in that directory, so `-resume` can select the wrong session and two drivers can
corrupt one cache DB. Give every rung its own launch directory.

### An idle Nextflow JVM ignores SIGTERM

That orphan did not die on `kill`. Nextflow traps SIGTERM to run its shutdown hook, and a driver with
nothing left to shut down can sit in it indefinitely. `kill -9` cleared it and the node went from
**214 pids to 69** — ~145 of them were that one JVM's threads, which is also the answer to "what is
using the pid cap when `ps` shows fourteen processes": `pids.current` counts tasks, so it counts
threads.

WARNING only SIGKILL a driver after confirming it owns no queued grid jobs (`squeue` by job name).
A hard kill bypasses the shutdown hook that `scancel`s them, which is the documented way to orphan a
whole queue. The safe teardown is still to remove `PID.lock` and let the supervisor do it.

### The solver is a feasibility constraint in the modelling lane, not a parity preference

Measured 2026-09-12, same bin / medium / container / resources, solver the only variable:

| solver | draft | gap fill |
|---|---|---|
| open (SCIP) | 10m01s | **1h49m11s, still running when killed** |
| CPLEX 22.2.0 | 2m25s | **32m00s, 1.89 MB model written** |

The bin was the *smallest* in its sample's set — 684 proteins, a 570-reaction draft — so both figures
are lower bounds. The open solver does not finish gap filling on the cheapest input in the corpus,
against a transform duration of 2 h. Left in a launch plan that means one wall-clock timeout per bin,
retried, then swallowed by the ignore strategy: the arm reports complete and produces no models.

`run_metagem.py` therefore takes `--solver {open,cplex,both}` and the campaign launches `cplex`.
CAUTION `open` and `cplex` plan to the **same step count** — 11 paired, 13 single — so only the
BINDING tells them apart and a step-count gate passes either way. `gate_bindings.py` reads
`GB_SOLVER` and defaults to `cplex`; pass `GB_UNFILTERED=1` or every modelling step renders `<- []`.

## THE DRIVER BELONGS ON A COMPUTE NODE. This supersedes the login-node route above.

The login-node route works and is not good enough. Measured per node, which is the only way to see
this, because **`ssh fir` round-robins across login1/login2/login3**:

| node | file (page cache) | anon | oom_kill | drivers |
|---|---|---|---|---|
| login1 | 16.96 GB | 0.006 GB | 20 | 0 |
| login2 | 17.00 GB | 0.028 GB | 3 | 0 |
| login3 | 2.68 GB | 0.943 GB | 0 | 1 (healthy) |

`anon` is **tens of megabytes** on both dead nodes. The 16 GiB per-user cap is filled almost
entirely by **page cache from our own staging I/O** — image materialisation, reading reference
databases, walking large trees. Once `file` nears the cap, a starting JVM's heap floor is a demand
for instant reclaim and the kernel kills rather than waits.

**So clearing a node is not sufficient: the staging that must precede a driver poisons the very node
the driver then launches on.** L1 launched on a login2 that had just been cleared to 3.37 GiB and
`oom_kill 0`, and was killed at 13:47:50 — one `Killed` line, one more false-green `run completed`
one second later, `results/` holding bookkeeping only. `memory.reclaim` is root-only, so the cache
cannot be forced down.

**A compute node has none of this.** Verified on `fc30669` under `srun`:

    memory.max            ->  no user slice at all -- THE 16 GiB CAP DOES NOT EXIST THERE
    free -g               ->  755 GB total, 679 GB available
    scontrol ping         ->  Slurmctld(primary) at slurm1 is UP
    sbatch --test-only    ->  accepted, "Job ... to start ... partition cpubase_bycore_b1"
    relay start           ->  relay/fc30669 -> /tmp/msm_fc30669_unknown_user, watcher daemon, success

**That narrows this project's standing note that "the Slurm driver has no relay, so launch from a
login node".** The obstacle was never Slurm submission — a compute node reaches the controller and
submits fine. It was the relay, and the relay is node-local `/tmp` discovered by hostname, so it
starts on any node you can run a command on.

### The recipe — and the engine ALREADY HAS THIS ROUTE, so do not hand-roll it

**`METASMITH_DRIVER_SLURM=1` is a first-class, documented engine feature.** `RunWorkflow` reads it,
renders a *foreground* launcher (`start.slurm.sh`, `RenderLauncher(background=False)`), `sbatch`es it
with `--chdir` set to the run workspace, and writes the job id to `RUN.slurmjob`. Its own source
comments carry the same 29-hour login-node driver death that started this, and it says outright that
`RUN.pgid` is not sufficient and an empty `squeue` does not mean a run has stopped.

An orchestrator spent an afternoon building this by hand out of `--stage-only` plus a bespoke
`sbatch` plus `bash start.sh`. **The hand-rolled version has a silent failure the built-in does not**,
and it cost a launch — see the CAUTION below.

1. **Do every network-touching step from a login node first**, and only those: container pulls,
   image materialisation, reference-database fetches. Compute nodes move under a megabyte per second
   to every external host, so a pull there HANGS rather than fails. A `local`-labelled download step
   runs wherever the DRIVER runs, so moving the driver moves every download with it: staging the
   database and deleting the step is the fix, not retrying.
2. **Launch normally, with the driver env vars set.** The submission itself still happens from a
   login node — that is one ssh session, not a driver — and everything after it is on the allocated
   node:

       METASMITH_DRIVER_SLURM=1 \
       METASMITH_DRIVER_SLURM_CPUS=10 \
       METASMITH_DRIVER_SLURM_MEM=48G \
       METASMITH_DRIVER_SLURM_TIME=7-00:00:00 \
       METASMITH_DRIVER_SLURM_ACCOUNT=rrg-shallam-ab \
       PYTHONPATH="$PWD/src" mamba run -n msm python <driver> run <flags>

   CAUTION the built-in **defaults are 2 cpus and 8G**, which is the sizing that died
   `OUT_OF_MEMORY` in 65 seconds. Always pass all four.
3. **The relay is the one thing the built-in route does not do**, and it is now in both drivers'
   `SETUP_COMMANDS`, so it happens for you. Read that comment before changing it: the relay socket
   lives in the driver node's own `/tmp` and is discovered by hostname, symlinked into the agent home
   as `relay/<hostname>`; `runner.py` connects THROUGH it whenever `needs_relay`, which is true for
   the APPTAINER runtime. The line aborts the job if the symlink does not appear, rather than letting
   the driver run blind.

CAUTION **`--stage-only` leaves `workflow.config.nf` EMPTY, and `bash start.sh` on an empty config
admits every step against ONE cpu.** `_run_lane` returns on `--stage-only` *before* it calls
`make_slurm_config()`, and the config only reaches the run directory inside `RunWorkflow`. So the
hand-rolled recipe — stage, then sbatch, then `start.sh` — produces a run with no executor config at
all. The metaGEM pilot died 63 seconds in with

    error [nextflow.exception.ProcessUnrecoverableException]:
        Process requirement exceeds available CPUs -- req: 16; avail: 1

with a 0-byte `workflow.config.nf`, against 6,787 bytes on a run launched the normal way. **The check
is `ls -l <run>/workflow.config.nf` before you believe a staged run is launchable**, and the tell in
`.nextflow/history` is a session marked `ERR` a few seconds long. It is the same class as
`--mem=12G`: routing around machinery that already existed and losing what it did for you.

WARNING **the driver job's wall is now the run's deadline**, which the login-node route did not have.
Size it generously; a 7-day band costs a couple of cpus for the duration and that is trivial against
this campaign's budget.

WARNING **never `scancel` the driver job to stop a run.** That kills the JVM without its shutdown
hook, which is what `scancel`s the grid jobs, so it orphans the whole queue — see
[[grid-job-outliving-driver-loses-cache]]'s mechanism. Remove `PID.lock`, let the supervisor
terminate Nextflow's process group and let Nextflow scancel its own children, and the driver job then
exits on its own.

CAUTION run-control from the workstation goes blind, one level further out than it does for login3.
`CancelWorkflow`, `ReapWorkflow` and `InspectWorkflowProcesses` resolve their host from the agent
home's address and will report `empty: true` for a live run. `RUN.slurmjob` is the handle that
actually answers whether the driver is alive. Reach the node with `srun --jobid=<driver job> --pty
bash`, or hop to the node the relay symlink names.

### Size the driver job for the LOCAL EXECUTOR, not for the driver

WARNING the first compute-node driver died `OUT_OF_MEMORY` at 1m05s on `--mem=12G`, and the number
was the orchestrator's. A compute node has no *user-slice* cgroup, which is what removes the 16 GiB
ceiling — but the **job** has its own cgroup at whatever `--mem` you asked for, and **local-executor
steps run inside the driver's job**. On a login node those steps ran in the shared user slice, so the
driver's own footprint was all that mattered. Under `sbatch` they are charged to the driver's job.

The preset declares, in `src/metasmith/nextflow_config/slurm.nf`:

    localExecutor { queueSize = 4; memory = '8 GB'; cpus = 8 }

So the job must hold the JVM's `-Xmx6g` ceiling **plus** the 8 GB local budget **plus** apptainer and
the surrounding shell — roughly 14 GB before overhead. Ask for **`--cpus-per-task=10 --mem=48G`**:
ten cpus covers the local executor's declared eight plus the JVM, and 48 GB is deliberate headroom
rather than a computed minimum, at 6% of a 755 GB node. **If any local step's resources are later
overridden upward** — as the DRAM database download's were — raise the driver job to match instead of
discovering it as an `OUT_OF_MEMORY` thirty hours in.

This is the good kind of failure and worth recognising as such: an honest exit code with a named
cause, as against a SIGKILL followed by a false `run completed`.

## A `ref::` dict is not a general staging dict — the type library gates it

WARNING every registration site builds its own `DataInstanceLibrary` and loads its own list of
type-library files. `research/cami/run_cami_metag.py` has FIVE such sites, and they do not agree:
`build_inputs_pratama` loads ten type libraries including `annotation.yml`, while
`build_globals_pratama` loads three and does not.

So adding an entry to a shared dict like `STAGED_REFS` — which every site iterates — makes the sites
that lack that namespace fail, and the failure is:

    AssertionError: namespace [annotation] not found

which names the type library and not the dict that caused it. Adding `annotation::dram_db` to
`STAGED_REFS` stopped **all four arms** from planning at all. The fix was a separate
`STAGED_REFS_PRATAMA`, iterated only where its namespace is loaded, which also keeps the CAMI arms'
launch keys provably unmoved.

CAUTION **the absence of output was the only signal.** Re-measuring four keys returned nothing at
all rather than four keys, and a `grep -E "key="` over that run would have shown an empty result that
reads exactly like a failed grep. Read the raw output of a key measurement, or grep for `Traceback`
alongside `key=`.

## Check that the `local`-labelled steps are all CACHE-SERVED before you trust a compute-node launch

A compute-node driver cannot download. The four remaining database steps in the Pratama arm
(`downloadVibrantDB`, `downloadVirsorter2DB`, `downloadVcontact3DB`, `downloadCheckvDB`) are served
from `task_cache` shards laid down by earlier login-node runs, and **task cache shards are keyed on
the transform signature, not on the plan key**, so they survive a key move. The evidence that they
are genuinely cached is empirical and was already collected: job 59536279 showed 149 steps served
from cache and submitted exactly ONE step, which was the uncached DRAM download.

CAUTION do not check this with `find task_cache -iname "*downloadX*"`. The cache is
content-addressed — `1e/<hash>` directories plus a `cache.sqlite` whose `entries` table keys on an
opaque `transform_key` — so a name search returns zero for every step and reads exactly like
"nothing is cached". The check that works is reading the run's own log for `_cached` on those step
names in the first minutes, and tearing the run down if any of the four is submitted for real,
because it will hang rather than fail.

## LAUNCHING into the wrong agent home costs the task cache, and the driver reports nothing wrong

WARNING `MSM_AGENT_HOME_PRATAMA` and `MSM_AGENT_HOME_CAMI_LONG` both **default to the CAMI home**,
deliberately — the driver's own comment argues that sharing is the supported shape, because the
campaign's headline number is cache reuse between batches and a second home splits that measurement
across two trees that cannot see each other. But the Pratama arm's four reference-database downloads
were cached in a **separate** home at `/scratch/phyberos/pratama2026/metasmith`, and the CAMI home has
none of them (it has 379 cache entries of its own and no download steps at all).

So launching the Pratama arm without that variable staged it into the CAMI home and
`p04__downloadCheckvDB` **submitted for real instead of `_cached`** — which on a compute node hangs
rather than fails. Nothing in the launch reported a problem: it printed `Submitted: <key>` and a job
id like any healthy launch.

**The tell is in the launch output, one line above the job id:**

    Relay server already running at [/scratch/phyberos/cami/metasmith/relay/login3]

The relay path names the agent home. Read it on every launch. The second tell, in the run's own log,
is `WARN: It appears you have never run this project before -- Option '-resume' is ignored` on an arm
that has demonstrably run before.

**Check, before believing any compute-node launch:**

    grep -E 'Submitted process' <run>/_metasmith/logs.latest/agent.log | sed 's/.*> //' | sort | uniq -c

Every `download*` step must carry the `_cached` suffix. A correct Pratama rung 1 reads:

    1 p01__downloadVibrantDB_cached (1)      1 p03__downloadVcontact3DB_cached (1)
    1 p02__downloadVirsorter2DB_cached (1)   1 p04__downloadCheckvDB_cached (1)
    1 p05__interleave_zipped_short_reads (1)

Teardown was clean through `PID.lock`: driver job COMPLETED 20 s after the lock was removed, no
orphaned grid jobs, and the run honestly said `run failed ... with [2] ignored step(s)` rather than
printing a false completion — because removing the lock lets the shutdown hook run.

### `_cached` is necessary and NOT sufficient — read the twin's EXIT STATUS, not its name

A twin can be named `_cached`, be genuinely served from a shard, and still **exit 1**. Then the
product is absent, every consumer of it silently never runs, and the run reports completed. This
happened to `p03__downloadVcontact3DB_cached` on rung 1: four attempts, then `Error is ignored`,
and `vcontact3` at step 29 plus the whole viral tail became unreachable while the `Submitted
process` census above looked perfect.

So run the second half of the check too:

    grep -nE 'terminated with an error|Error is ignored|ignored step' <run>/_metasmith/logs.latest/agent.log

Any hit on a `_cached` step is a hard stop. `[N] ignored step(s)` in the closing line is the same
signal one layer out, and every step in these plans is retry-then-ignore with `failOnIgnore` off.

**The cause, because it generalises to every directory product.** The preset sets `scratch` to the
node-local `SLURM_TMPDIR` for every process, so a twin copies the cached tree into node-local
scratch and Nextflow copies it back with `nxf_fs_copy`'s **`cp -fRL`**. `-L` DEREFERENCES. Any
product tree holding a dangling symlink — vConTACT3 ships **404** of them, inside the mmseqs
temporary directories in its own published release — fails with hundreds of `cannot stat` lines,
and `.command.run` promotes the unstage's status to the task's. Fixed by adding `scratch = false`
to the drivers' appended `withName: '.*_cached'` block, which also halves the I/O.

**CAUTION the error paths name the DESTINATION, which is what makes this easy to misdiagnose.**
`cp: cannot stat '1-1-1.<hash>/v230/…'` is the task's own output directory, relative. That rules
out the shim's copy, whose source would be the absolute cache path — and GNU `cp -r` on coreutils
9.3 preserves symlinks anyway, verified by A/B on a real dangling-symlink subtree. The other tell
is the copy itself: 16,383 files and **zero** symlinks in the work dir against 6,399 files and 431
symlinks in the cache.

**Only the TWINS are exposed, and it is worth knowing why.** `nextflow_codegen.py:287` emits
`label 'x<label>x'` for every declared label, and 33 transforms in this library declare
`labels=["local"]` — so `withLabel: 'xlocalx'` DOES match, and the four real download steps get
`scratch = false` from it. Verified on a live Pratama `workflow.nf`: `grep -c xlocalx` is 4, on
`p01__downloadVibrantDB`, `p02__downloadVirsorter2DB`, `p03__downloadVcontact3DB` and
`p04__downloadCheckvDB`. The **twin** carries no label at all — `executor 'local'`, `cache false`,
cpus, memory — which is exactly why the label-keyed exemption misses it. That also explains why the
original download staged those 404 dangling symlinks without trouble while the twin serving the
same tree from cache could not.

**CAUTION do not carry "`xlocalx` matches nothing in this pipeline" across arms.** It is true of a
**CAMI** plan, which contains no download step at all, and false of a Pratama plan. Check the arm
you are looking at.

## A hand-rolled `apptainer exec` inherits $HOME, and the host's python packages win

apptainer mounts `$HOME` by default and CPython prepends `~/.local/lib/pythonX.Y/site-packages`
to `sys.path`. So a host-installed package **shadows the image's own**. Measured on fir while
commissioning the reference-arm AMBER scorer:

    ImportError: libjpeg.so.62: cannot open shared object file
      File "/home/phyberos/.local/lib/python3.12/site-packages/PIL/Image.py", line 103

`amber.py` imports matplotlib, matplotlib imports PIL, and it got the **host's** PIL inside the
container, whose image carries no libjpeg. It reads as a broken image, not as a path problem.

Run every ad-hoc container as:

    apptainer exec --no-home --cleanenv --env PYTHONNOUSERSITE=1 --bind <what you need> <sif> ...

which is the same trio `src/metasmith/env/environment.py`'s own image probe uses. **The engine's
steps are not exposed**, because the metasmith container binds `/scratch/phyberos` and two
`.globus` paths and never the whole home directory — so this bites only hand-rolled invocations,
which is exactly what a post-hoc scorer or a probe is.

## A discriminator can be right about the axis and blind to the width

`score_reference_amber.py` decides which column of a contig-to-bin table holds contigs by testing
which column's values occur in the assembly — a real test, not a guess, and the one thing that also
catches a table belonging to another sample. It inspected **two** columns.

nf-core/mag's real table has **four**: `assembly_id contig_id binner bin_id`, written as a literal
at `workflows/mag.nf:467` and published unconditionally through `storeDir` to
`GenomeBinning/contig_to_bin/contig_to_bin_map.tsv`. On that file the check finds contigs in column
1 and takes column 0 — `assembly_id`, constant for the whole sample — as the bin. **Every contig
into one bin, scored, reported, no error.**

Two rules out of it:

1. **A membership test bounds the columns it inspects, not the columns that exist.** State the
   width the reader assumes and make it REFUSE anything wider, rather than widening the search.
2. **Read the producer's format before writing the consumer's parser.** The four-column shape, the
   `binner` column that distinguishes DAS Tool's refined set
   (`binning_refinement/main.nf:72` stamps `binner: 'DASTool'`), and the unbinned rows that arrive
   with `binner` REMOVED (`main.nf:51`, so they must be dropped by name or they become a bin
   literally called `null` holding every unbinned contig) are all in the source and none of them
   are in the format documentation.

The scorer now takes that file via `--nfcore-contig-to-bin` and splits it into one AMBER label per
binner. Proven equivalent rather than merely working: MetaBAT2's rows, encoding the same assignment
as the verified two-column path, produce a **byte-identical** `results.tsv`.

## The inode numbers, measured — and the denominator that made them look bad

| thing | inodes | note |
|---|---|---|
| ordinary metasmith task, work dir | **9** | interleave, seqkit, bbduk, megahit, prodigal |
| cached step, durable shard | **~6** | 4,823 shards in the cami home cost 28,724 total |
| per-bin product shard | 130-170 | one file per bin; the p99 tail of the cache |
| `downloadVirsorter2DB` task dir | **31,684** | once per agent home, NOT per sample |
| `downloadVcontact3DB` task dir | **16,574** | likewise, and ×4 while the twin was broken |
| binning task dirs (COMEBin, MetaWRAP) | **UNMEASURED** | the only term still unknown |

Budget roughly **900 inodes per sample** for an 18-step arm — ~162 of work tree plus a cache cost
dominated by four per-bin shards. 229 samples is then ~206 K against 578 K free.

**CAUTION do not divide a run tree's file count by its sample count.** That is where
"~4,000 files per sample" came from: the trees measured contained reference-database staging task
directories, which are a once-per-agent-home cost. The same aggregate was once used to *retire* the
alarm, from a tree whose binning had died and which therefore held none of the per-bin terms. Both
measurements were correct; both inferences used the wrong denominator. **Decompose before dividing
— list the biggest task dirs first, every time.**

Reclaim levers, neither of which touches the science: `cleanup = false` in the preset keeps every
work directory for `.command.err` recovery and its own comment notes that flipping it frees disk and
inodes; and a failed step's retry directories are pure debris once you have read the error out of
one of them.

## The four CAMI-versus-nf-core parity differences are settled, and three needed no new compute

Read the numbers before re-opening any of these.

| difference | resolution | evidence |
|---|---|---|
| DAS Tool `--score_threshold` | **not a difference** | `DAS_Tool --help` in the pinned image: `[default: 0.5]`, which is what nf-core pins |
| phiX removal, nf-core only | **measured no-op** | `remove_phix` bowtie2 log: 127 of 16,647,376 pairs, 0.00% overall alignment rate |
| fastp vs bbduk | **deviation, measured non-confounding on CAMI** | fastp report: 33,294,790 → 33,294,752 reads, `too_short` 0, `low_quality` 0, `too_many_N` 38, adapters on 8,226 reads, mean length 150 → 149 |
| bowtie2 vs minimap2 for depth | **genuine deviation, the only one that can move an AMBER score** | forced: nf-core's short-read lane has bowtie2 only, this library has minimap2 |

CAUTION **do not pin a reference-arm QC parameter to match ours.** `reads_minlength = 51` was one
edit away from being set in `control.config` to match bbduk's floor. fastp's own report says
`too_short_reads` is 0, so the pin buys nothing — and a parameter changed specifically to match the
metasmith arm forfeits G1, which is to run the reference pipeline *as itself*.

CAUTION the trimmer bound holds for **CAMI only**, which is simulated and adapter-free. Pratama's
real groundwater reads are a different question, and there is no control arm there to confound.

## A driver's `params=` dict does not reach a transform, and one param lies about it

`RunWorkflow(params=...)` goes to **nextflow only**. `workflow_ops.py`'s `_parse` splits
every key on `_` and nests it, which is what a config selector needs
(`process_array` -> `params.process.array`). A transform's `context.params`, however, was
built purely from `.command.metadata` — `res` (cpus/memory/attempt), `gpu`, `rootfs` — so
every `context.params.get('<driver_key>', default)` silently returned its default.

Fixed: `bootstrap.py` now loads `<workspace>/workflow.params.yml` and offers both the
nested dict and every `_`-joined path, applied **before** the metadata block so a params
file can never override the resources the executor actually granted. Because the params
file already lives in the run workspace, the fix needs no codegen change and no re-stage,
so runs already in flight pick it up.

**What to carry even though the bug is fixed:**

- **cpus and memory DO arrive** (through `res`). That is why every resource override in
  this campaign worked while no param did, and why nobody suspected the channel.
- **A `.get(key, default)` whose default equals the value you meant to pin is not
  evidence.** metabat2's `--seed 1` read correctly for a campaign because the transform's
  own default is also 1. Seeing it right is what stopped anyone checking `--minContig`,
  which was silently 2500 against a pinned 1500. **Assert the value in the RENDERED command
  line**, from the task's own `.command.out`, never from the driver source and never from
  `workflow.params.yml` — the pin was in the params file the whole time.
- **An engine-side fix retires NO transform cache shard.** A shard keys on the transform
  signature, which hashes the protocol file, so a wrong-parameter result already promoted
  will be served back as `_cached` with no warning after the engine is fixed. Either edit
  the transform too (that retires the shard for free) or force re-execution. Never trust
  `-resume` to re-run a step whose *parameter* changed.

## The dev overlay ships as a TARBALL; the directory is only the fallback

`environment.py` stages `<agent_home>/dev/metasmith.tar` to node-local disk once per node,
keyed on that tarball's own `mtime-size`, and binds the **extracted** copy over
site-packages. `dev/metasmith/` is read only when tarball staging fails. So a file copied
into the directory alone is invisible to every compute node while looking right on Lustre.
**Push both, then rebuild the tar**, in every agent home:

    rsync -r --delete --exclude=__pycache__ --exclude='*.pyc' src/metasmith/ fir:<home>/dev/metasmith/
    ssh fir "cd <home>/dev && tar -cf metasmith.tar.new metasmith && mv -f metasmith.tar.new metasmith.tar"

Audit an overlay with `rsync -rn --itemize-changes`. **Content changes are the `s` column;
`T` alone is only a timestamp.** Doing this turned up that the driver-heap fix from the
morning's OOM sweep had never reached any overlay — 235 entries differed and exactly two by
content.

## `workflow.env.json` is the authoritative per-arm container list

A staged run's own `workflow.env.json` names every env and its container URI for the shape
that was **actually staged**, so unlike a hand-maintained list or a grep over transform
sources it cannot drift from what will run:

    grep -oE '"container":"[^"]+"' <run>/workflow.env.json | sort -u

Cache naming is that URI with `:// -> ..`, `/ -> _`, `: -> ..`, plus `.sif` and a
`.sif.verified` stamp. `/scratch/phyberos/imgcheck.sh` cross-checks all three arms at once.
This matters because **an absent image on a compute-node driver HANGS rather than failing**
— egress there is 0.14 MB/s — so it presents as a driver that looks busy. The driver's own
`CONTAINERS` list was missing das_tool for CAMI, ten of Pratama's twenty-one, and all three
of metaGEM's non-shared images.

## `results/` is a TARGET census, not a product census

`cami_contig_truth` reported COMPLETED, exit 0, and does not appear in `results/` at all,
while its real 128,677-byte gold standard sits in its work dir. "Check products, never exit
codes" still holds; the sharper form is that `results/` holds **targets**, so a genuinely
healthy intermediate reads as productless there. Check the step's work dir for an
intermediate.

## Size a pilot by binner viability, not by assembly time

A 200k-pair subsample assembles in minutes and **cannot validate binning**: 4,484 contigs
in 2.2 MB gave 6.37% readsWellMapped, negative coverage depths on 2 contigs, metabat2
exiting 0 with zero bins, and both MetaBAT2 and SemiBin2 failing their protocols. That is
the protocol behaving correctly — it refuses to ship an empty bin set — and the same shape
as the reference pipeline's `No bins with bin-score >0.5 found`. Such a pilot proves the
executor, the account, the binds, the staging and steps 1-9. Say so, and do not spend
attempts making a binner work on it.

## A snakemake tool's conda env name hashes its MOUNT PATH

snakemake 5.26 names a `--use-conda` env directory
`md5(realpath(conda_prefix) + env_yaml_bytes)[:8]`. VirSorter2 passes
`--conda-prefix <db-dir>/conda_envs`, so the env's NAME depends on where the database is
mounted. `downloadVirsorter2DB` builds it in a per-run work directory while
`virsorter2.py` always binds the product at `/db`, so the two names can never agree:

    md5("/db/conda_envs" + vs2.yaml) = 671930f2   what the run demands
    the staged DB contains             91185d68    built elsewhere

The run then tries to CREATE the env, which needs the network, and a compute node moves
0.14 MB/s — surfacing as `CreateCondaEnvironmentException: [Errno 2] ...` several screens
into `.command.err`, under three earlier rule errors that are consequences rather than
causes.

**Build the env at the path it will be USED from.** Bind the destination at `/db` during
setup. Conda envs are not relocatable — activation scripts and shebangs bake the prefix —
so computing the right name and renaming the directory does not work. Same shape as the
DRAM staging, which binds its output at `/db` so `DRAM.config` records `/db`-prefixed
paths natively.

**Do not reach for the tool's `--use-conda-off`-style flag without checking the image.**
VirSorter2 has one; the biocontainer's own env has no sklearn, pandas, numpy, screed,
prodigal or hmmsearch at all. It fails loudly, which is the lucky outcome — the classifier
is a pickled scikit-learn 0.22.1 model, so a half-satisfied env is the silent-wrong-answer
case.

Check this for ANY snakemake-based tool staged as a reference database before assuming a
staged database is portable.

## Audit every tool in an ensemble, not the first one

The per-binner parity audit against nf-core/mag had covered metabat2 only, and auditing one
binner of three feels like auditing the binners. Completing it found that **SemiBin2's seed
defaults to a system-chosen value**, so our SemiBin2 binning was not reproducible run to
run — the identical defect the campaign had already fixed for metabat2 and written up as "a
correctness fix rather than a tuning choice". Both are now unconditional.

It also found that **three apparent deviations were not deviations**: nf-core passes no `-s`
to metabat2 (200000 both sides), COMEBin's own `-b` default is 1024 and ours is
`min(usable_contigs, 1024)` (identical on any real sample), and DAS Tool's
`--score_threshold` is 0.5 on both sides because that is its own default. Establishing a
non-deviation is worth as much as fixing a deviation: guessed, each would have become a
false row in the deviations table.

Read the other pipeline's own config at the pinned revision, and each tool's own `--help`
from inside the pinned image. Not its documentation, and not a sibling tool by analogy.

## `bash -n` proves parsing, not expansion

A generated verification script contained ``echo "... the name `virsorter run --db-dir /db`
computes"`` — backticks inside a double-quoted string, i.e. command substitution, so the
step meant to PRINT a label would have EXECUTED virsorter. Syntactically valid, so `bash -n`
passed it twice. After generating a shell script, grep it for backticks and `$(` and confirm
each one is either intended or inside a `#` comment.

## Evaluate the CONSOLIDATED bin set; the individual binners are intermediates

When a consolidator runs, **the MAGs are its output**. A CheckM2 or AMBER number computed on
one binner's raw bin set is not a statement about any genome the pipeline reports — those
bin sets exist to be consolidated.

    das_tool consolidates      -> evaluate das_tool's bin set and table
    metawrap consolidates      -> metawrap consolidates INTERNALLY, so ITS bin set is the
                                  MAG set and IS evaluated (the cami `core` arm)
    aggregator + skani_dedup   -> the dereplicated output is the MAG set (Pratama)

So the criterion-12 comparison is DAS Tool against nf-core's `DASTool` rows, not per-binner
row against per-binner row.

**This is not a reason to stop PRODUCING the per-binner tables.** `das_tool` requires all
three, so they are produced and collected either way, which means a per-binner pass stays
available post hoc from artifacts the run keeps — `research/cami/score_reference_amber.py`
scores a two-column table out-of-plan in a format identical to the in-plan scorer's. Dropping
the six evaluation targets saved ~1,203 CheckM2 and ~1,203 AMBER tasks across the two CAMI
arms and lost no recoverable information.

**Do not "restore symmetry" because nf-core runs `post_binning_input = 'both'`.** That
setting exists so the pinned binners reach downstream tables at all, and nf-core publishes
its per-binner map unconditionally (`mag.nf:467`, `storeDir`) whether or not it QCs them.

**The trap this creates when a consolidator is DROPPED from an arm.** The Pratama arm's
MAG comparison reads the aggregator's ensemble while MetaWRAP — which is the paper's OWN
binner and that arm's real consolidator — is built, CheckM2'd and then never dereplicated
(`reproduction_map.md` row A11). Check what the comparison target actually descends from
before believing an arm compares like with like.

## Size a wrapper timeout to the TASK, never carry a pilot's cap into a rung

A rung died at exactly 3h00m00s from its own `timeout -k 30 10800` — the 30-minute pilot
convention scaled up and carried forward. Everything had completed cleanly; the only task
still running was healthy, and Nextflow's shutdown hook scancelled it, which `sacct` then
reports as `CANCELLED by <uid>` and reads like an external kill.

Two things follow. **A death at exactly a round number is a wrapper, not the workload** —
check your own invocation before the cluster. And **the wrapper is not the only wall**: the
task's own Slurm band applies independently, and fir's bands are 3 h / 12 h / 1 d / 3 d /
7 d, so a job whose own request sits at 3 h dies the same death from the other direction.

Also: measure elapsed from `sacct`, not from the driver log's submission line. Those differ
by queue time, and sizing a wall off the larger number hides how much headroom you have.

## CLOSED: a task DOES auto-route past fir's 3-hour band

Carried in three briefs as unproven. Confirmed 2026-09-12: job 59548173 requested
**12 cpus / 72 GB / Timelimit 16:00:00** and RAN — not rejected, not stuck pending on a
partition mismatch. fir's bands are 3 h / 12 h / 1 d / 3 d / 7 d, and a 16-hour request
landed above the first two without anyone naming a partition. **Do not name a partition
explicitly** — `sbatch` rejects the name outright and its own error says the scheduler
redirects automatically.

So a transform's declared `duration` may cross a band boundary without special handling,
and the earlier worry about keeping a duration under 3 h to avoid a band change was
misplaced. Recorded as a positive finding with its job id, because "we never saw one route"
is the weakest possible evidence and this campaign has been burned by that shape four times.

## CORRECTION: `dev/libraries.sh -bm` moves EVERY key. Rebuild metadata SCOPED.

This corrects an instruction given in several briefs, including one issued today. "Run
`dev/libraries.sh -bm` in your own copy to rebuild `_metadata`" is WRONG when you have added
a type to one library: that script runs `msm build all`, which rebuilds leaf instance-ids —
which hash path plus **mtime** — for *every* resource and transform directory, not just the
one you touched. So adding a type used by one arm moves the keys of every other arm, and the
mismatch looks like your change broke something unrelated.

Rebuild only the library you changed:

    PYTHONPATH="$PWD/src" python -m metasmith build transforms \
        -t src/metasmith_libraries/data_types \
        -r src/metasmith_libraries/transforms/<the one library>

If you have already run the full rebuild, the recovery is to restore the untouched
directories (`resources/env`, `resources/lib`, every `transforms/*` except yours) from a
clean checkout, confirm `diff -rq` is silent, then rebuild scoped. That is what B7 did after
following the bad instruction, and it is why the CAMI keys came back to baseline.

## `_stable_id` is only stable if the PATH STRING is

`_stable_id` deliberately hashes the path string rather than stat-ing the file, so an input
the workstation cannot see still keys consistently. That protection is only as good as the
string: if the registered path contains the checkout root, the id is **checkout-dependent**.

Measured: `run_metagem.py`'s `MEDIUM_TSV` default embeds an absolute checkout path into a
`_stable_id()` call, so the metaGEM keys differ in any copied tree while reproducing exactly
in the original — confirmed by running the identical command in both. **The metaGEM arm's
keys are therefore NOT portable across checkouts**, and comparing them between trees proves
nothing. Pre-existing, unrelated to any change today, and a real trap for anyone verifying a
copied tree against a baseline measured elsewhere.

## The "this plan fetches databases" warning under-reports. Do not use it as an inventory.

`DownloadSteps` (`nextflow_codegen.py:154`) qualifies a step only when **every** product's
type starts with `ref::`. So a download step whose product lives in another namespace is
silently omitted from the one message that exists to announce a plan's database cost:

    downloadVibrantDB      ref::vibrant_db             detected
    downloadCheckvDB       ref::checkv_db              detected
    downloadVcontact3DB    ref::vcontact3_db           detected
    downloadVirsorter2DB   annotation::virsorter2_db   INVISIBLE
    downloadDramDB         annotation::dram_db         INVISIBLE

Its own docstring says the difference between a 25-minute run and a 90-minute one "is worth
saying out loud before a run starts" — and it stays silent for two of the five download steps
in this library. **And those two are exactly the databases this campaign had to stage by
hand.** Not a coincidence: the awkward ones are awkward because they do not fit the `ref::`
shape, which is the same reason the detector misses them.

The reliable test for what a plan fetches is the step's `labels=["local"]` plus whether its
product arrives as a staged given — or simply grep the step list for `^download`. This
matters most on a compute-node driver, where an unfetchable step HANGS rather than failing.

Recorded as a known latent engine defect. The fix is a namespace-agnostic predicate — takes no
sample input, produces a reference artifact — but changing a codegen predicate while runs are
live is not worth it for a log line.

## Ask whether a containerised tool writes beside $HOME. Three transforms now needed this.

Inside these containers `$HOME` is `/home/phyberos` and it is **read-only**. Any tool that
writes a cache, config or scratch file beside `$HOME` on first use dies, and the error names
the symptom several frames before the cause:

    [Errno 2]  No such file or directory: '/home/phyberos/.cache/cobrapy'
    [Errno 2]  No such file or directory: '/home/phyberos/.cache'
    [Errno 30] Read-only file system: '/home/phyberos'

The fix is one line in the transform's command, `export HOME="$PWD"`, and it is now in **three**
transforms for the same reason: `logistics/downloadVirsorter2DB.py` (snakemake's package cache
follows HOME), `functionalAnnotation/virsorter2.py` (writes `$HOME/.virsorter`), and
`metabolicModelling/memote_score.py` (cobrapy writes `$HOME/.cache/cobrapy`). Three instances
is a pattern, not a coincidence: **add it pre-emptively for any new containerised tool**, since
it costs nothing when unnecessary and costs a whole array of tasks when it is.

Note the failure SHAPE, which is what makes it cheap to recover from and easy to misread:
memote failed ~100 tasks while all 131 CarveMe reconstructions succeeded, so the expensive half
of the lane worked and only the minute-per-model half broke. A flood of FAILED rows for one step
name is worth diagnosing before assuming the lane is broken.

And the counterpart trap, already recorded separately: the OPPOSITE configuration bites too —
when apptainer DOES bind `$HOME`, the host's `~/.local` shadows the image's packages and
surfaces as a missing `.so`. `--no-home --cleanenv --env PYTHONNOUSERSITE=1` is the other half
of this.

## Suppress a DIAGNOSED failure in a shared alarm channel. Never an undiagnosed one.

`memote_score` failed ~100 tasks per attempt, each retry arriving as a fresh Slurm job id that
no dedup can collapse. The cause was diagnosed to the error string and the fix was already
written, but the same watcher also carries OOM kills, node saturation, quota and every OTHER
Slurm failure — so a known alarm firing hundreds of times was **burying the unknown one, which
is the only kind worth waking for**.

Suppressed by job name, with the reason and a removal condition written at the site. The
distinction that makes this safe rather than negligent:

- **Diagnosed** — cause proven, fix written, outcome predicted. Suppressing keeps the channel
  signal-bearing, and the finding is already recorded elsewhere.
- **Undiagnosed** — suppressing is hiding, and the next reader inherits silence they cannot
  distinguish from health.

Write the removal condition into the suppression itself ("remove once the metaGEM lane is
relaunched"), or it outlives its reason and becomes a permanent blind spot — which is how a
watcher ends up not watching.

CAUTION a long-running bash script does not pick up edits to its own file. Stop and restart the
Monitor, or the suppression never takes effect and you will believe it did.

## Reading a RUNNING task: three instruments lie, one does not

A long-running task that looks deadlocked usually is not, and the checks that feel authoritative
here are the ones that fail. Measured on a live 48-cpu COMEBin at 6.5 h:

- **`sacct ... TotalCPU` reads `00:00:00` for any RUNNING job.** It is accumulated at step end.
  A zero says nothing about whether the process is burning CPU.
- **`sstat` returns a bare header on this cluster**, for every job, including a known-active
  positive control. It is not a usable instrument here at all — and the control is the only
  reason its emptiness does not read as "no CPU".
- **The work dir holds no products while a task runs.** A real step's outputs live in node-local
  `NXF_SCRATCH` until the unstage, so a product-based check — including this project's own
  documented COMEBin check, "watch for zero `cluster_res` files" — is a POST-HOC check and
  cannot be applied in flight. Their absence is not evidence of anything.
  There is also no `.command.err`; the file is `.command.log`.
- **The one reliable live signal is `.command.log`'s SIZE, MTIME and TAIL**, which streams back
  as the task runs. 1.14 MB with an mtime of *now* and a progress bar at
  `43/70 [01:35<01:00, 2.24s/it]` is proof of progress; nothing else available was.

## A Nextflow config SELECTOR beats the transform's own declaration, and the protocol sees the selector

Verified on a live task rather than reasoned about. `comebin.py` declares
`Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=4))`; the driver's appended
`withName: '.*__comebin' { cpus = 48; memory = '192 GB'; time = '3d' }` produced

    #SBATCH -c 48    -t 72:00:00    --mem 196608M
    .command.metadata first line:  res 48/192 GB/1

So the selector's values reach **both** Slurm and `context.params.get('cpus')`, and the
transform's declared 4 h never reaches the scheduler. Two consequences:

1. A transform's declared resources are **inert** wherever a selector matches its process name.
   Anyone reading the transform file to learn the thread count gets the wrong answer.
2. Because the protocol is told the selector's number, a tool that sizes its own thread pool from
   `params['cpus']` — COMEBin calls torch's thread setter with it — stays matched to the
   allocation. That is the good case; it is not automatic, and it is worth confirming from
   `.command.metadata` rather than assuming.

## A watcher that tails an APPEND-ONLY log re-fires on the previous attempt's terminal line

The H41 fetch watcher reported `H41 FETCH FAILED -- SHORT -- 6188474368/12125303198` while the
fetch was demonstrably gaining bytes: the process was alive and the file went
`6,581,743,616 -> 6,683,185,152` between two samples. The `SHORT` it matched was the FIRST
attempt's, still sitting in the log that the second attempt appends to.

**Scope every log-reading watcher to the current attempt.** `awk '/^START /{buf=""} {buf=buf"\n"$0}
END{print buf}'` keeps only the text after the last START marker. A bare `tail | grep` cannot tell
this attempt's failure from the last one's, and on a retried operation the last one's failure is
guaranteed to be there.

Two corollaries paid for by the same incident:

- **The pattern list must cover every terminal branch the script can print.** That watcher matched
  `SHORT` and `MD5 FAIL` and not `STALLED`, so a stall guard firing would have been reported only
  by the process-gone branch, several minutes later and with a vaguer message.
- **A stall guard tuned for a steady source is too tight for a flaky one.** ENA answered `http=000`
  one second after a successful 393 MB pass, so "two consecutive zero-gain passes" nearly stopped a
  working fetch. That is a real tension rather than a bug — the guard exists so a loop making no
  progress cannot LOOK alive — but the tolerance belongs to the source, not to the pattern.

And the distinction that makes an outer retry loop legitimate here, given this project's history of
corrupt resumes: **curl's own `--retry` re-sends from the offset it resolved at invocation start,
which is how a corrupt middle got written twice; a FRESH curl per pass recomputes `-C -` from the
file on disk.** The published md5 is the final gate either way, so a bad middle is never promoted.

## A THRESHOLD alarm on a persistent condition needs hysteresis, not just dedup

`watch_wave3.sh` emitted `INODE PRESSURE` on any reading above 850K. The quota then sat *stably*
at 854K, so the alarm re-announced the same unchanging fact every 7 minutes. That is worse than
silence: it is the mechanism by which a reader learns to skip the channel, and this campaign has
already lost a real DRAM-guard event to exactly that pattern.

**The distinction from the dedup rule is worth keeping separate.** Dedup ("remember what you
already said") fixes an alarm on a *discrete event* — a failed job id, a log line. A **threshold**
alarm is about a *continuous quantity*, and a quantity that crosses a threshold and stays there is
not one event: it is a state. So it needs **band-crossing**:

- emit when the value enters a new band (25K here), in EITHER direction, so a reclaim is as
  visible as a climb;
- emit a `CLEARED` line when it drops back below the threshold, because otherwise the last thing
  the channel ever said about the resource is that it was tight.

The general form: **ask whether the thing you are watching is an event or a state.** An event
wants dedup; a state wants bands and an all-clear. Getting this wrong in the noisy direction costs
the channel's credibility, and in the quiet direction costs the event.

    CAUTION a long-running bash script does not pick up edits to its own file, so every one of
    these fixes needs the monitor STOPPED and RESTARTED. That is now eleven instrument
    adjustments in this campaign, and the restart has been required by all of them.

## THE TEARDOWN METHOD DEPENDS ON HOW THE DRIVER WAS LAUNCHED

"Remove `PID.lock`, never `scancel`" is only half the rule, and I proved the other half the
expensive way: `scancel --batch --signal=USR1` on three engine-launched drivers gave
`State=FAILED`, left every `PID.lock` in place, produced **no cancel report at all**, and
orphaned **seven grid jobs** — two 9-hour COMEBins among them.

    submit_driver.sbatch  (bench route)       has `trap 'kill -USR1 "$child"' USR1`
                                              -> USR1 is GRACEFUL; the client catches it and
                                                 runs CancelWorkflow. Job ends COMPLETED with a
                                                 `{"method":"pidfile", ...}` report.
    METASMITH_DRIVER_SLURM=1  (engine route)  renders start.slurm.sh with NO USR1 trap
                                              -> USR1 is FATAL. The JVM dies without its
                                                 shutdown hook, and THE SHUTDOWN HOOK IS WHAT
                                                 SCANCELS THE GRID JOBS. Everything orphans.

**For an engine-launched driver: remove `PID.lock` while the driver is still ALIVE.** Its
supervisor then terminates Nextflow's process group, and Nextflow's own hook cancels the grid
jobs on the way out. Killing the driver first and removing the lock afterwards is the order that
guarantees orphans — which is exactly what I did.

**How to tell which route a run used, before touching it:** the bench route's driver job is named
for the lane (`probe_e2`, `mat_e3`) and its log lives under `/scratch/phyberos/bench/logs/`; the
engine route's is named `msm-driver-<key>`. If the job name starts `msm-driver-`, USR1 will kill
it outright.

    CAUTION you cannot capture an orphan's MaxRSS before cancelling it -- `sacct` does not
    populate MaxRSS for a RUNNING job, which is the same limitation that makes a long task look
    deadlocked. And a grid job that outlives its driver keeps the compute and LOSES the cache
    entry either way, so there is no stop order that preserves in-flight work. Decide whether
    you need the result BEFORE you stop the driver, not after.

## Teardown depends on the launch route AND on whether the job has STARTED

The launch-route half is already above: a lane launched through `submit_driver.sbatch` or
`run_e1.sbatch` stops with `scancel --batch --signal=USR1`, because both trap it, while an
engine-launched driver under `METASMITH_DRIVER_SLURM=1` does **not** trap it and must be stopped by
removing `PID.lock`. Sending USR1 to the second kind cost this campaign seven orphaned grid jobs and
three FAILED drivers with their locks left in place.

The second half, needed within four hours of the first and in the opposite direction:

**A job that has not started has no batch step to signal.** No driver, no grid jobs, no `PID.lock`,
nothing to wind down. `scancel --batch --signal=USR1` on a PENDING job errors and leaves it queued.
Use a plain `scancel`.

So the decision is two questions, in this order:

1. `squeue -j <id> -o '%T'` — PENDING? Plain `scancel`. Done.
2. RUNNING? Then the launch route decides: USR1 for a wrapper that traps it, `PID.lock` removal for
   an engine-launched driver.

Confirm by product either way. For a cancelled PENDING launch, the product is the **absence** of a
new run directory in the agent home — check it, because a launch that got as far as staging has
already written one and is no longer the pending case.

## A census whose filter IS the liveness test cannot see a half-torn-down run

The run census used `[ -f PID.lock ] || continue` to decide which runs were worth measuring, and
then the same file to decide which were live. A run whose lock had already been removed — by an
earlier teardown in the same window — was invisible to both, and its 10,142 inodes went unreported
until the reclaim measured them directly.

**Enumerate the runs directory, then test liveness per row.** One predicate must not do both jobs.
The same shape has now produced a false negative three ways in this campaign: a wrong-depth `find`,
a `pgrep` matching its own wrapper, and now a filter standing in for a measurement.

And when a census lists runs with no lock as "already torn down", do **not** read that as
"reclaimable". Three of those keys turned out to be the keys the next wave was about to relaunch
into, so their directories were live resume state for lanes that had not started yet.

## The apptainer cache holds three naming families, and only one takes the registry prefix

Cross-checking `images.txt` against `apptainer.cacheDir` reported **29 of 60 missing** and every one
was a false alarm. The cache contains all three of:

    quay.io-biocontainers-<tool>-<tag>.img                <- bare `biocontainers/x:y` in images.txt
    community.wave.seqera.io-library-<tool>-<hash>.img    <- already carries its host
    depot.galaxyproject.org-singularity-<tool>-<tag>.img  <- already carries its host

A bare `biocontainers/foo:tag` entry caches under `quay.io-...` because all five engine registry
settings (`apptainer`, `docker`, `podman`, `singularity`, `charliecloud`) are `quay.io`. A
derivation that drops the registry reports every bare entry as absent.

**The rule: prepend the registry for any entry with no host, then translate `/` and `:` to `-` and
append `.img`.** And the meta-rule this is the sixth instance of tonight — an empty or negative
result is a claim about your pattern before it is a claim about the world. Prove the instrument with
a positive control before believing a count of zero.

## A plan key is POOL-local as well as tree-local. Compare step counts across pools, never keys

Already recorded: a leaf instance id hashes path plus **mtime**, so two checkouts of the same commit
produce different keys for a byte-identical plan, which is why the launch checkout is frozen.

The narrower and more surprising half, measured tonight across three lanes: **a pool assigns its own
identity to a given when it imports it.** So the same driver, at the same commit, solving the same
targets, keys differently on the workstation than on fir — not because either tree moved, but
because the two pools imported the same givens under different ids.

    lane        local dry run     fir staged     steps
    E2 short    2NBvF7ug          WfOlaqLT       15 both
    E3          LEiHE7w5          Son2YJiI       26 both

**So a local dry-run key is not a prediction of the fir key and never was.** The comparable
invariant across pools is the **step count**, plus the bindings. Within one pool and one tree the
key is still exactly what `-resume` depends on, and within-tree repeatability remains the check to
run before a launch whose resume state is worth days.

This retires a piece of advice I gave three launch agents: "record the plan key before every submit
and compare it to the baseline." That is wrong twice over — a ramp rung must key differently because
it solves over fewer samples, and a key measured in another pool must key differently because the
pool minted its own ids. Both produce a mismatch that sends the reader hunting a tree that never
moved.

The three checks that are actually worth running, in order of what they catch:

1. **Step count against the expected shape**, which is portable across pools and trees.
2. **Bindings** — what each step consumes — which is the only thing that catches a plan that solves
   cleanly while answering a different question.
3. **Within-tree, within-pool key repeatability**, run once before a long launch, which is what
   `-resume` genuinely rests on.

## `sync.sh` must always be run from the SAME tree, or every plan key moves with no edit anywhere

The frozen-checkout rule protects against *editing* a tree that live runs depend on. This is the
same hazard arriving by a route that rule does not cover, and it needs no edit at all.

`sync.sh` copies the working tree with `rsync -a`, which **preserves mtimes**. A leaf instance id
hashes path plus mtime. So the mtimes of the `_metadata/index.yml` files in the destination checkout
are inherited from whichever tree ran the sync — and they determine every plan key solved from that
checkout.

Measured, and the gap is not subtle:

    fir:/scratch/.../checkout/f6d01f00/src/metasmith_libraries/.../amplicon/_metadata/index.yml
        mtime 1789276057  =  2026-09-12 22:07:37     <- the tree that synced it
    workstation:.../cami-run/src/metasmith_libraries/.../amplicon/_metadata/index.yml
        mtime 1789231877  =  2026-09-12 09:51:17     <- a different clean tree, same commit

**Two people with clean checkouts at the same commit produce two different sets of plan keys.** A
sync from the second tree would have silently discarded the resume state of every run staged from
the first, with `git status` clean and `diff -rq` over `src/` reporting nothing but `__pycache__`.

So: whoever ran the first `sync.sh` for a wave runs every later one. And the cheap pre-launch check,
which costs one command and directly tests the invariant, is to compare `_metadata/index.yml` mtimes
between the old and new checkouts before submitting anything:

    for l in amplicon aspire kbase pangenome; do
        stat -c %Y <old>/src/metasmith_libraries/transforms/$l/_metadata/index.yml
        stat -c %Y <new>/src/metasmith_libraries/transforms/$l/_metadata/index.yml
    done

Equal mtimes mean the keys hold. This is also why a fresh `git worktree add` is the wrong vehicle
for a sync even after copying the solver binary in: it has no `_metadata` at all, and building it
there stamps fresh mtimes, which is the same failure with an extra step.

## Print the absent case explicitly in a watcher

A watcher that greps for a value and prints nothing when it is missing cannot be distinguished from
a watcher whose pattern is wrong — and tonight that ambiguity produced six false readings in one
session: a truncated `sacct` field width, a `^`-anchored grep against an indented assignment, a
`-S 2026-01-01` window that returns one row where `-S 2026-09-01` returns 27,520, a `.command.sh`
flag audit against a file that never holds flags, a sweep of the driver's sbatch log for Nextflow
lines that live in `<run>/_metasmith/logs.*/agent.log`, and a cache cross-check that dropped the
registry prefix.

So emit the negative as a value: `NO-GRES-LINE`, `NO-ACCOUNT-LINE`, `UNREACHABLE`, `absent`. And put
a positive control beside any count that could be zero — the row count you scanned, a known-present
match — so a zero is attributable to the world rather than to the instrument.

## A staged run holds TWO copies of a masked transform, and only one of them executes

Checking whether a pinned transform actually reached a run, `find <run> -name memote_score.py`
returns two files with different contents:

    _metasmith/task/transforms/<id>/memote_score.py           1,644 B   the PINNED one, EXECUTES
    _metasmith/task/data/<id>/modelling/memote_score.py       2,414 B   the STANDARD one, a payload

The second is staged because the library itself is a `lib::modelling` **given** — the whole library
directory ships as data. It is not what runs. So a `find ... | xargs grep -c HOME` over the run
returns a mix, and whichever copy the shell happens to list first decides what the check appears to
say. **Landing on the data copy reports a working pin as broken**, which makes this the one
path-shaped false reading that fails in the reassuring-to-alarming direction rather than the reverse.

**The discriminator is the path, not the filename:** `task/transforms/` executes,
`task/data/<id>/<library>/` is a payload.

The cleanest single check is neither, though — it is the **step's signature hash**, because that is
computed from the file that will actually run:

    grep -oE 'signature [^ ]*' <run>/workflow.step_N.meta
    ...:lXhIC7fX9oWq   the pinned protocol
    ...:3yvv1z2WknpT   the standard protocol

Two related lines that do NOT discriminate, and will mislead if used:

- The solver's `[type] resolved by [library]` line names the library that **declares the type**, not
  the transform that was chosen. It still names the standard library after a mask.
- The **plan key and transform key do not move** when a protocol is pinned. A pin changes what runs,
  not the plan's identity, so a key comparison reports "no change" on a successful pin.

## `lfs quota` reports KiB, and the /scratch cap is 18.63 TiB. Say TiB

The enforced project quota, with its own header so nothing is inferred:

    lfs quota -p 83115734 /scratch
    Filesystem   kbytes       bquota       blimit       bgrace  files   iquota   ilimit
    /scratch     16635286996  20000000000  20000000000  -       335888  1000000  1000000

    field 2  kbytes  = KiB USED
    field 4  blimit  = 20,000,000,000 KiB  <- the cap, and it does not change
    field 6  files   = inodes used
    field 8  ilimit  = 1,000,000

    20,000,000,000 KiB / 1024^3 = 18.63 TiB   (= 20.48 TB decimal)
    bytes as TiB: field2 / 1048576 / 1024     and CALL IT TiB

`diskusage_report` agrees and rounds to `15TiB/ 19TiB`.

**Two wrong labels this produced, both mine, in the same session.** "12.48 TB of 20 TB" quoted the
raw `blimit` as though 20,000,000,000 were 20 TB — unconverted. "15.46 TB of 18.63 TB" did the
arithmetic right and printed TB where it is TiB. **The percentage was correct both times, because a
ratio is unit-free — which is precisely why the error survived twice**: the number being reasoned
with was right while the unit printed beside it was not.

So a quota figure gets quoted as a percentage *and* an explicitly-TiB pair, never as a bare "of 20".

## A safety grep must exclude comments and echoes, or it cries wolf on its own reassurance

A list-only census script was checked for destructive verbs before it ran, and the check reported
`-delete` = 2 and `truncate` = 2. All four occurrences were the script's own comment lines saying it
contains neither, plus a closing `echo "NOTHING WAS DELETED..."`. **A script that documents "no rm
here" fails a grep for "rm".**

The check that actually discriminates:

    grep -nE -- '-delete|truncate|rm ' <script> | grep -vE ':\s*#|echo '

and assert that count is zero. Keep the prose — the comment is worth more than the false positive
costs — but make the grep aware that prose exists.

## Count how many tasks share a property before reporting it. An array stub is not a task

Twice in one session a real property of ONE task was asserted of its population and sent someone
hunting a defect that did not exist.

**The megahit scratch case.** One task's `.command.run` read `NXF_SCRATCH=''`, which contradicts the
preset and implies a k-mer graph being written to Lustre. Reported as confirmed, twice. Then counted:

    plain megahit   scratch ON = 208   OFF = 3
    the sampled task: #SBATCH -J nf-p04__megahit_(101)   #SBATCH -o /dev/null

**The three with scratch off are the array-chunk stubs**, at indices 1, 101 and 201 — the same
pattern `squeue` shows for every batched step. `-o /dev/null` is the tell: a real task writes
`-o <workdir>/.command.log`. The stub's FILES manifest also listed ten-plus upstream work dirs,
which is nonsense for a per-sample assembly and exactly right for a batch wrapper enumerating its
members — a signal that was in the first output read and went unquestioned.

**The flye case, an hour earlier.** The preset defect was real and byte-identical in two standard
transforms, and the live lane was asserted to be exposed without checking what it had staged. It
runs a pinned transform with a platform-keyed table.

**The guard, one command, before any per-task claim leaves this session:**

    for d in <run>/nxf_work/*/*/; do
        j=$(grep -oE '^#SBATCH -J nf-p[0-9]+__<step>[a-z_]*' "$d/.command.run" 2>/dev/null | head -1)
        [ -n "$j" ] || continue
        ... classify, and COUNT both classes ...
    done

Report "3 of 211, all array stubs" rather than "megahit runs without scratch". And treat these three
as non-tasks in every census: an array stub has no products, no meaningful resources, and a
`.command.run` whose manifest describes the batch rather than a sample.

**Corollary that bit the prune gate the same hour:** counting *work dirs* over-counts tasks, because
a retry gets a fresh dir. `fastqc_trimmed` had **211 dirs all at exit 0 for 208 distinct task
indices**. A gate written as `dirs >= 208` would have authorised a 625 GB deletion while fewer than
208 samples were finished. Count **distinct task indices with exit 0**, taken from the `(N)` in the
job name.

## Decompose a derived number before you give it a name

A watcher computed `(work_dirs - distinct_successful_indices)` and printed it as `+Nretry`. The
arithmetic was right; the name was invented. The residual actually contains three different things:

    megahit: 211 dirs  =  193 ok  +  15 running  +  3 array stubs  +  0 failed

So `+22retry` meant "22 tasks I have not classified", and calling them retries sent a peer to gather
`sacct` MaxRSS and TIMEOUT evidence for OOM failures that **do not exist** — zero megahit dirs in
that run carry a non-zero `.exitcode`. It also retroactively falsified an earlier claim that three
`fastqc_trimmed` tasks had two successful dirs each; those three were the array stubs.

**The rule: a label is a claim.** If a number is `total - known`, report the components, not the
remainder. Classify every member explicitly and let each class carry its own count:

    stub     `#SBATCH -o /dev/null`, an array-chunk wrapper at index 1/101/201 -- not a task
    running  no `.exitcode` yet
    ok       `.exitcode` 0
    failed   `.exitcode` non-zero  -- and this one gets its own line, with index, code and path

This is the sibling of *count how many tasks share a property before reporting it*, one level along:
that rule stops a single sample being generalised, this one stops an unexplained remainder being
named. Three of this session's false alarms came from these two failures — the flye live-exposure
claim, the megahit scratch defect, and the megahit retries — and in each case the underlying
observation was accurate while the sentence built on it was not.

## Never pipe an inventory through `head`

A cache census was submitted with each invocation piped through `head -60` to keep the log readable.
The cami pool holds **5,854 shards**; the cap truncated it to roughly three entries per grouping, and
the log looked like a successful run. Nothing in the output said it had been cut.

For an inventory, write the FULL output to a file on scratch and aggregate there, returning only the
summary. A cap belongs on what you *read*, never on what you *collect* — and if a cap is
unavoidable, print the unclipped count beside the clipped list so the truncation is visible.

## A null intersection proves the sets are disjoint, not that one holds copies of the other

Asked whether a run's products were duplicated between its `nxf_work` and the task cache, the test
was: collect the inode set of `*.fq.gz` on each side and intersect them.

    nxf_work    208 files    625.5 GB
    task_cache  360 files  1,044.1 GB
    SHARED INODES: 0

That was reported as "every product is stored twice as separate bytes", and a 625 GB deletion was
proposed on it. **Zero overlap is equally consistent with the two sets being entirely different
files**, which is what these were: the cache's `.fq.gz` are *other, superseded runs'* products plus
the imported split reads, and not one of them is a copy of anything in `nxf_work`.

The census settled it: `lineage e2::trimmed_short_reads` is **one entry, 4.23 GB, belonging to run
`Gu9VJmwO`** — a superseded run — against 208 finished fastp tasks in the live run. So the live run's
products exist in `nxf_work` **only**, and deleting them would have destroyed 208 of 208 rather than
freeing a duplicate.

**To show two stores hold copies of the same logical object you must match the objects first** — by
name, by content hash, or by the index's own record of which run produced what — and only then ask
whether the matched pair shares an inode. Inode identity answers "hardlink or copy" for a pair you
have already established is a pair. It cannot establish the pairing.

Same shape as the other partial-view errors this session, but worth its own entry because the
measurement was correct, the arithmetic was correct, and the conclusion still did not follow.

## A passing positive control validates the instrument, not the CHOICE of instrument

Asked whether a live run's promoted cache shards were reachable, the test was `metasmith cache
explain <key>`. It returned `found: False` on five shards whose directories, manifests and product
files were all visible on disk. A positive control on an indexed key returned `found: True` — so the
command worked, the key format was right, and the negative looked earned. It was filed as an engine
defect with campaign-wide consequences.

**Two paths read that store, and `explain` is the one a human uses.**

    cache explain / cache list   -> ops/cache.py -> CacheStore.probe   reads the SQLITE INDEX
    a running task               -> metasmith.caching.invocation.probe reads ONLY THE SHARD
                                    (no tombstone file, a manifest, every listed file present)

Index rows are written at `caching/promote.py:record_run`, when the run **ends**. So `found: False`
on a live run's shard is by design. Measured with the runtime's own predicate:
**5 of 5 shards PROBE=HIT**, manifest lists 3 files, `out/` holds those 3, no tombstone.

**The control proved `explain` works. It could not prove `explain` was the right question** — and
because it passed, it made the wrong question feel settled. That is the opposite failure from the
rest of this session's instrument defects: there the instrument was broken, here it was healthy and
pointed at the wrong thing.

So a reachability claim gets tested with **the predicate the consumer actually calls**. Find the
caller first — here `nextflow_codegen.py:33` and `Orchestrator.groovy:621` name
`metasmith.caching.invocation` explicitly — and reuse its function rather than reimplementing or
substituting a sibling.

Two related stumbles from the same hour, both cheap to avoid:
- `manifest.cbor` is **CBOR**. A text grep for quoted filenames returned `listed=0` for manifests
  that list 3. Use the engine's `read_manifest`.
- `invocation.probe(root, key)` takes **bytes**. A hex string raises
  `'str' object has no attribute 'hex'` — which is an instrument error, not a cache miss, and must
  never be reported as one.

## A rate needs a long BASELINE, not many points. Two readings 2 h apart beat six over 6 min

**First time, bytes.** A 120-second delta read **2.36 TiB/h** and I escalated it as "1.1 hours to
full". The experimenter's six one-minute samples showed a net of **+0.13 TiB/h**, with individual
minutes swinging **±9 TiB/h**, because tasks write large temporaries and delete them. The fix was a
least-squares slope over ≥12 minutes that reports nothing until its window is full.

**Second time, inodes, about two hours later.** Two readings forty minutes apart — 457,277 →
530,616 — gave **~110K/h** and "roughly 4 hours of headroom". The experimenter's five readings over
90 minutes gave **~50K/h** and ~8 hours. Same error, same shape, on the other axis, after having
been shown it once.

**The rule: a rate needs a fitted slope over a window long enough to average the write-and-delete
cycle, and on this filesystem that is ≥12 minutes.** A two-point delta on any quota axis here is not
an estimate, it is a sample of whichever phase the tasks happened to be in.

**REFINED 05:10, and the refinement matters because my first phrasing was wrong.** The experimenter
later measured 334K -> 573,091 inodes over **2.3 hours** = ~100K/h, which vindicated my 40-minute
110K/h and retired their own 50K/h (taken from gappy monitor bands). So **the problem was never
"two points" -- it was TOO SHORT AN INTERVAL.** Two readings across a long baseline are an excellent
estimate; twelve readings across six minutes are noise. What a fitted slope buys on a short window is
resistance to the write-and-delete swing, not accuracy from sample count.

    The right rule: BASELINE >> the period of whatever oscillates. Here tasks write large
    temporaries and delete them on a scale of minutes, so a baseline of hours is reliable with two
    points, and a baseline of minutes needs a fit and is still weak.

I had generalised one correction into a rule broader than the evidence supported, which is its own
error and worth noticing: **being corrected once is not licence to over-apply the correction.**

And the reason it recurs is worth naming: **a two-point delta feels like a measurement** because both
endpoints are real. Nothing about the arithmetic signals that the interval is too short. The guard
has to be structural — refuse to compute a rate until the window is long enough — which is why the
watcher stays silent below 12 samples rather than reporting a provisional figure.

    BOTH AXES NOW MOVE FOR THE SAME REASON, which is why one watcher should cover both: a binner's
    bins land in the work dir AND are promoted to a shard, so every bin costs two inodes as well
    as twice its bytes. Watching bytes alone missed inodes becoming the tighter axis.

## Size a reclaim lever on the axis that binds, not the one you measured first

E4 chunk 1's entire `nxf_work` is **18.18 GB across 5,900 task dirs** — useless as a byte lever
(under 1% of headroom, a third of one bowtie2 BAM) and excellent as an **inode** lever. E2 long's
chopper is the mirror image: **111.3 GB in 41 dirs** — a real byte lever worth almost no inodes.

So the candidate list has to be re-sorted whenever the binding axis changes, and the two orderings
are close to inverted. Inode levers are many small dirs; byte levers are few large files.

A clean chain also makes the gate trivial. E4's is a line with no fan-in — `prodigal_from_bin →
carveme_from_orfs_cplex → memote_score`, each consumer reading exactly **one** upstream dir — so a
producer's dirs are prunable the moment its single consumer exits 0. Check the fan-in before building
a gate: a step with one consumer needs a far simpler gate than one with three.

## A work dir is also RUN-END STATE. Never delete a task's `.command.cache` before its run ends

`caching/promote.py:record_run` builds the cache index at **run end** by globbing
`nxf_work/**/.command.cache` — one record file per task. For each record it indexes the shard, copies
the task logs into the shard, and writes that member's `InvocationEvent` to
`_metasmith/trace.jsonl`.

So a mid-run prune that removes `.command.cache` leaves shards that:

    still serve lookups        -- `invocation.probe` reads the shard, not the index
    are never INDEXED          -- `cache list`, `gc` and any index-based accounting miss them
    get no TRACE events        -- which the lineage report and `collect.py`'s TraceIndex read

That is a silent loss of *bookkeeping*, not of data, which makes it the hard kind to notice: the run
finishes, the products are there and reachable, and only the reporting is short.

**The rule: a mid-run work-dir prune must EMPTY the dir and KEEP `.command.cache`.** That is ~2 inodes
per task instead of ~13, so it still works as an inode lever — roughly 85% of the saving for none of
the risk.

    This also sharpens the earlier B16 lesson rather than replacing it. There, `explain` reading
    the index while the runtime reads the shard meant a shard missing from the index was still
    reachable -- true, and it is why the products survive. What it does NOT cover is that the
    index and the trace are BUILT FROM THE WORK DIRS at run end. So "the shard is reachable"
    answers the data question and says nothing about the bookkeeping question, and those are
    separate.

**Generalise it: before deleting anything a run wrote, ask what runs at run END.** A work dir looks
like pure intermediate storage right up to the moment something globs it.

---

## A resource ladder lives in TWO generated files. Grepping one is not checking.

`grep "withName: '.*<step>'" workflow.config.nf` returning nothing does **not** mean a step has no
`task.attempt` ladder. There are two places a ladder can live and that grep sees only one:

    workflow.config.nf      driver-side `withName` selectors -- what the DRIVER pinned
    workflow.resources.nf   generated from the TRANSFORM's own `Resources` declaration

B18 was raised as "metaSPAdes has no retry ladder, so the largest samples are silently lost". The
grep was correct — there is genuinely no driver-side selector for spades — and
`workflow.resources.nf:38-41` carries `memory = { (2**(task.attempt-1)) * 192 GB }` all along. The
retry doubled to 384 GB and ran.

**So: read BOTH files, and prefer the run's own rendered `--mem` on the retry attempt to either.**
And beware the adjacent trap that made the wrong answer feel confirmed — a flat `memory = '192 GB'`
was present in `workflow.config.nf`, under `withName: '.*__comebin'`. A literal that matches the
number you expect, under a selector for a different step, reads exactly like the pin you were
looking for.

## Map a Slurm array index to a nextflow task index through `nxf.log`, NEVER by arithmetic.

`59636440_5` is **`p05__spades_pratama (6)`**, not (1) and not (5). The array suffix and the nextflow
task index are independent numberings, and there is no offset that converts one to the other
reliably.

The cost of assuming they correspond: I read a work dir as "task (1)'s retry, rendering an identical
`--mem`, so the ladder is inert". It was task (1)'s **first attempt** — a different sample, whose
196608M was correct. One wrong mapping turned a working ladder into a fabricated data-loss blocker.

    grep the run's nxf.log for the array job id to get the task index, then read THAT task's dirs.

This is the fourth member of a family already in these orders: `pNN` is a per-run plan index and
carries no agent home; a job's owner cannot be read off its name; `sacct --name=nf-p08__clean`
returns nothing because Slurm stores the array suffix. **Every identifier in this stack is scoped to
something, and the scope is never the thing you want it to be.**

## A file count is not an inode count wherever hardlinks are possible.

`find <dir> | wc -l` counts path entries. The Lustre project quota counts **inodes**, and a
hardlinked file is one inode with several names — so unlinking names frees nothing until the last
one goes.

This matters most for conda trees, which is exactly where reclaim candidates accumulate: conda
normally builds an environment by **hardlinking out of its package cache**, so deleting the cache
after building an env can free almost nothing while `find | wc -l` promises tens of thousands.

    find <dir> -type f -links +1 | wc -l     0 means the tree is a full copy; the count is real
    find <live-tree> -type l -lname '*<dir>*' | wc -l   0 means nothing points back into it

Both were 0 for `_vs2_stage`, so its 32,551 was genuinely reclaimable. Run both before quoting a
reclaim figure, and remember the already-recorded companion rule: **on a live filesystem the size of
a deletion is what the deleting tool reports, never what the quota moves** — Lustre's accounting lags
a large unlink, and a job that samples the quota in the same second reports a negative saving.

## Write the numbers down, THEN delete the tree.

A run directory stops being evidence the moment its measurements are in a ledger, and not before.
`iy8YLaGr` was deleted only after its CheckM2 law (51 bins → 63 inodes, 57 → 69) was recorded;
`5vqR1dv8` was inventoried before release and turned out to hold **the only COMEBin bin count this
campaign has** (104) plus the first measured magnitude of the `--minContig` confound (43 bins at
2500 against 51 at 1500 on the *same sample*).

**Before endorsing any run-directory deletion, list its `results/` and count each product.** A tree
that looks like spent intermediate storage can be the sole carrier of a number no ledger holds, and
the check costs one command.

## When a count does not match, doubt the IDENTIFICATION before you doubt the count.

Twice in one shift a mismatched count was the cheapest available signal that I was looking at the
wrong objects, and both times I filed it as a loose end instead.

    the abort said "1 files name <run>"      my grep found 3 in task_cache
      -> the gate does not scan task_cache at all; it matched the run's OWN workflow.nf

    the cache held 70 large fq.gz            my interleave tree held 66 files
    705.31 GB                                744.04 GB
    exact byte matches between them: 1 of 66
      -> they were never the same product. 65 of the 70 are bbduk `clean_short_reads`,
         the assembler's INPUT, with 65 metaSPAdes tasks live against them.

In both cases I had a correct measurement, an off-by-a-few count, and an explanation for the gap
("cached task logs also name it", "different gzip settings compress differently"). The explanation
is the tell: **if you are reaching for a reason the counts differ, the likelier reason is that the
two sets are not the same set.**

## IDENTIFY A CACHE SHARD FROM ITS MANIFEST, NEVER FROM ITS FILE SHAPE.

Every shard carries `manifest.cbor` beside its `out/`, and `strings` is enough to read it:

    istep_name<name>          the step that produced it        e.g. bbduk_pratama
    jdtype_name<ns::type>     the product type                 e.g. sequences::clean_short_reads
    grelpath                  out/<the product filename>
    gslot_id / gparents / glineage / hconsumes / olineage_payload

I identified 70 shards as interleaved reads because they were ~11 GB `.fq.gz` files and the
interleave tree has the same size profile. They were bbduk's trimmed output — the direct input to
megahit and metaSPAdes — and recommending their eviction while 65 metaSPAdes tasks were running
against them was one dry run away from being acted on.

**A file's extension and size describe its shape. Only the manifest says what it IS.** The same
applies to the reverse direction: `find task_cache -iname '*downloadX*'` returns zero for every
step, because the cache is content-addressed — a name search cannot find anything in it, and zero
reads exactly like nothing being cached.

## An ARRAY JOB'S PARENT DIRECTORY has a `.command.err` that belongs to no task.

Diagnosing the E2-long metabat2 failure, I read `ce/d1811d65…` and found:

    .command.err   60 bytes -- `grep: write error: Broken pipe` / `tr: write error: Broken pipe`
    .command.out   29 KB -- the NODE RELAY's aggregate log: several `apptainer exec` blocks with
                   different `.bounce.*` files, two metabat2 tags probed ten seconds apart, and
                   an `'active' file was deleted`

**None of that is one task's output**, and every line of it looked like evidence. I built two
hypotheses on it — an image-availability failure and a relay-guard abort — and both were wrong. The
real cause was in the task's own dir: jgi reporting `1826529 reads and 231 readsWellMapped`.

**Then my second attempt mapped indices to dirs through nxf.log's `submitted process` lines** and
produced two directories with no `.exitcode` and no `.command.sh`, whose only error text was
`Error: Failed to append to file: $trace_file` — literal **unexpanded** shell template text, not a
failure.

    Map a task index to its work dir through nxf.log's  `Task completed > ... work-dir=`  line.
    Never through `submitted process`, and never by reading the array parent.

Companion to the rules already here: a Slurm array index is not a nextflow task index; `pNN` is a
per-run plan index; and `sacct --name=nf-p08__clean` returns nothing because Slurm stores the array
suffix. **Every identifier and every directory in this stack is scoped to something, and the scope
is never the one you want.**

## ONE nf-core table, TWO contig-id formats: DAS Tool writes the assembler's full FASTA header.

`GenomeBinning/contig_to_bin/contig_to_bin_map.tsv` is gathered by `collectFile` from every
binner, and they do not agree on what a contig id is:

    DASTool    k141_10 flag=1 multi=2.5908 len=1055     <- MEGAHIT's FULL header, spaces and all
    MetaBAT2   k141_100008                              <- the bare name
    COMEBin / SemiBin2   the bare name

`assembly_id` is identical for all four, so a membership test against the assembly's contig
**names** drops every DAS Tool row while the other three pass. In the reference scorer that silently
cost the one label that matters — DAS Tool's refined set IS the reported MAG set, per the
principal's correction that the per-binner sets are intermediates.

    split the contig id at whitespace and take the first token, before any membership test.

**And DAS Tool emits an explicit `…-DASToolUnbinned-….fa` pseudo-bin.** Once the ids match, scoring
it creates one enormous bin holding every contig refinement rejected — the same hazard as the
empty-`binner` rows, wearing a name instead of a blank. Drop any `bin_id` containing `unbinned`.

Measured on the rung-1 table, with every row accounted for:

    193,896 rows = 144,998 kept + 48,898 unbinned
    COMEBin 62,459/62,459 · MetaBAT2 34,562/34,562 · SemiBin2 31,306/31,306
    DASTool 16,671/65,569        <- the rest is the Unbinned pseudo-bin, correctly dropped

So DAS Tool's refinement keeps about a quarter of what the raw binners assigned. That is a result,
not a defect, and it is only visible once the ids parse.

## A PER-GROUP TALLY, NOT AN AGGREGATE, IS WHAT CATCHES A LOST GROUP.

The scorer printed `128327 rows over 3 binners … dropped 0 unbinned, 65569 not in this assembly`.
Every number in that line was correct. Nothing failed. It emitted three labels and read as a
complete comparison.

**An aggregate drop count cannot show that ONE group lost everything.** The fix is a per-group
kept/seen line plus an assert that refuses to proceed when a group present in the input contributes
zero rows:

        COMEBin: 62459 kept of 62459
        DASTool: 16671 kept of 65569
    assert not empty, "dropped EVERY row of [...] while keeping other binners"

This is the same shape as the campaign's other silent half-answers — a completion check that counts
started tasks, a directory created before it is populated, `Task completed` with `exit: 1`. **Any
instrument that splits its input into groups must report per group and fail on an empty one.**

## AN IMPORT REGISTERS A FILE IN PLACE. Its `size_bytes` in the index is NOT cache-resident bytes.

`_entry_rows` reports 742.1 GB of `sequences::short_reads_pe` in the pratama store. Physically inside
that store:

    *.fastq.gz  n=1    0.00 GB
    *.fq.gz     n=74   705.31 GB      <- bbduk's clean_short_reads, a different product entirely

Every imported row's `path` points **out** of the cache and at the original tree:

    34 rows -> /scratch/phyberos/pratama2026/interleaved/reads_2019/...
    34 rows -> /scratch/phyberos/pratama2026/interleaved/reads_2022/...

**So a large import total is a pointer, not a copy.** Deleting the source tree would destroy the only
copy of the data AND orphan every index row referencing it. `evict_cache.py`'s refusal to touch
imports — *"an import may be the only copy of its data"* — is literal.

**And the consuming tasks read the tree directly**, not a shard: `p02__bbduk_pratama` reads
`interleaved/reads_2022/SRR32696711.fastq.gz`, `p01__seqkit_reads` reads
`interleaved/reads_2019/ERR3858121.fastq.gz`, both located by `-J nf-<step>` in `.command.run`.

**This pair of trees looked like a duplicate TWICE and was not, for two different reasons:**

1. The cache's 705.31 GB of `.fq.gz` looked like the interleaved reads by size and extension. It is
   bbduk's trimmed output — **the live assembler input**.
2. The index's 742.1 GB of imports looked like a second copy by byte total. It is the same bytes,
   counted through a `path` that leaves the cache.

The discriminator both times was **reading what the manifest or index NAMES** — `dtype_name`,
`step_name`, `path` — rather than comparing sizes. A byte total tells you nothing about where the
bytes are.

Companion accounting note: 68 index rows covered **65 distinct filenames** (three duplicate rows)
against 66 files in the tree, one of which was never imported. Reconcile the counts before drawing
any conclusion from them — see the count-mismatch rule above.

## A TOOL'S OWN SUMMARY STATISTIC IS NOT THE MEASUREMENT YOU NEED.

To pick `jgi_summarize_bam_contig_depths`' `--percentIdentity` for ONT reads I needed read-to-contig
identity. Flye prints `Alignment error rate: 0.228` in a log I was already reading, so I used
`1 - that` and recommended a floor of 75.

Measured from the BAMs instead — primary mapped only, `100 * (1 - NM/aligned_length)` over the CIGAR's
M/I/D/=/X:

    Flye's error rate implied    identity 76.8-80.1%
    the BAMs actually say        identity 85.8-88.0%   (p5 83.1-86.2, p95 88.2-90.0)

**Off by 6-8 points, one-directionally.** Flye's figure is computed during its own consensus over raw
reads and charges unaligned or clipped portions that an aligned-block identity does not. The
recommendation built on it was wrong, and — worse — it could not have surfaced the finding that
decided the value: at a floor of 85 the plant dataset loses **22%** of its reads while the gut dataset
loses **2%**, a dataset-dependent coverage bias inside one lane. A mean, from either source, hides
that entirely.

    the proxy was convenient, already in front of me, and named the right quantity in words.
    None of that makes it the same statistic.

**And state the definition whenever you report an identity**, because there are several and they
differ by points: over the aligned block (soft clips excluded) versus over the full read length,
with or without indels in the denominator. The number is meaningless without it.

Companion to the Q40 rule: NanoSim's flat Q40 string misled the flye preset and this identity filter.
This one is a level up — **the misleading number was a tool's own summary of the data, not the data.**

## PICK A THRESHOLD FROM THE DISTRIBUTION, NEVER FROM THE MEAN.

Half the reads sit below the mean by construction, so a floor set near one discards about half the
data. The experimenter refused a mean-derived value for exactly this reason and was right to.

What a distribution gives you that a mean cannot:

    - the fraction retained at each candidate floor, which is the actual decision variable
    - whether a candidate sits near an edge (80 is ~3 points below plant's p5: safe)
    - whether one candidate is ASYMMETRIC across datasets, which is what ruled out 85

Report p5/p10/p25/p50 and the retained fraction at every floor under consideration. Sample a couple
of thousand reads per unit — 2% of a BAM was 13k-20k reads and the two samples per dataset agreed to
two decimal places, so precision was never the constraint.

## RENDERED cpus/memory/time ARE NOT IN THE CACHE KEY. A driver resource change costs nothing.

`caching/invocation.py:76-86` hashes exactly:

    { "v": CACHE_KEY_VERSION,   # 6
      "tk": transform_key,
      "sig": signature,
      "s": slot_key,
      "b": int(branch),
      "up": sorted(set(upstream_slot_ids)) }

and `signature` is built at `models/workflow/cache_decisions.py:41` as
`f"{step.transform._hash}:{protocol_sig}"` — **both properties of the transform SOURCE**, not of what
the driver rendered.

So the boundary is clean, and it is the one that matters when a run needs resizing mid-campaign:

    a driver-side resource change (withName, --mem, cpus, a narrower width)  ->  invalidates NOTHING
    editing `Resources(...)` INSIDE a transform                              ->  retires that
                                                                                 transform's shards

**A driver can be restarted with different resources for free**, keeping every promoted shard. This
retrospectively explains why COMEBin's 4-hour declaration could be overridden on a live task without
the cache noticing (the B13 withdrawal).

Companion to the already-recorded rules that a **transform edit** retires shards while **engine code**
and **driver source** move no plan key: three different artifacts, three different blast radii, and
only the transform source touches the cache.

## A CONTIG-OVERLAP CHECK CANNOT DETECT A CROSS-SAMPLE PAIRING. Assert the `@SampleID`.

MEGAHIT names contigs `k141_<N>` **independently in every assembly**, so two unrelated assemblies of
one corpus share almost all of their contig NAMES while sharing none of their sequences. Measured on
two CAMI rung-10 samples:

    correctly paired gold standard vs das_tool table:  16,392 of 16,393 shared   (99.99%)
    the WRONG gold standard        vs the same table:  16,351 of 16,393 shared   (99.74%)

**0.25 percentage points apart.** In B21 that cost nine of ten samples their AMBER score with no
error raised: the scorer joined on name, found near-total overlap, and compared one assembly's bins
against another assembly's truth.

    the overlap check is NECESSARY and NOT SUFFICIENT. The only field that distinguishes a
    correct pairing from a colliding one is the gold standard's `@SampleID`.

**So:** anything that accepts a **pre-built** gold standard must assert its `@SampleID` names the
same assembly the prediction came from. Anything that **builds** its own gold standard from the
sample's own truth and BAM is safe by construction — but its overlap assert is not what makes it
safe, and it should not be cited as though it were.

This corrects a claim made repeatedly in this campaign: that the BAM-versus-assembly overlap assert
in `score_reference_amber.py` is "the guard that cannot be forgotten". It can be passed at 99.7% by
exactly the error it was written to catch.

## `group_by` PINS ONE INSTANCE. `parents={...}` DOES NOT.

Measured in B21 from nine tasks' own `consumes` records, in a transform whose table and gold
standard carry the **identical** `parents={asm}` declaration:

    DVPQRF16  sequences::megahit_assembly     -> 1 per task
    YP3AbZYf  das_tool_contig_to_bin_table    -> 1 per task     <- this is the `group_by` key
    sJmEEMdm  contig_gold_standard_table      -> **10 per task**

    A `parents={X}` requirement is narrowed to ONE instance when X IS THE `group_by` KEY.
    It is NOT narrowed when X is merely another parented requirement -- then every
    ancestrally-compatible candidate is staged into that slot.

    (That is the experimenter's formulation and it is sharper than my first one, which said
    `parents=` never pins. It does pin -- transitively from the grouping key.)

The only difference between the slot that resolved to 1 and the slot that resolved to 10 is that one
of them was the grouping key. This is *lineage constraints are ancestral* seen from a new angle —
every gold standard descends from *an* assembly, so `parents={asm}` never narrows to *this*
assembly's.

**THE SHAPE THAT PINS, verified at 208-task scale.** `semibin2` and `metabat2` in WfOlaqLT declare
`asm parents={meta}`, `bam parents={asm}`, `group_by=asm` — and every slot, the BAM included, holds
exactly **1** instance in every task sampled (2 semibin2 + 2 metabat2 of 208 ok each). Amber's
broken shape was `group_by=table`: `asm` still resolved to 1 *via the table's ancestry*, but `gold`
parented to that **unpinned** `asm` took all 10.

**So pinning propagates DOWN from the grouping key to what the key's own ancestry determines — it
does NOT propagate ACROSS to a sibling parented to one of those ancestors.** The working shape is
therefore: make the per-sample anchor the `group_by` key, and parent everything that must be
one-to-one with the sample to that key.

    AND IT MISLEADS IN BOTH DIRECTIONS: the plan's own `sar` metadata reported arity **1** for
    that slot while the runtime consumed **10**. A plan-time arity is not a runtime guarantee, so
    do not read `sar` as evidence a slot cannot fan in -- see the arity note above.

    CHECK IT THIS WAY: read a task's `.command.cache` `consumes` and count the ids per slot.
    Any slot with more than one id, in a transform that expects one, is a fan-in.

## A CACHE RECORD'S `shard` FIELD IS A CONTAINER PATH. Rebuild the host path from `key`.

`.command.cache` records the shard as the path the **task** saw — `/msm_home/task_cache/…` — because
that is where the agent home is bound inside the container. A tool running on the host and
`stat`-ing that path finds nothing.

The experimenter's first shard-eviction dry runs qualified **0 of 208** for exactly this reason, and
a zero from a gate is indistinguishable from "nothing is eligible". The fix is to **derive the host
shard path from the record's `key`** — the same `shard_dir(cache_root, key.hex())` the engine uses —
rather than trusting the recorded path.

Same family as the other scope traps here: `pNN` is a per-run plan index, a Slurm array suffix is not
a nextflow task index, `produces[].relpath` is relative to the SHARD not the task dir, and now a
record's `shard` is relative to the CONTAINER not the host. **Every path and identifier in this stack
is expressed in some frame, and it is rarely the frame you are standing in.**

## Tombstoned shards can still appear in `cache list` / `cache explain`.

`probe` reads the shard and short-circuits on the tombstone, so the runtime sees a clean miss. But
the sqlite index is written by `record_run` at run END — so after a run whose shards were tombstoned
mid-flight finishes, **the index gains rows for shards that are tombstoned and empty.** An indexed
row is not evidence a shard can serve. Trust `invocation.probe`, never the index, for reachability.

## FOR A CONCURRENCY COUNT USE `squeue`, NEVER `sacct`. sacct returns one row per job STEP.

A watcher reported **12 comebin tasks RUNNING** when the truth was **4**. It counted
`sacct -j <array> -n -o State | sort | uniq -c`, and `sacct` emits a row per **step** — the task,
`.batch` and `.extern` — so 4 tasks read as 12. `squeue` lists one row per array element and gives 4.

    concurrency          ->  squeue -j <array> -h -o '%T' | grep -c '^RUNNING$'
    per-task accounting  ->  sacct, and filter `grep -vE '\.(batch|extern)'`
    MaxRSS               ->  sacct, and take it ONLY from the `.batch` row

**The number fed a live decision** — whether a 208-task array at ~9.8 h each finishes inside a 7-day
driver wall. At the false 12 it was ~7.1 days, marginal; at the true 4 it is **~21 days**, a 3x
overrun. A 3x over-count in the reassuring direction is the worst shape an instrument error can take.

Third `sacct` trap on record here, and they all come from the same cause — **one row per step, not
per task**: `JobID%14` truncates `59583112_0.batch` and breaks a `grep -v batch` filter; MaxRSS is
absent from the array-task row and present on `.batch`; and a naive state tally triples.

    AND A COMPANION EMPTY-RESULT: enumerating a run's arrays from its log with
    `grep -oE 'submitted process p[0-9]+__comebin[^;]*; jobId: [0-9]+'` found NONE for a run with
    FIVE live arrays -- the log reads `submitted process p06__comebin (1) > jobId: 59647611;
    workDir: ...`, so a pattern anchoring the job id at the end matches nothing. A run with five
    arrays reported as having none, and every derived total came back 0.

## THROUGHPUT HERE IS THE ACCOUNT'S CPU SHARE, NOT ANY ONE STEP'S WIDTH.

Measured at 07:23 by the experimenter from `squeue`/`sprio`: the account was running **~3,800 cpus**
and **`spades_pratama` alone held 2,256 of them** (47 tasks x 48 cpus). Every pending WfOlaqLT
comebin array sat at priority 2,075,104 — **above** every spades retry except the one holding the
Resources reservation — and all of them were PENDING on **(Priority)**, never (Resources).

**So comebin was not narrow because comebin was mis-sized. It was narrow because another step was
holding half the account's cpus.** Four concurrent comebin tasks against a 208-task array is a
share problem wearing a sizing problem's clothes.

    before resizing a step because it runs few tasks at once, read WHY its tasks are pending.
    (Priority) means you are competing with your own account; (Resources) means the cluster.
    Only the second is a sizing question.

**And the lever is whichever step holds the most cpus, not the one that looks slow.** Here that
makes a `spades_pratama -t 48 -> -t 16` test the throughput intervention — ~2,080 cpus returned
across 65 tasks — while restarting comebin narrower would have competed for the same share, killed
4 running tasks, and recomputed 697 GB of evicted bowtie2 shards.

**The recoverable-failure note that made declining safe:** if the driver hits its 7-day wall the
trap cancels and wave 2 resumes from cache, so an overrun costs a relaunch rather than the work.
Check that a wall overrun is recoverable before treating it as a deadline.

## `src/` is not what a run stages. Check the run's own transform tree.

A run stages its own copy of every transform at `<run>/_metasmith/task/transforms/<id>/`, and a
lane may run a PINNED transform that differs from the standard library at the same path name.
Reading `src/metasmith_libraries/...` tells you what the standard library does and nothing about
which code a live lane executes.

    THE TWO PLACES A PINNED TRANSFORM LIVES -- check BOTH before reporting absence:
      <run>/_metasmith/task/transforms/<id>/<step>.py                     the staged copy
      <checkout>/research/metasmith_benchmark/library/transforms/**/<step>.py   the pinned source
    and the one that will mislead you:
      <checkout>/src/metasmith_libraries/transforms/**/<step>.py          the STANDARD library

    Measured 2026-09-13: `memote_score.py` carries `export HOME="$PWD"` in **12 of 12** checkouts at
    the benchmark-library path and in **0 of 12** at the standard path. Grepping the standard path
    produced a confident "the fix shipped nowhere" about a fix that had shipped everywhere.

Paid for twice, the same way both times. **B12:** the Flye preset defect is real and byte-identical
in both standard flye transforms, and I asserted E2 long was exposed — it runs a pinned `e2/flye.py`
with a platform-keyed MODE table and no `mean_quality` branch, so the fix had shipped before the
alarm. **The @SampleID split:** `cami_contig_truth.py:33` passes `Path(iasm.local).stem` as the
gold standard's sample name, and I asserted WfOlaqLT's AMBER rows were therefore unattributable and
proposed a driver mapping table. It runs a pinned `e2/gold_standard.py` whose line 25 passes
`read_metadata["sample"]`, and its products carry `@SampleID:strain_sample_26`. Both proposals were
unnecessary.

**And note which file is NOT the discriminator.** Both gold-standard paths call the same
`lib::cami_gold_standard.py`; the run's own staged copy has the identical `@SampleID:{sample_id}`
write. The name comes from argv[4], which the *transform* supplies. A shared helper being identical
on both paths is not evidence the two paths behave the same — look at what the caller passes it.

**VERIFYING A MECHANISM IN A SOURCE FILE IS NOT VERIFYING THAT A RUN REACHES IT.** This is the
source-level form of the artifact-versus-capability rule, and it sits one level above the
`GB_ALLSTEPS` requirement: that one catches a change to a shared transform's requirements, this one
catches reading the wrong copy of the transform entirely.

## A startup validation can reach the NETWORK, and an unused default can kill a run

nf-core/mag 5.5.0 declares `checkm_download_url` with its own default set to a **URL**, and the
schema marks that param a `file-path` that must exist:

    nextflow.config:170   checkm_download_url = "https://zenodo.org/records/7401545/..."
    nextflow_schema.json  {"type":"string","format":"file-path","exists":true,
                           "default":"https://zenodo.org/...", "hidden":true}

nf-schema resolves it through Nextflow's `file()`, which for an `https://` path performs an **HTTP
HEAD**. So the startup validation pass makes a network request. `run_checkm = false` in our pinned
config, so **the param is never used** — and validation of an unused, hidden default failed the whole
run at 6m59s, exit 1, before a single task was submitted (E1 short, job 59656993).

**Measured: the failure was a GLOBAL Zenodo outage, not node egress.** This matters because the
obvious workaround is wrong:

    fir LOGIN node    checkm tarball 504    zenodo.org root 504
    the workstation   checkm tarball 504    zenodo.org root 504
    same workstation  github.com     200    <- positive control

A login node moves ~550 MB/s externally, and `zenodo.org` root is 504 too. **There is no node where
that validation would have passed**, so moving the head to a login node fixes nothing. The fix is
`validate_params = false`, which gates only nf-schema's startup pass and is in no task hash.

**The general rule: a pre-flight check that touches the network is a pre-flight check that can fail
for reasons unrelated to your run, and a hidden default you never set is still validated.** Before
blaming a launch failure on the cluster, HEAD the URL from two independent hosts with a positive
control — the same discipline as proving an instrument before believing an empty grep. Same family as
the `"exists": true` on both sample-sheet read columns that made B3's first sheet unvalidatable, and
as the engine's own `DownloadSteps` warning that cannot see a non-`ref::` download step.

    AND CHECK WHETHER THE OUTAGE BLOCKS ANYTHING BEFORE RAISING IT. Verified at the time: the
    Pratama 1,275-MAG archive (`pratama2026/zenodo_17897233{,_unpacked}`), metaGEM's 46/46 published
    payloads (14,154 files) and the CheckM2 database (3,082,500,605 B) are all on disk, and
    `grep -il zenodo` over every staged `workflow.nf` returns zero. So no live lane needed a fetch.

## An mtime-preserving writer is invisible to `find -mmin`, and OCI layer unpack is one

When a tree grows and `find -mmin -N` cannot account for it, the writer is **restoring timestamps**,
not creating fresh ones. `tar -x`, `rsync -a`, `cp -p`/`cp -a` and — the one that bit this campaign —
**apptainer's OCI layer unpack** all preserve the mtimes inside the archive. A newly written file can
therefore carry last year's timestamp.

**THE INSTANCE THAT ACTUALLY BIT US: Nextflow's `publishDir` in COPY mode.** A head launched before
`link` mode copies every published file into `out/`, and the copy **keeps the source mtime**. Measured
2026-09-13: `bench/` went 70,306 -> 262,068 inodes, driven by E1 long's QUAST_BINS — 166 work dirs
holding 88,114 inodes (~530 each) plus `out/GenomeBinning/QC` at ~91K — and all 164 QUAST_BINS
finished together, so the whole thing landed as one burst. `find -mmin -6` over every live run tree,
both E1 work trees and all three `task_cache`s accounted for ~12K new entries against ~200K of quota
growth, because the published copies carried their sources' timestamps.

    THE CHEAP GUARD: `skip_quast = true` where QUAST is not in the comparison table. E1 short would
    have run ~832 QUAST_BINS for ~440K inodes -- nearly half the quota -- and the flag gates QUAST
    and QUAST_BINS only. Check what a per-BIN step costs before a 208-sample rung, not after.

A second instance of the same class, which cost an hour of wrong diagnosis: a live apptainer pull on
a login node:

    apptainer.bin pull -F .../wave2_b3_nfcore/apptainer_cache/nxf/<image>.img docker://<image>
    mksquashfs .../wave2_b3_nfcore/apptainer_tmp/w32/bundle-temp-<n>/...

`apptainer_tmp` is **outside every per-run tree a campaign census walks**, so it is missed twice
over: invisible to an mtime filter and absent from the directory list. This is the third time that
same tree has mattered — 263,470 inodes from one mass pull, 111,932 reclaimed later, and now this.

**The pattern is a SPIKE, not a leak:** unpack a rootfs (tens of thousands of inodes), squash it,
delete. So a census taken between pulls reads near zero — ours read 10,658 — and a 2-minute sample
sees 44,000/h while a 14-minute fit sees 343,000/h. Neither window is wrong; the process is bursty.

    CHECK, in this order, when inodes climb and the runs look innocent:
      ps -u <user> -o args= | grep -E 'apptainer|mksquashfs|tar |rsync|cp -[pa]'    on EVERY node
      find <apptainer_tmp> -xdev \( -type f -o -type d \) | wc -l    twice, minutes apart
      ls -1t <apptainer_cache>/nxf/*.img | head    -- if the newest is OLD, a pull is landing a
                                                      NEW image the prepull list never had

**And the durable cause: a prepull inventory goes stale against the running configuration.** 60
images were prepulled and verified; the checkout then wanted a 61st (`bcftools_htslib`) that
`images.txt` never listed. Every image beyond the prepulled set is a fresh unpack and a fresh spike.
Re-derive the list from the RUNNING configuration (`nextflow inspect` on the live profile) rather
than trusting a list built for an earlier revision or the test profile.

**READ `etime` BEFORE CALLING A PROCESS THE CAUSE.** The worked example above was wrong: that pull
showed `etime 1-03:32:41` — started the previous day and hung 27 h in the gzip compression test, tree
holding 32 inodes, incapable of the burst it was blamed for. `ps -o args=` omits elapsed time, so a
hung process from yesterday is indistinguishable from a busy one. Use `ps -o etime=,args=`, and treat
a process whose age exceeds the growth window as ruled out by arithmetic before reasoning about it.

Do NOT kill a pull mid-squash to reclaim inodes: a compute node cannot fetch, concurrent pulls crash
with a Go register dump (the fix is a scratch `APPTAINER_TMPDIR` plus capped concurrency), and an
interrupted unpack can leave the rootfs tree behind — converting a transient spike into a permanent
one.

## A watcher written for "either element" exits on the FIRST one and abandons the rest

2026-09-13: `by703zqdb` and its fir-side twin (job 59665019) both watched the `-t 16` array with a
`term >= 1` condition, reported element `_1`, and **exited** — leaving `_0` running with no capture,
while `_0`'s outcome was still owed to the experimenter. Nothing failed and nothing warned; the watch
simply stopped existing, which is the same silence a dead watcher produces.

**The rule: a watcher on an ARRAY must either loop until every element is terminal, or be re-armed
per element.** "Fire on the first result" is the right design when one result answers the question,
and the wrong one the moment a second element carries information the first cannot — here `_1` was the
deliberate top-end input and `_0` bounds the smaller-input side, so they answer different questions.

    the check before arming:  if this fires once and exits, what is left unwatched?
    and after it fires:       re-read the owed list, not just the event

Same family as the discharged-watcher problem (a watcher whose brief is complete becomes noise) but
inverted: here the brief was only half complete when the watcher retired itself.

## `grep -B/-A` turns a filtered view into an unfiltered one

2026-09-13: a semibin2-filtered `sacct` view with `-B1 -A1` for context admitted an adjacent
`carveme_from_orfs_cplex` row, and its 33,543,960K OOM read as semibin2 failing at the 32 G rung —
which would have undercut the 251-of-251 conclusion drawn in the same breath. The context rows are
*not* filtered, and they look like they belong because they share the surrounding format.

**Use context flags to READ, never to COUNT or ATTRIBUTE.** If a row is going to carry a claim,
re-select it without `-B`/`-A` and confirm its own JobName.

**AND CARRY `WorkDir` IN EVERY FAILURE SWEEP.** A JobName names no run: `nf-p10__carveme_from_orfs_cplex`
appears in three different runs on one day. Add `-o ...,WorkDir` to the `sacct` line and fold it to the
run id:

    sacct -u <user> -S <date> --parsable2 -n -o JobID,JobName,State,ReqMem,WorkDir \
    | awk -F'|' '$1 !~ /\.(batch|extern)$/ { n=split($5,a,"/"); run="?";
        for(i=1;i<=n;i++) if(a[i]=="runs") run=a[i+1]; print run"  "$4"  "$3 }' | sort | uniq -c

**`sacct -o WorkDir` works even after `scontrol` has aged the job out** — measured 2026-09-13:
`scontrol show job 59598635` returned nothing while `sacct` gave the full path. So a failure is never
unattributable, and attributing one by name is never necessary. Doing it anyway put an E4 OOM in the
record that belonged to a stopped li2019 run, and would have contradicted that lane's zero-failure
result.

Same family as the per-run plan index (`pNN` names no owner) and the `.extern`/`.batch` step rows
(one job, three rows): every one is a case where the rows next to your answer resemble your answer.


## Two runs are not an A/B unless you can NAME what is held constant

2026-09-13, caught twice in one exchange. I compared `carveme_from_orfs_cplex` in HQ5SrqFe (32 G,
3 failures) against lE94xbfH (16 G, 1,996 successes) and concluded one corpus's bins were harder. The
runs differ in **bin provenance** (recovered bins vs published MAGs), **plan**, **checkout**, and
possibly **study membership** — chunk 1 is sorted by study and may not contain the study I named.

**Before comparing two runs, write down what is identical.** If the list is empty or unknown, the
comparison supports no causal sentence — report each run's numbers separately and leave the reason
unstated. A difference in outcome across two runs is a difference across everything that differs
between them.

Same error as `never-ab-across-two-toolchains` in memory (a musl-static build cost 12-15 ms of
startup and inverted the sign on small payloads), and as the COMEBin 12-cpu-vs-48-cpu figures, which
were only quotable because the SAMPLE was held constant and were explicitly NOT quotable across bins.
