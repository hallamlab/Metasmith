# Isolating the squashfs wedge — forced-SIF runs of a locally built container

## The question

`use-sandbox` was adopted in 2026-05 as the workaround for a hang while reading a
container's rootfs (Bug E.2 — the "nextflow JVM hang"). The wedge has since stopped
reproducing. So: **with the sandbox arm unavailable and normal SIF execution forced, does
the hang come back?**

That needs a container whose relay binaries are real (the relay's fork chain is the
suspect), a host that actually reads a SIF through FUSE, and a way to tell a genuine
green from a green that never touched the code path.

## What the scripts do

| script | where it runs | what it does |
|---|---|---|
| `01_build_container.sh` | dev box | pip → docker → SIF, with real `msm_relay.*` copied in from a sibling scope whose relay source is byte-identical |
| `02_ship_sif.py` | dev box | rsyncs the SIF to the exact path `MakeMaterialiseCommand` probes for, so deploy skips the pull |
| `03_deploy.py` | dev box | `Agent.Deploy()` to `--host <ssh alias>` or `--host local`, then prints the host's rootfs verdict |
| `04_probe_sif.sh` | target host | five rungs, differing only in what the container launch descends from |
| `05_validity_and_jvm.sh` | target host | proves `squashfuse_ll` was in the chain, then repeats the relay rungs with the JVM |
| `10_pangenome_forced_sif.py` | dev box | the same question asked by a real workflow: deploy → stage → run → collect → verify, on a fresh home whose store has never held an image |

The rungs, in order: bare `apptainer exec`; the deployed `msm` wrapper; a launch dispatched
by the **relay daemon** (fork, `SIGCHLD` ignored, `nohup`'d launcher); a container that
**bounces out through the relay to launch another container** (the production shape, two
readers alive at once); and N of that last one at once, because the reported wedge was
flaky.

A rung that overruns its patience is **not killed before its evidence is taken** — the one
datum that separates a *stopped* reader from a *deadlocked* one is the reader's `STAT` plus
its per-thread `wchan`, and both vanish with the process.

## Results

Image `quay.io/hallamlab/metasmith:0.20.0-4d1289c`, built 2026-07-29 with all four relay
binaries real (1.4–1.8 MB, correct ELF/Mach-O magic). The GUI bundle is deliberately absent
(no node on the dev box), which changes the build hash and nothing on the rootfs path.

### chamois — apptainer 1.4.5, kernel 5.4, no setuid starter → GREEN

Verdict `use-sif`, no sandbox present, so this is the forced-SIF configuration by default.

| rung | payload | result |
|---|---|---|
| 1 bare | `metasmith --help` | ok 4s, help printed |
| 2 `msm` wrapper | `metasmith --help` | ok 4s, help printed |
| 3 relay-dispatched | `metasmith --help` | **ok 4s, help printed** |
| 4 nested (container → relay → container) | `metasmith --help` | ok 4s |
| 5 fanout ×8 of rung 4 | `metasmith --help` | ok 6s |
| 6 bare | `nextflow -version` | ok 2s, banner |
| 7 relay-dispatched | `nextflow -version` | ok 4s, banner |
| 8 nested | `nextflow -version` | ok 4s, banner |
| 9 fanout ×8 of rung 8 | `nextflow -version` | ok 4s |

Deploy itself is a tenth data point: `deploy_from_container` extracts the relay by running
metasmith **inside the shipped SIF**, and it completed in 3 s.

**Validity, confirmed rather than assumed:** while a container was held open,
`/usr/libexec/apptainer/bin/squashfuse_ll -f -o allow_other,ro,…,offset=40960 /proc/self/fd/3
/var/lib/apptainer/mnt/session/rootfs` was serving it, `/sys/fs/fuse/connections/69` existed
with `waiting=0`, and the container's own `/` was an overlay whose lowerdir is that
FUSE-served rootfs. No `Converting SIF file to temporary sandbox`. So the FUSE path was
genuinely exercised.

Two things worth keeping:

- The reader is **multi-threaded** (`Sl`) — apptainer's own, launched without `-s` — and it
  holds its **own PGID**, distinct from the launching session. That is the upstream 1.4.5
  change: the reader's lifecycle is no longer entangled with the job's process group, which
  is the property `use-mount` and `use-sandbox` were buying by hand.
- `msm_relay bounce` returns 0 whatever the dispatched command did. On a bounced rung, "ok"
  means "completed without wedging"; the payload's own success has to be read out of the
  log. The first pass through rungs 7/8 hid a `nextflow` crash that way (`--no-home` leaves
  `$HOME` unwritable, and apptainer refuses `--env HOME=…`; `--home /tmp` is the fix).

### capella, this dev box — WSL2 kernel 6.18.33.2, apptainer 1.4.2 → GREEN

The host class the wedge was originally reported on, and apptainer ≥1.4 resolves to
`use-sif` there too, so it is forced-SIF by default as well. All nine rungs green in 2–4 s,
FUSE validated the same way. Full table in `06_local_results.md`.

### Rung 10 — a real workflow, capella, forced SIF → the nesting shape held

The five rungs above are synthetic launches. `10_pangenome_forced_sif.py` runs the GUI's
`wooden-stallion` pangenome recipe (2 genomes → `getNcbiAssembly` ×2 → `ppanggolin` →
`heatmap`) on a fresh agent home under `scratch/`, which is what makes "no sandbox" a
property of the store rather than a claim about what the code chose.

The first pass reached step 1 only — the bundle's `ppanggolin.py` calls
`ExecutionContext.SourceOf`, absent at 0.20.0, so the protocol raised and retry-then-ignore
swallowed it. What it still exercised is the production shape end to end: nextflow head
container → relay bounce to the host → step bootstrap container → relay again → tool
container, six times over, two of them to full depth (the `ncbi-datasets` SIF was pulled and
executed). Roughly 13 distinct `squashfuse_ll` readers, several alive at once, apptainer
1.4.2 on WSL2 — the original reporting configuration. Nothing wedged, and the store ended
with two SIFs and no sandbox while the shared cache next door held unpacked sandboxes for
all three tool images.

Pin the image to the one the recipe was authored against (`0.20.1-bf54d6f`) to get past the
`SourceOf` mismatch; the two large tool images (`ppanggolin` 1.1 GB, `python_for_data_science`
2.9 GB) have still never been materialised as SIFs, and they are the interesting ones.

## Answer

**No. Forcing normal SIF execution does not bring the hang back** — not on chamois
(apptainer 1.4.5) and not on WSL2 (apptainer 1.4.2), on either the `metasmith --help` or
the `nextflow` payload, and not under an 8-way fan-out of the nested production shape. In
every case the rootfs was demonstrably being served by `squashfuse_ll`, so these are greens
on the suspect path rather than greens that bypassed it.

What that does and does not establish: it bounds where the bug **is not**. The negative
covers apptainer 1.4.2/1.4.5 on kernels 6.18.33.2 (WSL2) and 5.4, which between them are
every host currently available — and, notably, *not* apptainer **1.3.0**, the version in the
original report. That is now the only untested variable that was present when the wedge was
first seen.

The one structural difference the runs did surface, measured rather than inferred: on
apptainer **1.4.2** the `squashfuse_ll` reader shares the launching script's **process
group**, and on **1.4.5** it holds its own. That entanglement is what lets a group-level
signal tear the rootfs out from under a live container (the original report's `Transport
endpoint is not connected` tail), and 1.4.5 fixes it upstream — which is the property
`use-mount` and `use-sandbox` were buying by hand. It is not sufficient on its own: nothing
here wedged on either host.

**The sandbox arm now serves two unrelated purposes, and only one of them is retired.** The
E.2 wedge was run-side; micb0's is build-side — apptainer's bundled mksquashfs 4.7.5
segfaults on large images, which is why `external_checkm2` and `gtdbtk` fall through to
`build --sandbox`. Nothing here tests that, so the fallback earns its place on those hosts
whatever happens to E.2.

Next rung, if this needs pushing further: apptainer 1.3.0 on WSL2 (the exact original
configuration), and rung 10 carried through the two large tool images.

## Traps this cost time on

- **`_store_root()` is `${APPTAINER_CACHEDIR:-<agent home>/container_images}`, expanded on
  the execution host.** Pre-placing a SIF under the agent home does nothing on a host where
  `APPTAINER_CACHEDIR` is set — deploy looks in the cachedir, fails to pull an unpublished
  tag, and dies at the relay-missing assertion. `02_ship_sif.py` refuses a remote host with
  the variable set for exactly this reason; the local arm has to place into the cachedir.
- **An empty `/sys/fs/fuse/connections/` proves nothing at rest.** The mount lives only as
  long as the container, so the reader has to be caught while a container is held open.
- **`METASMITH_APPTAINER_ROOTFS=sif` is inert against a store that already holds a
  `.sandbox`.** `_ExecInEnv` skips materialising whenever `[ -e sif ] || [ -d sandbox ]`
  passes, and `MakeRunCommand`'s ternary then prefers the directory — the override is never
  consulted on that path. So the store root, not the override, is what actually decides:
  point `APPTAINER_CACHEDIR` at a fresh directory, or a "forced sif" run is quietly a
  sandbox run. A developer box is the likely victim, since its shared cache accumulates
  sandboxes from every past experiment.
- **The completion sentinel is not a success signal.** Steps retry and then ignore
  (`nextflow_codegen.py`), so a transform that raises in its protocol yields a run that
  writes `run completed at` and produces nothing from that step. Assert on
  `terminated with an error exit status` and on the expected outputs, not on the sentinel.
