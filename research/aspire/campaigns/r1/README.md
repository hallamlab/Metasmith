# GMCF_3495 — run r1

Metasmith driver for Steven Chen's GMCF_3495 metagenomes: **assembly, bins,
functional annotation, and taxonomy** for 34 libraries, on the Alliance cluster
**fir**, with the two largest reference databases moved off-cluster.

## What is here

| File | |
|---|---|
| `run_r1_metag.py` | the driver — `list-samples`, `check-dbs`, `stage-reads`, `setup`, `run`, `status` |
| `samples.tsv` | 34 samples (S1–S33 + NTC), their Globus source paths and raw sizes |
| `.cache/` | generated: input xgdb, rendered DAG, nextflow config, submitted task keys |

## Which metasmith

> **Historical.** Both scopes below were retired when this work moved into the
> metasmith monorepo, and neither path exists any more. This section records what
> the run was executed against — it is not a recipe you can follow today. The
> driver's `MSM_LIB` default now points at `src/metasmith_libraries` in this repo,
> which carries the same ExecWithEnv port; the engine it would resolve against is
> whatever this repo ships, not 0.19.1. Re-running is a re-plan, not a replay.

This run was pinned to two scopes, not to the deployed `msm` environment:

- metasmith **0.19.1** — `projects/metasmith/steven-c-metag`
- libraries — `projects/metasmith-libraries/steven-c-metag`

```
conda activate msm_stevenc
export PYTHONPATH=/home/tony/agentic_workspace/projects/metasmith/steven-c-metag/src:/home/tony/lib/locals
python run_r1_metag.py <subcommand>
```

The scope's `src` **must** come first: the global `/home/tony/lib/locals` symlinks
`metasmith` at `projects/metasmith/dev`, and without the prepend you silently run
0.18.x instead. Do not repoint that symlink — spanish-lakes, fabfos and cyanoverse
all resolve through it.

The pinned libraries scope is not optional either. 0.19.x **removes**
`ExecWithContainer` in favour of `ExecWithEnv().ifContainerDo(...)`; the old name
is in `_FORBIDDEN_CALLS` and a static scanner rejects any transform still using
it, so `metasmith-libraries/main` cannot execute against this engine at all.

Earlier drivers carry a warning never to run against a metasmith source checkout,
because its `LiveShell` used an fd-5 control trampoline that hung over plain ssh.
That is fixed on this line (in-band MARKER), which is why running from source is
viable here — but confirm it with a trivial remote call before trusting a long
run to it.

## The data

34 paired libraries, 430.8 GiB of gzipped reads, staged from the chinook Globus
guest collection (`/Received_raw_data/GMCF_3495/`) to
`fir:/scratch/phyberos/gmcf3495/reads/`. Sizes span four orders of magnitude:
S25 is 80 GiB, S2 is 13 MB — so per-sample resource behaviour will not be
uniform, and the largest samples are where the memory floors get tested.

Each sample appears twice on Globus: a `*_L7_ds.<hash>/` dir holding the
full-lane `_L007_` fastqs, and a sibling `*_ds.<hash>/` holding a few-KB `_L001_`
pair. The `_L001_` files are the sequencer's QC subsample, not data — `stage-reads`
moves only the `_L007_` pair, flattening it to `<sample>_R{1,2}.fastq.gz`.

`NTC` is the negative control. It is in `samples.tsv` so it can be profiled
deliberately; pass `--exclude NTC` to drop it from a run. Whatever it produces
must be labelled as a control wherever these results are published.

## Order of operations

```
stage-reads --yes     # Globus chinook -> fir scratch (~431 GiB)
setup --run           # deploy the agent, prefetch all 21 tool containers
check-dbs             # every declared input path must exist
run --dry-run         # plan + render the DAG
run                   # stage + submit
status
```

`setup` is not optional. Compute nodes have no outbound network, so a container
missing from `<agent_home>/container_images` at submit time is a job that dies on
pull, and any reference DB the planner cannot see as pre-staged gets a
`download*` step it can never execute.

## Reference databases

Declared in `DB_PATHS`, against the lab's own nested library at
`/home/phyberos/project-rpp/lib/`:

| dtype | path |
|---|---|
| `ref::uniref50_diamond_db` | `lib/diamond/uniref50.dmnd` |
| `ref::kofamscan_profiles` | `lib/kofamscan/profiles.tgz` |
| `ref::kofamscan_ko_list` | `lib/kofamscan/ko_list.tsv` |
| `annotation::eggnog_data` | `/scratch/phyberos/databases/eggnog` |
| `ref::kraken2_db` | `lib/kraken2_2026` |
| `ref::centrifuger_db` | `lib/centrifuger_r232` |

Two of those need explanation.

**eggnog points at scratch, not at the library.** `lib/eggnog` holds only a
compressed `eggnog.db.gz`; the transform needs the unpacked tree. The scratch
copy is the same one the spanish-lakes w2 ORF driver uses.

**centrifuger is a directory here, not a prefix.** The index is four split files
`<prefix>.{1,2,3,4}.cfr` and the classifier wants `-x <prefix>`, but the
transform derives that prefix itself by globbing `*.1.cfr` inside the directory —
so `DB_PATHS` names the directory and `DB_PROBE_SUFFIX` is empty. The index
inside is `cfr_gtdb_r232+refseq_hvfpc`: a **superset** of bare r232, and any
methods text should say so rather than claim plain r232.

### The two big databases are off-cluster

Metabuli's r232 build (~744 GB on disk, 634 GB compressed) and GTDB-Tk's r232
package (~110 GB) both run as on-demand Arbutus services rather than living on
fir. Each is a reference Cinder volume plus a small script set
(`arbutus-infra/dev/scripts/{metabuli,gtdbtk}-{up,run-batch,submit,down}.sh`),
copy-on-write cloned per job, read-only at three layers, costing zero cores at
rest.

That is why neither `ref::metabuli_ref` nor `ref::gtdb` appears in `DB_PATHS`,
and why `taxonomy::metabuli*` is not in the target set. It also has a pleasant
structural consequence: with both large databases gone, nothing left in the
target set waits on a staged database, so this is **one** submission rather than
the two waves an on-cluster metabuli would have forced.

Both campaigns run after this DAG, but stage from the **published tree** rather
than from a run directory: `run_arbutus_campaigns.py harvest --from-published`.
Published names carry the sample and the binner (`<sample>.<binner>.<n>.fa`),
which is the attribution the lineage graph cannot supply, and it leaves the
campaigns independent of scratch cleanup.

`arbutus-infra` is not on this node and a scope for it is not provisionable — its
tenancy credential and worker key are gitignored, so a clone cannot authenticate.
It lives on **capella**; drive the submit scripts in place there over ssh instead
of copying secrets across.

`--with-gtdbtk` still exists and still refuses to plan until a release tree is
staged and added to `DB_PATHS`, because otherwise the planner quietly satisfies
`ref::gtdb` with `downloadGtdbDB` — a ~110 GB login-node wget.

### GTDB release

Every GTDB-based tool in this run is on **r232**: centrifuger's on-cluster index,
the metabuli Arbutus service, and the GTDB-Tk Arbutus service. Read, contig and
bin taxonomy are therefore directly comparable, which was not true of earlier
drafts of this run.

`kraken2`/`bracken` is **NCBI**, not GTDB. That is deliberate — it is the
non-GTDB second opinion — and it must not be "reconciled" onto r232.

One version pairing is a hard floor rather than a preference: **gtdb232 requires
metabuli ≥ 1.2.0.** 1.1.0 cannot read that database at all. `resources/env/metabuli.env`
is pinned to `1.2.0--pl5321h0bb26bb_0`, the reference volume's `MANIFEST` records
the floor as `MIN_METABULI`, and `metabuli-run-batch.sh` checks it before a job
starts rather than letting it fail hours in.

## The plan

Per sample, plus one cross-sample `skani_dedup`:

- **assembly** — `interleave → seqkit_reads → bbduk → megahit → prodigal`, plus
  `assembly_stats` (BAM, per-contig and per-bp coverage)
- **bins** — `{metabat2, semibin2, comebin}` → `checkm2` per binner →
  `aggregator` → `skani_dedup`
- **functional annotation** — `diamond_uniref50`, `kofamscan`, `eggnog_mapper`,
  `proteinbert` on the predicted ORFs
- **taxonomy** — `kraken2`+`bracken` and `centrifuger` on reads. Contig and bin
  taxonomy are the off-cluster campaigns above.

The per-binner `checkm2` fan-out only happens because each is added with a
distinct bin-fasta target as its parent. Without those parents the planner
satisfies it with **one** binner's bins and the other two go unqualified.

`proteinbert` declares 8 cpus / 32 GB / a 12 h ceiling and makes no GPU request.

## fir specifics

- `module load apptainer`, alone. The `module load gcc/9.4.0` that must precede
  it on Sockeye is a Sockeye quirk and is wrong here.
- SLURM allocation is `rrg-shallam-ab` (`def-shallam_gpu` for GPU).
- comebin defaults to **CPU** (`--comebin-device gpu` to override). Its CUDA
  torch falls back cleanly under `apptainer --nv` and honours `-t {cpus}` on
  every stage, and on fir the low-priority GPU queue stalled it 24 h+ while CPU
  nodes scheduled in seconds.
- `bbduk` and `megahit` both auto-detect **node** RAM, which blows past the
  SLURM cgroup. The transforms pin `-Xmx`/`--memory` to the allocation; the
  64 GB floors here plus `process.tries=4` give 64 → 128 → 256 → 512 GB.
- fir nodes are 192 cores / 768 GB, against Sockeye's 32 / 190. Thread counts
  inherited from a Sockeye config are worth revisiting rather than transplanting.
- Watch the `rpp-shallam` **file count**, not just its space: the allocation was
  at 361K of 500K inodes going into this run, and a previous spanish-lakes run
  failed on that quota. Per-bin fan-outs are what consume it.

## Lineage

Adapted from the spanish-lakes **river** drivers
(`projects/spanish-lakes/river/scripts/run_w{1,2,3}_*_river.py`), which split
this same chain into four separately-submitted waves against fir because that
run had 101 samples and a login-node PID cap to respect. At 34 samples there is
no reason to stage the waves apart, so this is one plan. The multi-sample
enumeration, `.cache`-scoped task keys, dry-run/render split, and comebin config
override all come from those drivers.
