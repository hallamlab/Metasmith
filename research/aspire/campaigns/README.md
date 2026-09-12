# ASPIRE campaigns

Real runs of this pipeline family on HPC. One so far.

| | |
|---|---|
| `r1/` | GMCF_3495 — 34 lung metagenomes on the Alliance cluster **fir**: assembly, binning, functional annotation, taxonomy. Complete; published and mirrored. |
| `JOURNAL.md` | the ten session debriefs from the run, carried out of the project that held them |

## Where this came from

`r1/` is the whole tracked tree of the separate `lung-microbiome` project's
`feat/run1` branch — 55 commits, on no remote, ~1 MB. It is drivers, logs, the
sample sheet, the gap-fill scripts and the campaign attribution mirrors. The 11 GB
of campaign output beside it in that worktree is derived data, regenerable from the
published tree, and stays out; that project's own `.gitignore` says so and this
migration honours it.

There is no `src/` package for this pipeline and one should not be invented. ASPIRE
is a transform library (`src/metasmith_libraries/transforms/aspire/`) plus this
research tree — a module directory does not oblige an application package, and an
empty one would be worse than none.

## Two things to know before running anything in `r1/`

**The run is history, not a recipe.** It executed against metasmith 0.19.1 pinned to
two scopes that no longer exist, and `r1/README.md` says so at the point where it
would otherwise read as instructions. The driver's `MSM_LIB` default now points at
this repo's `src/metasmith_libraries`, which carries the same ExecWithEnv port — but
the engine is whatever this repo ships. Re-running is a re-plan, not a replay.

**`r1/.gitignore` ignores `.cache/` and `.campaigns/`, and the tree carries tracked
files inside both.** They were force-added upstream and force-added here: the input
library, three rendered DAGs, the nextflow config, and the campaign attribution
mirrors — 95 of the 136 entries. `git add research/aspire/campaigns` picks up 41 of
them and silently drops the rest. Use `git add -f`, and check the count.

## What the run produced

Published at `~/project-rpp/steven_c_gmcf3495/metagenomics` on fir (693 GB, 995
inodes) and Globus-mirrored to the manuscript collection. Reads, contigs and bins all
carry GTDB r232.

**The headline MAG count in that tree is wrong wherever it is quoted.** GTDB-Tk
returned 8 of the 67 "quality MAGs" as Unclassified carrying zero of 120 bacterial
and zero of 53 archaeal markers; their contigs are 5,584 of 5,587 *Homo sapiens*.
CheckM2 scores prokaryotic completeness and cannot reject out-of-domain input, so
host DNA enters as a plausible mid-range score. **59 prokaryotic MAGs + 8 host-DNA
artifacts.** The bins were left in the tree deliberately — Unclassified is a real
result and silent removal is worse — with the caveat recorded above the layout
table. Entry 10 of `JOURNAL.md` is the source.

One item is open and blocked rather than deferred: `run_arbutus_campaigns.py` shells
out to `arbutus-infra/dev/scripts/*-submit.sh`, and that project is not on this node.
It lives on capella and its submit scripts are driven in place there, because its
tenancy credential and worker key are gitignored and a clone cannot authenticate.
The absolute path to it in that script is therefore correct as written and should not
be swept.
