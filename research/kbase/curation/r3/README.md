# Curation round 3 -- what the standard library is missing

## Purpose & Contents

Round 2 named the vocabulary KBase uses: 54 core task verbs over 35 entities. This round asks
whether `src/metasmith_libraries` performs each verb, and proposes the transform where it does not.
**It proposes. It does not build.** No transform, type or template is written here.

The verdict is on the STANDARD library, not on the generated port under `../../library/`. The port
carries 371 stub transforms and runs none of them.

The argument lives in the published report, so that it lives in one place and cannot drift:

**<https://claude.ai/code/artifact/7d5039a5-3489-4488-9928-9701ea0cebec>**

## The rule this round is built on

**A proposal is a tool with a job.** A verb whose apps are a family of table manipulations does not
become five transforms named after the manipulations. It becomes one refactor, or a `declined` entry
with a reason. The first draft failed that test on KBase's whole `matrix` group -- five verbs that
read as generic linear algebra and are community ecology the aspire lane already performs.

## The gap

| verdict | verbs | |
|---|---|---|
| covered | 14 | a shipped transform performs the verb |
| deferred | 7 | the verb is an import, which a template declares as an input |
| plumbing | 6 | the verb moves objects, and metasmith fans out instead |
| partial | 3 | a shipped transform performs part of it |
| missing | 14 | nothing performs it |
| declined | 10 | nothing performs it and nothing should |

22 transforms and 3 refactors answer every verb except the two declined outright. They need 26 new
types and 4 new tool environments, and they sit on top of 97 transforms the library already ships.

| stage | canonical workflows | narrative copies |
|---|---|---|
| today | 64 / 631 (10%) | 373 / 2407 (15%) |
| + the 22 transforms | 400 (63%) | 1657 (69%) |
| + the 3 refactors | 574 (91%) | 2262 (94%) |

Reachable means every verb in the workflow is covered or is plumbing. The residual 57 workflows end
on `edit_model` or `edit_media`.

Per app rather than per workflow: of the 493 in the catalog, 197 are plumbing and 48 are imports.
The 245 that remain split 100 extended, 65 existing, 43 proposed, 27 refactor, 7 declined, 3 folded.

## Where the transforms go

`src/metasmith_libraries/transforms/kbase/<task>/`, one directory per round 2 task verb. The port's
own generated tree is `research/kbase/library/transforms/kbase/`, flat and named `Module__app.py`.
The two are separate roots and neither loads the other.

## What each file owns

| file | owns |
|---|---|
| `proposals.yml` | the verdict per verb, the proposed transforms and their signatures, the refactors, the new types and environments |
| `serving.yml` | which shipped transforms already serve each verb |
| `render.py` | the gate, then `proposals.jsonl`, `app_map.jsonl` and `summary.json` |
| `build_artifact.py` | the published report, from those files and `_page.css` |
| `app_map.jsonl` | one row per catalog app: its verb, its disposition, and what serves it |

`render.py` reads every count from `../r2/` and `../../catalog/`, never from `proposals.yml`. It
asserts the two name the same 54 verbs, that every path in `serving.yml` resolves to a file, and
that every proposal states what running it buys.

## Findings that cost a search

**bakta is two transforms, and the CDS half does not exist anywhere.** cyanoverse was searched on
both hosts. `annotations/PLAN.md` and `ab48/PLAN.md` both record "No new bakta transform -- reuse
`bakta_noncoding.py` unchanged", and `search-transcription-factors/scripts/run_bakta_batch.sh`
invokes the binary directly with the same `--skip-cds`. cyanoverse names proteins with the
annotation palette instead -- InterProScan, eggNOG, DeepEC, PredicTF, BUSCO, antiSMASH -- each
already a hit/definition pair here. The CDS arm is `bakta_proteins`, a separate entry point beside
`bakta` in the pinned 1.11.0 image, verified present and taking a protein FASTA.

**The shipped non-coding transform declares 2 of the 12 files bakta writes.** Read from
`bakta.main` in that image: `.tsv .gff3 .gbff .embl .fna .ffn .faa .inference.tsv
.hypotheticals.tsv .hypotheticals.faa .json .txt`. `.gbff` is the one that matters, because it is
the annotated genome record everything downstream of `annotate` reads.

**Three capabilities ship and are locked to one namespace.** `amplicon/blast_map_asvs.py` is a
complete query-against-subject BLAST. The six aspire ecology transforms do every KBase community
statistic. Both are typed to one pipeline, so nothing else reaches them. That is a refactor, not a
transform to write.

**GTDB-Tk already writes the marker alignment and throws it away.** `taxonomy/gtdbtk.py` runs
`classify_wf`, which performs identify and align, then globs only `classify/*summary.tsv`.

**The metabolic lane is written, in the fabfos scope.** `vs_gem/fba_scaffold.py` parses MetaNetX
equations into cobra reactions and inserts universe reactions, `bridge.py` holds the curated models,
`env::cobra.env` already runs inside a metasmith transform at
`src/fabfos/build_references/transforms/benchmark/host_gpr_gem.py`. Four proposals adopt it.

## What is left open

Reachability is a claim about the VERB graph: every verb has a producer. It does not prove the
planner finds a chain. A later round has to write the transforms and re-solve the 623 shapes in
`../../templates/solve_results.jsonl`.

`binning` records a decision rather than a proposal. `sequences::bin_fasta` extends
`putative_genome` and not `assembly`, so a bin reaches checkm, GTDB-Tk and fastANI and reaches no
annotator. **CAUTION** separately, `binning::derep_mag_ref` is required by both inStrain transforms
and produced by nothing. That is a dangling requirement regardless of what is decided.
