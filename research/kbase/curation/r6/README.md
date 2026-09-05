# Curation round 6 — the harness picks the runtime, and the type graph stops asking

Round 5 wrote and ran every KBase parity transform and handed forward a punch list.
Most of that list turned out to be one of two things: a decision nobody had made, or a
fact an author was asked to restate that the engine already knew. This round deletes the
restatement and works the rest.

## What this round did

- **The two execution arms are gone.** A body declared one arm per runtime —
  `ifContainerDo` and `ifVirtualEnvDo` — and across 259 chains, 122 declared both and
  **121 passed a byte-identical command to each**. One call launches a tool now:
  `context.ExecWithEnv(env=, cmd=)`. All 259 call sites moved with their argument text
  lifted verbatim out of the source; `verify_migration.py` is the check, and 258 came
  back byte-identical with one deliberate change.
- **The preflight asks the environment, not the body**, and in both directions. It used
  to ask whether a body declared an arm. It now asks whether the environment resource
  declares the key this runtime needs — and the container direction was never checked at
  all, which is what let `cobra.env` ship with a `conda:` line and nothing else.
- **Four type-graph threads close.** `bowtie2_bam` is an `alignment::bam`; the modelling
  and metabolomics lanes meet; `sequences::sample_name` exists; `derep_mag_ref` has a
  producer. None needed a new mechanism.
- **The cobra image is pushed and verified**, and all four modelling transforms re-run
  against it reproduce round 5's numbers exactly.

## The suite, run to completion

It had not completed since the engine changed, and this round changed the engine again. Run one
directory at a time, each capped at 12G — a single un-chunked run was OOM-killed at 60 minutes
by a box under load, and piping it through `tail` meant that hour left a 10-byte file.

**2078 passed, 5 failed, 312 skipped, 5 xfailed.** `suite.log` is the record.

One of the five is round 6's own, and the guard was right to catch it:
`test_module_surface.py::test_exported_names_survive` holds that a split module may not silently
stop exporting a name. `metasmith.models.libraries` stopped exporting `CONTAINER_ARM`,
`VIRTUAL_ENV_ARM` and `EnvDispatch` — deliberately, they have no callers left — so those three
were removed from `fixtures/module_surface.json` with the reason recorded beside them, rather
than the snapshot being regenerated wholesale, which would have absorbed any other drift too.

The other four are pre-existing and none is caused by this round:

- **Two `test_cache_real_nextflow` timeouts.** Not flakiness. Its cleanup walks up from the work
  dir to `/tmp/pytest-of-tony` and chmods that whole tree inside a container against a 120s
  timeout; the tree holds 57,030 files that belong to every other pytest run on the box. Written
  up in `docs/metasmith/plans/consolidation-followups.md`.
- **`test_stage_real_libraries_clones_into_sandbox`** asserts a `.git` its helper stopped
  writing when the library stopped being a separate repository.
- **`test_an_engine_is_staged_for_this_platform`** — no `msm_solver` built for this scope. It is
  a per-scope build artifact, and it is also why 237 of the solver tests skip.

## What is in this directory

| file | what it holds |
|---|---|
| `_plans_r6.txt` | the eleven templates' plans. `diff` against `../r5/_plans_r5.txt`. |
| `verify_migration.py` | compares every migrated call site against its pre-migration source in git |
| `anon_pull.py` | asks each registry whether an ANONYMOUS pull resolves each env's image |
| `anon_pull.txt` | that sweep's output: 90 files, 9 unresolvable, 1 with no `container:` |
| `suite.log` | `tests/metasmith` run to completion, one directory at a time |

## The insight both type joins turn on

`test_no_cross_file_extends` forbids an `extends` that names a type in another file, and
rounds 3, 4 and 5 read that as closing two questions. It does not. **Matching never
needed `extends`**: a type satisfies another when its property set is a SUPERSET, and
subsumption is cross-file capable. Each pair differed only in `_` — which is a *matched
property*, not a description — so aligning `_` and moving the prose to a `#` comment was
the whole join, in both cases.

That is also why the arms could go without a deprecation: the checks the author-side
eligibility rule asked for were already in the engine, written where they can fail
honestly rather than in prose for a human to apply.

## Decisions this round made rather than fixed

**A bin is not an assembly.** A bin is a mapping of contigs; annotation must not sit
downstream of one, or every annotation is scoped to one binner's grouping and has to be
redone when the grouping changes. Three tests asserted this without saying why, which is
why it was re-raised three rounds running. The reason is now at `bin_fasta` in
`data_types/sequences.yml` and the tests point at it.

**`star_bam`, `merged_bam` and `organellar_bam` do not join `alignment::bam`.** The
first two descend from `sequences::assembly` through `star_index`, so the lineage
constraint on the three binners and on `bcftools_variants` would not stop a splice-aware
RNA aligner's output being read as binning coverage or as variant evidence. Only the
type does.

**`sample_name` is deferred, not mandatory-with-no-producer.** Like `ncbi::genome_name`.
Round 5 showed twice that a mandatory requirement nothing produces takes every target in
a spec down, not just the one that wanted it.

## What running and re-solving found that nothing static would

**A self-loop in the assembly lane.** `a4_mags_from_metagenome` grew from 13 steps to 19,
and the cause is not the bam types. Its plan is an eleven-long
`seqkit_filter_contigs` → `polypolish` → `filter` ladder before `assembly_stats`: both
transforms produce a type that EXTENDS `sequences::assembly` and both *require*
`sequences::assembly`, so each satisfies its own requirement and the solver walks the
cycle. Every product in the ladder is consumed by the next step, so it is a real chain
rather than dead branches. The ladder was already five long before this round; widening
`alignment::bam` perturbed the search and lengthened it. This is a solver and
transform-modelling question, not a type one, and it is the clearest item to hand
forward.

**`esm_c`'s two arms were the only divergent pair in the repository, and the arm that
looked portable was the broken one.** Its container arm prepended
`pip install huggingface_hub`; the conda arm dropped it — but
`envs/fabfos/python_for_data_science.yml` did not declare the package either, so the
"portable" route would have died at import had it ever run. The package went into the
conda spec and the install stays in the one command, which is what six transforms under
`logistics/` already do.

**The cobra biocontainer is real and still not usable.** `quay.io/biocontainers/cobra`
IS cobrapy, is public, and its LP solver returns the right objective. But its newest tag
is 0.29.1 from December 2024 — the package moved off bioconda to conda-forge — and it
carries neither `requests` nor `scipy`, while `lib::modelling` fetches BiGG models with
requests. Checked rather than assumed.

**Three environments recorded as unused are used by 21 transforms.** `rdkit`,
`equilibrator` and `dgbyg` were carried forward as required by nothing and proposed for
deletion. That count was taken over the standard library alone; across
`src/fabfos/build_references` they are required by 16, 4 and 1 transforms — the whole AAM
and direction lanes. Nothing was deleted.

## What this round did not close

- **`hallamlab/cobra` is private**, so `cobra.env`'s `container:` line is written and
  commented out. Quay creates a new repository private and the changevisibility API needs
  an OAuth token, which a docker push credential is not. One click at
  `https://quay.io/repository/hallamlab/cobra?tab=settings`, then uncomment the line —
  `cobra.env` carries the verification command.
- **The assembly self-loop above.** Named, measured, not fixed.
- **`pathologic.env` names `:latest` in a personal namespace.** The only floating tag in
  ninety files, and `txyliu/` rather than `hallamlab/`.
- **Six environments carry no `conda:` line**, so they are unrunnable under MAMBA. The
  preflight now says so by name at staging instead of failing at run.
- **Thirteen transforms still use `read_metadata` as a grouping ancestor** and were not
  retrofitted onto `sample_name`. Two sites were wired; the lane is not done.
