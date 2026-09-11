# Curation round 4 — the transforms land, and the chains are planned

Round 3 read all 493 KBase apps against the shipping standard library and proposed 22
transforms plus three refactors. It wrote no code, on purpose. Round 4 turns that proposal
into declarations, curates the ASPIRE lane it sits beside, and then asks the planner whether
the chains connect.

**The report:** https://claude.ai/code/artifact/b2081ebc-9c1f-416c-a17f-2374c88ef523

## What this round did

- 21 transforms now exist under `src/metasmith_libraries/transforms/kbase/`, one directory
  per KBase task verb. Every body is a stub that touches its outputs; the signature is the
  deliverable. `research/kbase/curation/r3/proposals.yml` is where their intended behaviour
  is written, and it is what the next session works from.
- 28 new types across two new namespace files (`comparative.yml`, `modelling.yml`) and seven
  existing ones, plus four pinned tool environments.
- The ASPIRE lane went 50 rows to 46 and 138 types to 132, and eleven rows came off the
  `aspire::run` gate onto the generic `amplicon::survey` node.
- Eleven analyses were planned end to end. All eleven solve.

## What is in this directory

| file | what it holds |
|---|---|
| `aspire_topology.md` | the verdict on every one of the 50 ASPIRE rows: folded, collapsed, or kept, with the reason. Written before any edit. |
| `template_gate.md` | the eleven shipped templates before and after, step for step, and the four requirements that became ambiguous. |
| `analyses.md` | the eleven analyses, the two that failed first and why, and the non-minimal plans. |
| `_render_artifact.py` | renders the report above from the probe's JSON records. |
| `_plans_head.txt`, `_plans_r4.txt` | every step of every shipped template's plan, both sides. `diff` them. |
| `_ambiguity_head.txt`, `_ambiguity_r4.txt` | every requirement with more than one producer, both sides. |
| `_head_templates.log`, `_r4_templates.log`, `_analyses_run.log` | the runs themselves. |

The probe that produced the analyses lives outside this directory, with the other library
probes: `research/metasmith_libraries/probe_kbase_parity.py`, writing
`kbase_parity.jsonl` beside it.

## What round 4 did not do

Round 3 named three refactors. The first — the ecology lift — is done. The other two are
still open: shipping a transcriptomics template, and deciding whether a bin is an assembly
(`sequences::bin_fasta` extends `putative_genome` and not `assembly`, which decides whether
bin → annotate → build_model is walkable at all).

Two findings from this round are also open, both recorded in `analyses.md`: there is no
sample-level grouping type above `sequences::read_metadata`, which is what a hybrid isolate
assembly needs; and `modelling::` runs beside `metabolomics::metabolic_model_sbml` rather
than feeding it.
