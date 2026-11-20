# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
template-targets

## Waiting on the user

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- template-targets — "E5 + taxonomy or other stuff from cyanoverse + annotation work with Beth", incl. viromics
  - read-classes — short / long / hybrid from one SRA accession
  - mp-port-check — "ported metapathways (needs chcking that the port was sucessful)"
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape)
- replace-old — retire metagenomics_from_paired_reads; goldens; touched tests
- run-4 — "check that an agent can E2E install msm and run the template"
- later-templates — pangenome, isolate assembly, asv, transcriptomics from the same library

## Workers

## Closed
- library-shape — one library; one tool per job by default; rivals in transforms/alternatives/
