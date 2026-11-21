# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
template-targets / taxonomy

## Waiting on the user

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-21: engcyc metagenomics is the full template; every lane deselectable; docs list each lane's reference size; run 4 runs a stated subset.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- template-targets — "E5 + taxonomy or other stuff from cyanoverse + annotation work with Beth", incl. viromics
  - taxonomy — Metabuli, GTDB-Tk on quality MAGs, LCA* contigs, rRNA
  - annotation — ORFs, UniRef50, KOfam, MP lane, Bakta, tRNA, antiSMASH, PathoFact, mobileOG, DRAM
  - gem — CarveMe + MEMOTE on quality MAGs
  - viral — callers, CheckV, vOTUs, DRAM-v, iPHoP, vConTACT3
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape)
- replace-old — retire metagenomics_from_paired_reads; goldens; touched tests
- run-4 — "check that an agent can E2E install msm and run the template"
- later-templates — pangenome, isolate assembly, hybrid assembly, asv, transcriptomics from the same library

## Workers
- mp-fix — port feeds PathoLogic product names + ECs; rerun 2-3 CPE3 assemblies on fir; compare (own worktree branch)

## Closed
- binning — DAS Tool over 3 binners -> CheckM2 -> quality MAGs (>=50/<=10)
- mp-port-check — fix and recheck; MP lane ships once pathway overlap rises
- read-classes — template picks the mode by loading one assembler folder; downstream takes any assembly
- library-shape — one library; one tool per job by default; rivals in transforms/alternatives/
