# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
catalog-survey

## Waiting on the user

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-21: ask questions as plain messages, never popups (accidental clicks). Popup answers after ~wrapper-ports are suspect: CarveMe answer re-asked.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-21: engcyc metagenomics is the full template; every lane deselectable; docs list each lane's reference size; run 4 runs a stated subset.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- catalog-survey — state: survey-kbase reported 2026-11-21 (est. ~50/54 KBase verbs answered; reachability 91% is an unre-solved projection, 1 of 3 refactors done; gene_set still unproduced; bacterial RNA-seq chain plans in 6 steps but never ran, no template; ~20 rival groups library-wide incl. read QC, read profilers x6, GTDB-Tk x5, embeddings x5, carveme cplex). survey-analyses reported 2026-11-21: 17 analysis kinds outside the library. Mature gaps: msconvert+MZmine 4.7 feature table; matchms/LipidBlast annotation; featureCounts+strand sweep, DESeq2 dose trend, TPM (beth_rnaseq/library); amplicon threshold selection + cluster-stability bootstrap (steven-chen); tree inference / TreeSAPP refpkgs; ONT methylation pileup + reference diff (kalen mocklib); fosmid insert recovery (biofactorial). Exploratory: compound-ID ensemble (mock + approved plan), lipid abundance, MS ordination/MI, MMD-RFF, in-silico screen, CLIPZyme, copper sites, NMF/VAE guilds, gene-importance ML, GECCO/DeepBGC, Rfam, gapseq. Experiment-local libraries: metasmith_benchmark/library, beth_rnaseq/library, kalen_crage/mocklib, compound_id/mock. "percent coverage of kbase, and work in transcriptomics, and the scadc metabolomics? ... curated set of everything"
- annotation-aggregate — one table of every annotation lane; consumers subset it (4-lane, CarveMe BiGG lane, DRAM-port lanes)
- wrapper-build — port DRAM, DRAM-v, PathoFact into parts in the standard annotation format; template waits
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape)
- replace-old — retire metagenomics_from_paired_reads; goldens; touched tests
- run-4 — "check that an agent can E2E install msm and run the template"
- later-templates — pangenome, isolate assembly, hybrid assembly, asv, transcriptomics from the same library

## Workers
- mp-fix (resumed) — continues feat/engine/mp-fix from a5be461b (UniRef50 product names beside CLEAN ECs); rerun on fir; per-MAG overlap matters most

## Closed
- isolate-hifi — assemblers/isolate = hifiasm_meta only; hifiasm, filtlong_targeted, miniasm deleted; hybrid stays its own folder
- gem-reopen — CarveMe stays; its BiGG search is a lane in one aggregate annotation table that consumers subset
- wrapper-ports — wait for all ports; ports emit the standard annotation format (GPR lanes)
- viral — 4 callers, CheckV, curation, vOTUs; DRAM-v (ported), CCTyper, vConTACT3, iPHoP with MAGs grafted
- template-targets — all lanes decided (see children)
- gem — E5 split CarveMe 1.6.6 + MEMOTE on subset MAG ORFs; old CarveMe deleted
- annotation — UniRef50, KOfam, IPS, eggNOG, 4-lane GPR, Bakta nc, tRNA, antiSMASH, PathoFact, mobileOG; models per MAG; DRAM ported
- taxonomy — GTDB-Tk on quality MAGs + rRNA scan; Metabuli and LCA* out (1.4 TB DB)
- binning — DAS Tool over 3 binners -> CheckM2 -> quality MAGs (>=50/<=10)
- mp-port-check — fix and recheck; MP lane ships once pathway overlap rises
- read-classes — template picks the mode by loading one assembler folder; downstream takes any assembly
- library-shape — one library; one tool per job by default; rivals in transforms/alternatives/
