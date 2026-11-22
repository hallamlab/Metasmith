# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
rival-audit

## Waiting on the user
- lab-sifs: DTU academic licence downloads (SignalP 6, DeepTMHMM, DeepLocPro; Phobius via its own form) — Tony requests them; I build the lab SIFs from them and from the Pathway Tools installer on fir

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-21: Tony is losing track: short messages, one question, stack everything else.
- 2026-11-21: ask questions as plain messages, never popups (accidental clicks). Popup answers after ~wrapper-ports are suspect: CarveMe answer re-asked.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-21: engcyc metagenomics is the full template; every lane deselectable; docs list each lane's reference size; run 4 runs a stated subset.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- wrapper-build-run — start DRAM, DRAM-v, PathoFact port workers once annotation-aggregate lands (decision in DECISIONS.md)
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape)
- replace-old — retire metagenomics_from_paired_reads; goldens; touched tests
- run-4 — "check that an agent can E2E install msm and run the template"
- mp-port-ship — mp-fix landed on feat/engine/mp-fix (a5be461b, b91e3394, 846b4182): per-MAG overlap 0.49 -> 0.62-0.66, community 0.55 -> 0.63-0.69 (names + CLEAN conf >= 0.5). Decide: enough to ship, or pull the next lever (MetaCyc protein DB hits + Swiss-Prot ECs, as MP4)
- shape-targets-followups — from worker d1feecd8: hybrid case skipped MEGAHIT only by solver choice (no rule; same gap as consume-distinguishing-products); `when` on an input-only type gives an error that does not mention conditions; same target with two `when` lists duplicates; no GUI widget for `when` (yaml/json/Python only)
- hint-fix — my search_exhausted hint claims every target is producible when one case cannot reach one (hybrid-solve V2/V3); gate it per case
- mixed-study-check — a study holding short and hybrid samples is one case; plan 'succeeds' with OPERA-MS for samples lacking long reads; fail it
- type-traps — `_` description is a matched property; extends cannot cross namespaces; pool keeps a given's first parents (match by name)
- cache-issues — from mp-fix: resource edits do not invalidate the cache; adding an env recompiles every env id and misses the whole cache; a rerun refuses on a stale lock of an exited driver; comparison.md section is hand-written over a generated file
- port-gaps — mature gaps after run 4, one at a time: MS ion ID (pilot ok) + MS ordination (figures tuned, https://claude.ai/artifact/6EbejKytP9ZMrtv5Bt1n8V); MZmine feature table; matchms/LipidBlast; RNA-seq extras; amplicon thresholds; trees/TreeSAPP; ONT methylation; fosmid insert recovery
- exploratory-gaps — stay in their projects until methods settle.
- experiment-libs — fate of benchmark/library, beth_rnaseq/library, kalen mocklib, compound_id mock
- later-templates — pangenome, isolate assembly, hybrid assembly, asv, transcriptomics from the same library

## Workers
- licensed-build — licensed package input -> build step -> image -> lane; off by default (feat/engine/licensed-build)
- annotation-aggregate — annotation table + GPR table from it (feat/engine/annotation-aggregate)
( Branches to merge later: feat/engine/mp-fix, feat/engine/hybrid-solve, feat/engine/shape-targets d1feecd8 — research/hybrid_solve/solve.py add/add conflict with hybrid-solve; take shape-targets' copy)

## Closed
- licensed-tools — download is an input; a build step makes the image once (cached); lanes off by default; lab SIFs built from lab downloads
- localization-lanes — DeepTMHMM + PSORTb lanes (DeepTMHMM licensed)
- wrapper-build — DRAM/DRAM-v/PathoFact ported: run only lanes we lack, reuse ours, upstream rules over the annotation table
- annotation-aggregate — two levels: annotation table (every lane) -> GPR table (reaction lanes); consumers subset
- catalog-survey — template now (+ resistome lanes, FEAST dropped); mature gaps after run 4; MS ion ID + ordination join port-gaps
- mixed-session — support now: targets carry the given types they need; each shape gets the targets it satisfies
- read-types — short_reads and long_reads only; parity/platform in read_metadata; pe/se types deleted in stdlib
- hybrid-assembly — chosen ahead of time; explicit hybrid target; hybrid lane in the standard library; no hybrid SRA outcome
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
