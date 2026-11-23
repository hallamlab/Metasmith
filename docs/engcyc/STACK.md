# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
solve-scenarios — isolate (11 steps), hybrid (173) and short (172) solve after the solver fixes (merged 1e3f3ae7; budget 512 911a5e4a). SRA accession: merge_candidate_calls now anchors on read_metadata and engcyc loads ncbi.yml (8ca8722b), but the solve still fails on the getSraReads fork (long-reads outcome cannot reach the targets; "no single plan within 247 iterations"). Fable agent diagnosing on feat/engine/sra-fork.

## Waiting on the user
- sra-fork: A typed accessions (sra_short_accession / sra_long_accession, one fetch each; recommended) vs B drop unroutable fork outcomes vs C per-outcome target subsets. Cause: getSraReads' long outcome is solved first and cannot reach instrain/metaphlan/strainphlan; refusing is the documented contract (2 tests in tests/metasmith/flow/test_forks.py); short outcomes merge to 176 steps

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-21: Tony is losing track: short messages, one question, stack everything else.
- 2026-11-21: ask questions as plain messages, never popups (accidental clicks). Popup answers after ~wrapper-ports are suspect: CarveMe answer re-asked.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-21: engcyc metagenomics is the full template; every lane deselectable; docs list each lane's reference size; run 4 runs a stated subset.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- annotation-aggregate-followups — from 6504e5cb: research drivers reparse_run.py and scadc_metag_gpr_4lane.py call the deleted 4-lane lib (port them); PathoFact not in the table (no batch merge, unknown output, image unpullable) — goes with the PathoFact port; BiGG lane has no BiGG->MNXR crosswalk so it is not in the GPR tables, and carveme_from_orfs still runs its own search instead of reading bigg_diamond_hits; antiSMASH/RGI/AMRFinderPlus parsers written from docs, check on real output; lib::fabfos_gpr is a fabfos name for general code; annotation table needs an assembly (bare-ORF fosmid path keeps the 4-lane route); gpr_4lane still a separate producer of gpr_table (pinned equal to the all-lanes subset)
- licensed-build-followups — recipes verified on real downloads (770ed56c, merged 01a47c9f); five SIFs in ~/licensed-staging/sif; on fir licensed/ and chinook archive (journal 24a54a65). Open: DeepLocPro has no env file or lane, public image push to quay needs Tony's ok (CC BY-NC-SA); test_built_image.py not rerun after last two recipe fixes; PT SIF drops xvfb/less (GUI mode untested); DeepTMHMM uses every CPU (cap threads in its lane); docker loads the whole archive per call (~4 GB PT); build steps run on login node; fir drivers pin agent image 0.24.2; no DeepTMHMM/DeepLocPro/Phobius consumer lanes yet; Phobius Perl warnings harmless
- dram-port-followups — from 3abba16d: old dram_annotate_genes + buildDramDB (24 GB Pfam-A.full) overlap the port, retire (no rivals); IPS Pfam lacks version so Pfam ids never reach distill (no shipped form uses them); MEROPS via DIAMOND not mmseqs; rank only C/D/E (no reverse search); UniRef50 top hit lacks DRAM's bitscore>=60 cut; genome stats lack taxonomy/completeness/tRNA/rRNA; dbCAN/VOGDB descriptions written at merge, not chunk; nothing run end to end; comparison vs upstream DRAM on one CPE3 MAG (research/dram_port/README.md) not run
- dramv-port-followups — from 826885a5: old dramv.py + viromics_survey_from_assembly still target annotation::dramv_distill (retire after comparison); annotation_table grouped by assembly so a multi-sample study builds duplicate frozen-set tables; every annotation_table now runs the RefSeq viral search; vMAG_stats VirSorter category/Circular/Prophage always empty (VirSorter1 name suffixes); reps without VirSorter2 calls score 5, no AMGs; template gate (-b) not run; comparison vs upstream DRAM-v not run; docker daemon hung on altair (stopped container dramvfix, /tmp/dramv_fix/sif)
- wrapper-build-run — DRAM done (3abba16d); DRAM-v done (826885a5); PathoFact done (fed84f3d, merged 875a1f6e)
- pathofact-port-followups — from fed84f3d: ported PathoFact 2.0 (bd5d527), not v1: MGE arm is geNomad (reused), no PlasFlow/MOB-suite/VirSorter/DVF (Tony accepted 2.0, 2026-11-22); six env images digest-pinned but none pulled or run in a container; hmmsearch and DeepARG lanes never run; annotation table now needs 6 GB PathoFact DB + 1.9 GB DeepARG before it builds (make lanes deselectable); SignalP variant unsolvable until a signalp6_results producer exists (lab-sifs); run_port.py pathofact_amr now pulls the full table, old fir pathofact_db lacks source; ORFs Prodigal vs upstream pyrodigal-gv (50% overlap match); dereplication rep = first in FASTA; tool versions differ (RGI BLAST, AMRFinderPlus 4.2.7, geNomad 1.11); SignalP6->gff3 mapping from public code; deeparg image diamond version unconfirmed; fir comparison (research/pathofact_port/README.md) not run; delete /home/tony/scratch_pf (1.5 GB)
- mp-faithful-followups — from 7a5ad1e3/39ddc95b (merged f17dcc96): MAG overlap with MP4 0.62-0.66 -> 0.92-0.94, community 0.95-0.96. Open: annotation_table now REQUIRES swissprot/uniref90/metacyc lanes whose refs (UniProt 2025_04, MetaCyc 27.0 from licensed PT) have no download/build transforms, so a default solve drops the table (probe now gives them); full engine PathoLogic route solved, not run on fir; diamond_uniref50 keeps its own BSR copy (cache); research/mm_compare/ecoli_pathologic.py plans differently now; remaining diffs DIAMOND vs FAST, no eggNOG/CAZy names, no identity>=20 cut, different PT 27 build; _clean_stitle fix changes UniRef50 names in GPR lanes; metapathways.env repinned 4.0.1 -> 4.0.3
- multi-sample-collapse — AsSamples (instances.py:295) makes a sample = all descendants of its index item's ancestors; engcyc parents read_metadata to the shared study, so every sample of a study collapses into one (the mixed probe is one sample). Fix before any multi-sample engcyc run
- hybrid-budget — superseded by solver-investigation (Tony wants the cause found, not a split). Merged 891e4c34: solver fixes (reflexive lineage anchor; one endpoint fills several targets), rename to engcyc_metagenomics, engcyc_prep_references (29 targets, 40 steps, ~680 GB, steps identical to main plan). Hybrid/mixed still fail: a hybrid sample has 4 competing assemblies (MEGAHIT, Flye, raw Flye, OPERA-MS) and the search runs out of its 256-iteration budget; short reads already use 255/256 (any new target may break short too); 384 iterations -> 47 GB solver memory. Options: (1) per-shape transform allow-list (engine change; reverses read-classes), (2) hybrid-only input type (rejected before), (3) lineage-aware scoring + memory cap (research), (4) split engcyc into staged templates (assembly+binning, then annotation from assembly)
- metabuli-r232 — worker building HQ r232 Metabuli DB on fir (/scratch/phyberos/engcyc/metabuli_r232/); then: where to host it (open), library download transform + Metabuli lane in engcyc_metagenomics and prep
- template-build-followups — licensed lanes (PathoLogic, SignalP) and every-lane table are left out of the target list, not off-by-default targets; PathoFact BGC column reads raw antiSMASH JSON; CarveMe rebuilds DIAMOND hits from the BiGG lane table with zeros for missing columns; UniRef50 current release vs EC map 2025_04; iPHoP still uses the skani cluster table; MetaBAT2/SemiBin2 long-read settings not folded in; short_reads_pe/se types not deleted; no hits tables for DIAMOND or PathoFact HMM lanes; SignalP has no lane or hits table; PathoLogic gather lacks identity>=20 cut; hybrid downstream lanes read whichever assembly the planner picks (CAUTION in description); UniProt 2025_04 archive layout and MetaCyc export untested; dev/libraries.sh -b not rerun in full; test_annotation_driver and test_reads_to_ecspr_workflow fail on altair (host setup?); short-read solve takes 197 s; missing lanes: DeepVirFinder (only research e5), PSORTb, DeepTMHMM, Phobius, DeepLocPro; ~20 research/ and data/ files still name metagenomics_from_paired_reads
- gpr-data-layout — test_gpr_schema::test_the_schema_covers_every_gpr_table_in_the_tree fails on untracked data/fabfos/runs/{fang,fuhrer_clones,keio}/gpr/*.parquet (pre-schema layout); regenerate or drop those tables
- release-0.26 — geoquery cuts 0.26.0 first, alone. At template-build (b): merge released feat/dev into rc, carry decided fixes untested. After run 4 is clean (c): release from rc; first check geoquery_from_aoi_table still solves and its tests pass (no regression). AGENTS.md conflict to resolve on our side
- deeplocpro-licensed — Tony 2026-11-22: "DeepLocPro is under license" / "it is installable from git? that is simpler". Licensed lane (off by default, never a published image) whose build step installs from GitHub (docker/deeplocpro recipe, pinned commit) instead of taking a user download; image built and cached locally per user. Lab copy = ~/licensed-staging/sif/deeplocpro.sif (GitHub build). quay external_deeplocpro:1.0 pushed private by mistake — Tony to delete
- dev-bugs-followups — fixed on feat/engine/dev-bugs, merged b36fc450 (186c5503 probe warns, e05d3e76 failed-run sentinel + reason, fdd1630c leaf stamp re-mint, f50942f8 `msm data ensure` batch + retry; arrays already pinned). Value rows: ID = hash(value + path) (948f6929, merged 5d03cd86; old values keep ID until edited; old Python-API values stay plain leaves); agents older than 0.26.1 lack `data ensure` (redeploy); old indexes catch edits only after next save; resource-edit invalidation (cache-issues) not checked end to end; reply to the five engine/dev reports at release
- run-4 — "check that an agent can E2E install msm and run the template"
- shape-targets-followups — from worker 5155d9ce: hybrid case skipped MEGAHIT only by solver choice (no rule; same gap as consume-distinguishing-products); `when` on an input-only type gives an error that does not mention conditions; same target with two `when` lists duplicates; no GUI widget for `when` (yaml/json/Python only)
- hint-fix — my search_exhausted hint claims every target is producible when one case cannot reach one (hybrid-solve V2/V3); gate it per case
- mixed-study-check — a study holding short and hybrid samples is one case; plan 'succeeds' with OPERA-MS for samples lacking long reads; fail it
- type-traps — `_` description is a matched property; extends cannot cross namespaces; pool keeps a given's first parents (match by name)
- cache-issues — from mp-fix: resource edits do not invalidate the cache; adding an env recompiles every env id and misses the whole cache; a rerun refuses on a stale lock of an exited driver; comparison.md section is hand-written over a generated file
- port-gaps — mature gaps after run 4, one at a time: MS ion ID (pilot ok) + MS ordination (figures tuned, https://claude.ai/artifact/6EbejKytP9ZMrtv5Bt1n8V); MZmine feature table; matchms/LipidBlast; RNA-seq extras; amplicon thresholds; trees/TreeSAPP; ONT methylation; fosmid insert recovery
- exploratory-gaps — stay in their projects until methods settle.
- experiment-libs — fate of benchmark/library, beth_rnaseq/library, kalen mocklib, compound_id mock
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape) (deferred by Tony 2026-11-21)
- later-templates — pangenome, isolate assembly, hybrid assembly, asv, transcriptomics from the same library

## Workers
- metabuli-r232 — new worker (Sonnet, medium) continues the fir build from the old worker's scripts
- sra-fork — Fable agent: SRA accession solve (getSraReads fork) on feat/engine/sra-fork
(Merged into rc 2026-11-22: mp-fix, hybrid-solve, shape-targets, licensed-build 86592509, dramv-port incl. dram-port + annotation-aggregate; pathofact-port 875a1f6e. Merge again when lab-sifs, mp-faithful, pathofact-port land.)

## Closed
- solver-investigation — three causes (lineage-blind selection, DFS thrash on unsatisfiable late slot, full cross product of target applications); fixes merged 1e3f3ae7, budget 512 (911a5e4a); hybrid needs 313 iterations
- replace-old — merge finished branches now (done); template built after the last three ports, replacing the old one in the same commit
- mp-outputs — only MP4's pathway table (score, coverage, ORFs, report fields) replaces ours; other MP4 outputs not rebuilt
- mp-port-ship — faithful to MP4: names + MetaCyc hits + Swiss-Prot ECs, CLEAN out of PathoLogic input; recheck then ship
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
