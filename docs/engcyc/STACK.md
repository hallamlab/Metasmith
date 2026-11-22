# engcyc library and template — discussion stack

Goal: one curated library that serves every template, and an "engcyc metagenomics" template
(E5 + cyanoverse taxonomy + Beth annotation + viromics) replacing metagenomics_from_paired_reads,
then run 4 (a naive agent installs msm end to end and runs it).
Decisions: docs/engcyc/DECISIONS.md

## Current
lab-sifs

## Waiting on the user
- lab-sifs: Tony fills the forms in the alcor browser seat (viewer VNC). Found 2026-11-21: SignalP 6.0i fast = DTU sw_request form (institutional email, link by email); DeepTMHMM = Microsoft Forms request from dtu.biolib.com/DeepTMHMM (non-commercial; package sent); Phobius = direct download phobius101_linux.tgz (academic); DeepLocPro = public GitHub Jaimomar99/deeplocpro, CC BY-NC-SA 4.0, no form, so not a licensed lane (build from GitHub); Pathway Tools installer already on fir

## Preferences
- 2026-11-19: one topic at a time, dfs-interview.
- 2026-11-21: Tony is losing track: short messages, one question, stack everything else.
- 2026-11-21: ask questions as plain messages, never popups (accidental clicks). Popup answers after ~wrapper-ports are suspect: CarveMe answer re-asked.
- 2026-11-19: phyloFlash dropped from the template. BBDuk (JGI) -> MEGAHIT for short reads.
- 2026-11-21: engcyc metagenomics is the full template; every lane deselectable; docs list each lane's reference size; run 4 runs a stated subset.
- 2026-11-19: keep as much as possible in one library. kbase may fold in. aspire and transcriptomics are covered too.

## Stack
- annotation-aggregate-followups — from 49112ec6: research drivers reparse_run.py and scadc_metag_gpr_4lane.py call the deleted 4-lane lib (port them); PathoFact not in the table (no batch merge, unknown output, image unpullable) — goes with the PathoFact port; BiGG lane has no BiGG->MNXR crosswalk so it is not in the GPR tables, and carveme_from_orfs still runs its own search instead of reading bigg_diamond_hits; antiSMASH/RGI/AMRFinderPlus parsers written from docs, check on real output; lib::fabfos_gpr is a fabfos name for general code; annotation table needs an assembly (bare-ORF fosmid path keeps the 4-lane route); gpr_4lane still a separate producer of gpr_table (pinned equal to the all-lanes subset)
- licensed-build-followups — from 717e28f8: build recipes unverified against real downloads (verify with the staged files in ~/licensed-staging); DeepLocPro is public CC BY-NC-SA on GitHub, so drop its licensed type and build from GitHub; fir apptainer build without root unknown (else build SIFs on altair); docker loads the whole archive per call (~4 GB PT); build steps run on login node; fir drivers pin agent image 0.24.2 (SIF copied into store, not run in place); no DeepTMHMM/DeepLocPro/Phobius consumer lanes yet
- dram-port-followups — from bb2ede96: old dram_annotate_genes + buildDramDB (24 GB Pfam-A.full) overlap the port, retire (no rivals); IPS Pfam lacks version so Pfam ids never reach distill (no shipped form uses them); MEROPS via DIAMOND not mmseqs; rank only C/D/E (no reverse search); UniRef50 top hit lacks DRAM's bitscore>=60 cut; genome stats lack taxonomy/completeness/tRNA/rRNA; dbCAN/VOGDB descriptions written at merge, not chunk; nothing run end to end; comparison vs upstream DRAM on one CPE3 MAG (research/dram_port/README.md) not run
- wrapper-build-run — DRAM done (bb2ede96); DRAM-v running; PathoFact next (decision in DECISIONS.md)
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
- rival-audit — list every rival per job and move it to transforms/alternatives/ (from library-shape) (deferred by Tony 2026-11-21)
- later-templates — pangenome, isolate assembly, hybrid assembly, asv, transcriptomics from the same library

## Workers
- dramv-port — DRAM-v port on feat/engine/dramv-port (based on dram-port)
( Branches to merge later: feat/engine/mp-fix, feat/engine/hybrid-solve, feat/engine/shape-targets d1feecd8, feat/engine/annotation-aggregate 49112ec6, feat/engine/licensed-build 717e28f8, feat/engine/dram-port bb2ede96 (on annotation-aggregate) — research/hybrid_solve/solve.py add/add conflict with hybrid-solve; take shape-targets' copy)

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
