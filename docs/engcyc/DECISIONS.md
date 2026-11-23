# engcyc library and template — decisions

## library-shape (2026-11-19)

Question: how does one library serve every template when the solver has no tool preference?

Tony: "We should curate a set so that preference is not an issue". Rivals go to an opt-in folder.

Design:
- The standard library stays the one library. aspire, kbase and transcriptomics are already folders
  in it, and all 13 folders load together in about 2 s per target.
- The default folders hold one tool per job and read class. MEGAHIT is the short-read assembler.
  Flye is the long-read assembler.
- Rival tools move to one opt-in folder, `transforms/alternatives/`. No template loads it by default.
  A template that needs a rival (for example hifiasm for isolate assembly from HiFi) loads it and pins
  the type.
- No engine change.

Landed: the rival audit (stack topic rival-audit) lists the moves once the template lanes settle.

## read-classes (2026-11-21)

Question: which read classes does engcyc metagenomics take, and how do hybrid and isolate assembly fit?

Tony: "Isolate and hybrid assembly are opt in modes that must connect to down stream tools." He
rejected per-sample input types ("I don't think you can do input sets like that"): a hybrid sample
holds a short and a long read set, so MEGAHIT and Flye still qualify for it.

Design:
- The template picks the mode by loading exactly one assembler folder.
  - `assemblers/metagenome`: MEGAHIT for short reads, Flye for long reads. engcyc metagenomics loads it.
    It covers all three SRA outcomes: long, paired short, single-end short.
  - `assemblers/hybrid`: a MEGAHIT draft under its own type (E5's type fence), then OPERA-MS.
  - `assemblers/isolate`: the isolate assemblers.
- Everything downstream stays in shared folders and takes any `sequences::assembly`, so every mode
  reaches it. Template targets hang off the generic assembly, which fork-cases also needs.
- Read-class settings (MetaBAT2 `--percentIdentity 80` and SemiBin2 long-read mode for long reads,
  Flye mode from the platform) live inside one transform. It reads the read class from lineage at run time.
- Hybrid and isolate templates go to later-templates. The isolate template has no binning targets.

Landed: the build of engcyc metagenomics (assemblers/metagenome). Refines library-shape: assemblers
are grouped by mode rather than parked in `alternatives/`.

## mp-port-check (2026-11-21)

Question: did the MetaPathways port succeed, and does it ship in engcyc metagenomics?

Evidence: `research/beth_metapathways/comparison.md` (15 CPE3 assemblies against the MP4 results in
`data/beth_mp/mp4`). Genes 98.3-99.8% identical. The port names more genes and assigns more ECs.
Pathway overlap is only 0.52-0.57 (community) and 0.46-0.60 (per MAG). The report blames the
Pathway Tools input: MP4 gives product names plus ECs, the port gives CLEAN ECs only. Untested.

Decision: fix and recheck. The port's Pathway Tools input carries product names as well as ECs.
PathoLogic reruns on 2-3 of the 15 assemblies on fir, and the comparison runs again. The MP lane
ships in the template once pathway overlap rises. The rest of the template does not wait on it.

Landed: worker mp-fix (see STACK.md Workers).

## template-targets: scope (2026-11-21)

Decision: engcyc metagenomics is the full template. Every lane is a target the user can deselect in
the GUI. The docs and the template description list each lane's reference download size. Run 4
runs a stated subset.

## template-targets / binning (2026-11-21)

Question: how do the three binners become the quality MAGs that taxonomy, MetaPathways per MAG and GEM consume?

Decision: MetaBAT2 + SemiBin2 + COMEBin -> DAS Tool -> CheckM2 -> quality MAGs at >= 50% complete
and <= 10% contamination. No within-sample skani (DAS Tool removes the overlap). Cross-sample
dereplication waits for a study-level template.

Build notes: promote CheckM2, DAS Tool (drops a binner with an empty table) and COMEBin (empty table
on a tiny assembly) from the benchmark folders. Fold E5's long-read settings into the standard
MetaBAT2 (`--percentIdentity 80`) and SemiBin2 (`--sequencing-type long_read`), keyed on the read class.
CheckM1 becomes a rival.

## template-targets / taxonomy (2026-11-21)

Question: which taxonomy targets go into engcyc metagenomics?

Decision:
- GTDB-Tk on the quality MAGs (Beth's `gtdbtk_quality`, GTDB r232).
- rRNA scan on the assembly (barrnap + BLAST against GTDB rRNA). It replaces phyloFlash and works
  for long reads.
- Metabuli and LCA* are left out of the template. Tony: "Metabuli requires a large reference and may
  not be practical... check". The pinned GTDB r232 Metabuli database is 634 GB packed and 1.4 TB at
  the unpack peak, over nibi's 1 TB scratch quota. Both stay in the library for users with the disk.
  LCA* needs Metabuli's votes, so it goes with it.

## template-targets / annotation (2026-11-21)

Question: which annotation lanes go into engcyc metagenomics?

Decision:
- Gene function on the assembly's ORFs: DIAMOND UniRef50, KOfamScan, InterProScan, eggNOG.
- 4-lane GPR table. Tony: "The 4 lane is crucial, we can drop the 7 lanes". The 7-lane GPR becomes a rival.
- Genome features: Bakta non-coding, ARAGORN tRNA, antiSMASH, PathoFact (AMR, virulence, toxins,
  MGEs), mobileOG.
- Metabolic models run per MAG only. Tony: "These are just models at different aggregate levels,
  which i think should be per mag for now. MP on community is dubious if just modelling everything as
  an open soup". So MetaPathways runs per quality MAG. Community PathoLogic leaves the template.
- DRAM is ported into its parts, as MetaPathways was: its component searches become our transforms,
  and its distill summary is reimplemented over them. Tony: "Lets try for all annotation lanes", so
  the other wrapper pipelines (PathoFact, DRAM-v) get the same treatment. Tracked as stack topic
  wrapper-ports.

## template-targets / gem (2026-11-21)

Question: how is the GEM lane built per quality MAG?

Decision: E5's version. Tony: "But no rivals. 1 way to make the carveme gem".
- MAG ORFs are the subset of the assembly ORFs on the MAG's contigs (E5 `mag_orfs`), so a MAG's
  genes keep the IDs of their contig-level annotations. MetaPathways per MAG uses the same ORFs.
- CarveMe 1.6.6 in two steps: DIAMOND against BiGG, then carve and gapfill on SCIP with a named medium.
- MEMOTE scores the model, with E5's `HOME=$PWD` fix.
- The one-step CarveMe 1.6.1 transforms are deleted, not parked in `alternatives/`. There is one
  way to make a CarveMe GEM.

## template-targets / viral (2026-11-21)

Question: which viral callers and extras go into engcyc metagenomics?

Decision:
- Callers: all four (Tony: "Everything"). geNomad, VirSorter2, VIBRANT, DeepVirFinder, with E5's
  settings. Then E5's chain: merge calls, CheckV, Pratama curation and provirus trim, mmseqs vOTUs
  (95% identity, 80% coverage), representatives >= 5 kb with the island filter. vOTUs cluster within
  the sample.
- Extras: DRAM-v (through the DRAM port), CCTyper, vConTACT3, and iPHoP. Tony: "Iphop, but using mags
  as host library". `iphop_add_to_db` grafts the sample's quality MAGs onto the iPHoP base database,
  with GTDB-Tk de novo trees (`gtdbtk_de_novo_quality`), and `iphop_predict` runs against that.

Build notes: `iphop_add_to_db` requires `viromics::contig_study` and a skani `cluster_table`. Binning
decided on no within-sample skani, so the build either feeds it the DAS Tool set or relaxes that
requirement. The iPHoP base database is large; the docs state its size.

## template-targets (2026-11-21)

Closed with its children: scope, binning, taxonomy, annotation, gem, viral. Read classes are in
read-classes. The target list is these decisions together. The template description names each lane.

## wrapper-ports (2026-11-21)

Question: does engcyc metagenomics wait for the DRAM, DRAM-v and PathoFact ports?

Decision: wait for all ports. Each port lands with a comparison against the original tool's output.

Why port: Tony: "Dram import lets us deduplicate and standardize dram outputs with ours and also
match with our precedent format for annotaions that could potentially feed in as lanes to a gpr
table". A ported search that we already run (KOfam, UniRef) is run once. Each part emits the
standard per-gene annotation format, so it can feed the GPR table as a lane.

## gem-reopen (2026-11-21)

Question: CarveMe annotates on its own. Does it stay the one way to build the per-MAG GEM?

Tony: "For the gpr table, we prolly want an aggregate set of all, which can then be subsetted by
consumers to fit needs, like 4lane, or carveMe's single lane. We want to do the annotation outside of
carveMe, rather than replciate".

Decision:
- CarveMe stays the one way to build a GEM (gem decision holds).
- CarveMe does no annotation of its own. Its DIAMOND search against BiGG becomes an annotation lane
  run outside it (E5's `carveme_annotate_bigg` split already does this), and carve + gapfill reads that lane.
- One aggregate annotation table holds every lane. Each consumer takes the subset it needs: ECSPr
  the 4 lanes, CarveMe the BiGG lane. Design of the table: stack topic annotation-aggregate.
- Supersedes the 4-lane GPR target in the annotation decision: the target is the aggregate table.

## isolate-hifi (2026-11-21)

Question: how does HiFi isolate assembly stay out of the metagenome templates and still reach the
downstream tools? Can a hybrid assembly target force the hybrid lane inside one template?

Tony: "we have 3 folders for assembly. One of them is isolate. We load it separately." "Hifiasm meta
as the canonical isolate assembler, i accept other recommendations".

Decision:
- `assemblers/isolate` holds hifiasm_meta as the one isolate assembler. Its product counts as
  `sequences::isolate_assembly` (an assembly and a putative genome), so every downstream lane takes it.
  Only the isolate template loads the folder.
- hifiasm, filtlong_targeted (it fed only hifiasm) and miniasm are deleted. No rivals.
- pbBam2fastq moves to a shared folder: it converts input, it does not assemble.
- Hybrid stays a separate folder loaded by a hybrid template, not a target inside engcyc metagenomics.
  A hybrid sample holds short reads, so MEGAHIT from the metagenome folder would also make an
  assembly, and a target or `parents=` pin cannot pick between two producers of one generic type
  (memories two-producers-need-a-masked-library, consumed-assembly-steals-its-lanes-slot). Reasoned,
  not solve-tested; Tony accepted the recommendation.
- Callout: hifiasm_meta is HiFi-only, so an ONT isolate has no assembler in the isolate folder.

## hybrid-assembly / two-assemblers + hybrid-input (2026-11-21)

Question: how do hybrid samples reach OPERA-MS when the same library holds MEGAHIT, and how does a
hybrid sample come in from SRA?

Evidence: hybrid-solve worker (feat/engine/hybrid-solve 1ad9ba60, research/hybrid_solve/). A generic
assembly target sends hybrid samples to MEGAHIT and ignores the long reads. An explicit OPERA-MS
target sends them to OPERA-MS, and downstream uses it. One SRA run is one library on one platform,
so a hybrid sample is always two runs.

Tony: "it really needs to be known and choosen ahead of time." "you need to specify a hybrid assembly target."

Decision:
- Hybrid is chosen ahead of time. A hybrid sample gives a short read set and a long read set (or a
  short-run and a long-run accession, each fetched with its class known, no fork), and the user asks
  for an explicit hybrid assembly target. Everything downstream hangs off it as a generic assembly.
- getSraReads keeps its three outcomes (long, paired, single). There is no hybrid fork outcome.
- The hybrid lane (MEGAHIT draft under its own type, then OPERA-MS) joins the standard library.
  Revises read-classes: hybrid is not a separate folder or template.
- Open: a session mixing short and hybrid samples needs the hybrid target on the hybrid samples only
  (per-shape targets; the planner already takes a target per case).

## read-types (2026-11-21)

Question: can the read types be just short and long, with the details in read_metadata?

Decision: yes.
- getSraReads has two outcomes: `sequences::short_reads` and `sequences::long_reads`.
- Parity (paired, single) and platform (ONT, CLR, HiFi) live only in read_metadata. BBDuk, MEGAHIT,
  assembly_stats and Flye already read them at run time.
- Delete `short_reads_pe`, `short_reads_se`, `clean_short_reads_pe`, `clean_short_reads_se` from the
  standard library. Retype `ora2fastq` and the givens of fosmid_inserts_from_pooled_reads and
  ecspr_survey_from_pooled_reads to `short_reads`.
- Research libraries keep their types, so old benchmark caches stay valid.
- Effect on read-classes: the template has two SRA outcomes to route, not three.

## mixed-session (2026-11-21)

Question: can short and hybrid samples share one session?

Finding: the planner already solves a target per case, but `Spec` builds one target set and
`Case.ByShape` copies it to every shape, so the API and the GUI never reach it.

Tony: "lets try to support it now. can be awkward, but as long as it works, we can refine as we go"

Decision:
- Thread per-shape targets through `Spec` and the GUI now, before run 4.
- First cut: a target may name the given types it needs (for example the hybrid assembly target
  needs `sequences::long_reads`). Each shape gets only the targets its givens satisfy. A target
  with no condition goes to every shape.
- Effect on mixed-study-check: a short-only sample no longer gets the hybrid target, so the false
  OPERA-MS "success" goes away for split studies. A study that stays one case still needs the check.

Landed: worker shape-targets on feat/engine/shape-targets.

## catalog-survey (2026-11-21)

Question: in what order do the analyses outside the template come in, and what does the template
take from cyanoverse and the Spanish lakes/rivers work?

Decision:
- Now: the engcyc template and run 4, with the E5 variants absorbed. Cyanoverse taxonomy is already
  in (GTDB-Tk on quality MAGs + rRNA scan).
- Spanish lakes/rivers resistome lanes join the template, each deselectable: RGI/CARD,
  AMRFinderPlus, MEGARes, BacMet, VFDB, IntegronFinder, MobileElementFinder, inStrain,
  MetaPhlAn/StrainPhlAn (beside PathoFact and mobileOG, already in).
- FEAST is dropped (Tony: "ok, drop FEAST"). Its source table is Antonio's hand compilation with no
  public download, and `feast.py` never reads the sample.
- After run 4: the mature gaps one at a time (port-gaps).
- Exploratory work stays in its projects. MS ion ID (compound-ID ensemble, pilot run succeeded) and
  MS ordination (figures tuned, https://claude.ai/artifact/6EbejKytP9ZMrtv5Bt1n8V) move to the
  after-run-4 port list (Tony, 2026-11-21: "fold them in with category 2, after the run").

Landed: this file; port order in STACK.md.

## annotation-aggregate (2026-11-21)

Question: one aggregate table of every annotation lane, or two levels?

Tony: "the 2 level tables work for me"

Decision:
- Level 1, annotation table: one row per ORF x lane x term, every lane (reaction-mapping or not:
  Pfam/InterPro, CAZy, PathoFact, AMR, mobileOG, DRAM-port lanes, ...). Consumers subset it.
- Level 2, GPR table: built from the annotation table, the lanes whose terms map to reactions, with
  per-lane scores. It holds every reaction-mapping lane. The 4-lane GPR and CarveMe's BiGG lane are
  subsets of it. Supersedes the 4-lane GPR as the target (gem-reopen).

Landed: worker annotation-aggregate on feat/engine/annotation-aggregate.

## wrapper-build (2026-11-21)

Question: what does "importing" DRAM, DRAM-v and PathoFact mean?

Tony: "looks good to me" (DRAM), "yes, that all looks good" (PathoFact).

Decision:
- DRAM: run only the search lanes we lack (dbCAN/CAZy, MEROPS, VOGDB) as annotation-table lanes.
  Reuse KOfam, InterProScan Pfam, and UniRef50 in place of UniRef90. Run DRAM distill over the
  annotation table.
- DRAM-v: the same pattern over the viral lanes.
- PathoFact: reuse RGI and the viral callers (VirSorter2, DeepVirFinder). Add only its unique parts
  as lanes: toxin HMMs + SignalP, the virulence-factor classifier, DeepARG, PlasFlow/MOB-suite. Its
  gene-on-mobile-element join runs over the annotation table and the contig calls.
- Each port gets a comparison against the upstream tool's own output (as the MetaPathways port).

Landed: workers start once annotation-aggregate lands (they emit into its format).

## localization-lanes (2026-11-21)

Question: since SignalP comes in with PathoFact, which adjacent tools come too? (Prior work:
GORG-Dark proteorhodopsin signal peptides, where the first TM helix is easy to miscall as an SP.)

Tony: "yes"

Decision: add DeepTMHMM (TM topology + SP) and PSORTb 3 (bacterial localization) as annotation lanes.
PSORTb ships openly. DeepTMHMM and SignalP 6 fall under licensed-tools.

## licensed-tools (2026-11-21)

Question: how do lanes that need a licensed tool (PathoLogic/Pathway Tools, SignalP 6, DeepTMHMM,
DeepLocPro, Phobius) work for a user who must download the tool themselves?

Tony: "ok, can we also add a step to build the image? so that users can create from downloading
themselves? for our lab, can you create local sifs?"

Decision:
- The user's download is an input, like a reference DB (`licensed::<tool>_package`).
- A build step turns that package into the tool's image, once, and the cache keeps it. The lane runs
  on the built image. Nothing licensed goes into a published image.
- No package given: the lane is unreachable. Licensed lanes are off by default. Selecting one
  without its package fails with an error that says where to get it.
- For the lab: build local SIFs from the lab's own downloads (Pathway Tools installer already on
  fir; the DTU tools need Tony's licence forms).

Landed: worker licensed-build on feat/engine/licensed-build.

## mp-port-ship (2026-11-22)

Question: ship the PathoLogic lane at 0.62-0.66 per-MAG overlap with MetaPathways 4, or pull more
levers first?

Tony: "we just want to replicate the capabilities of metapathways... so i guess pull the lever" and
"yes, lets try to be faithful to metapathways".

Decision:
- Feed PathoLogic what MP4 feeds it: UniRef product names, MetaCyc protein search hits with their
  reactions (METACYC lines), and Swiss-Prot ECs. CLEAN comes out of the PathoLogic input (it stays
  an annotation-table lane).
- Also as MP4: search UniRef90 beside UniRef50, pin and record the UniProt release, skip proteins
  under 60 aa for the PathoLogic input only (the annotation table keeps every protein).
- Tony (same day): "we want the annotation lanes to match the existing (our) pattern, but when
  pathologic consumes, its transform then does the conversion into 'metapathways style' like
  filtering out proteins". The new searches (MetaCyc proteins, Swiss-Prot, UniRef90) are ordinary
  annotation-table lanes with no MP filtering. The PathoLogic transform reads the annotation table
  and applies every MP4 rule itself. The same holds for any port: lanes in our format, the
  consumer converts.
- Recheck on the three CPE3 assemblies on fir; ship at the overlap it reaches, documented in
  comparison.md.
- MP4 outputs not yet rebuilt (function hierarchy tables, per-ORF function+taxonomy table,
  product-name merge) are a separate topic: mp-outputs.

Landed: worker mp-faithful on feat/engine/mp-faithful (from feat/engine/mp-fix).

## mp-outputs (2026-11-22)

Question: which MetaPathways outputs does the port rebuild?

Tony: "we dont care about the other metapathways outputs, but we should generate some usable output
for pathologic" ... "can we replace with extracted pathway table from pgdb? ... what does MP4 do?"
... "ok".

Decision:
- Do not rebuild MP4's hierarchy count tables, per-ORF function+taxonomy table or product-name merge.
- Replace `pathway_table` and `bin_pathway_table` with MP4's per-PGDB pathway table: SAMPLE,
  PWY_NAME, PWY_COMMON_NAME, PWY_SCORE, NUM_REACTIONS, NUM_COVERED_REACTIONS, ORF_COUNT, ORFS, plus
  Pathway Frequency Score, Pathway Abundance and Reason to Keep from the pathways report. One row
  per pathway, per MAG and for the community. The PGDB archive stays.

Landed: added to worker mp-faithful (feat/engine/mp-faithful).

## replace-old (2026-11-22)

Question: how do the worker branches come together, and when does the engcyc template replace
metagenomics_from_paired_reads?

Tony: "ok"

Decision:
- Merge every finished worker branch into feat/engine/rc now and settle conflicts while small.
- Once PathoFact, mp-faithful and lab-sifs land, one worker builds the engcyc metagenomics
  template on the merged tree with every decided lane, each deselectable, and retires
  metagenomics_from_paired_reads in the same commit, goldens regenerated
  (MSM_REGEN_TEMPLATE_GOLDEN=1).

Landed: merges cfe197cd (mp-fix), 60408efe (hybrid-solve), 949c87ca (shape-targets; solve.py
took shape-targets' copy), fd066d45 (licensed-build), 8a944d44 (dramv-port, carrying dram-port and
annotation-aggregate). Metadata compiles; test_spec, test_cases, test_licensed_lanes pass;
test_gpr_schema 20/21 (the failure reads untracked fabfos run tables on the old layout, not code).

## pathofact-version — 2026-11-22

Question: the PathoFact port follows PathoFact 2.0 (mobile elements from geNomad), not v1 (PlasFlow, MOB-suite, VirSorter, DeepVirFinder). Keep it?

Tony: "accept rec 2.0"

Design: port PathoFact 2.0 at commit bd5d527. Its MGE arm reuses our geNomad lane, and it adds no v1 MGE tools. It uses SignalP 6, which the licensed lane provides.

Landed: feat/engine/pathofact-port fed84f3d, merged into feat/engine/rc as 875a1f6e.

## release-order — 2026-11-22

Question: when does our work join the geoquery 0.26.0 release?

Tony: "they can go first then at b (which should allow us to apply any fixes taht we have already decided on, but it just wont be tested) / we just need to make sure we dont regress what they do after c"

Design: geoquery releases 0.26.0 alone. After the template build, merge the released feat/dev into feat/engine/rc and carry the decided fixes, untested. After run 4 is clean, release from rc. Before that release, check that geoquery_from_aoi_table still solves and its tests pass.

Landed: message sent to geospatial-query-dfs (2026-11-22). Release-0.26 entry in STACK.md.

## lane-deselect — 2026-11-22

Question: the annotation table requires every lane (a requirement cannot be optional), so any
consumer of it runs all lanes. How do lanes become deselectable?

Tony: "option a, but we do lanes -> consuming tool, rather having lanes -> per consumer aggregate -> tool"

Design:
- Each consuming tool (PathoLogic gather, PathoFact reports, DRAM and DRAM-v distill, the GPR
  tables, CarveMe's BiGG input) requires only the lane products it reads. It builds the
  standard-format rows in its own step with the shared annotation-table code. No per-consumer
  aggregate type sits between the lanes and the tool.
- The 4-lane GPR is one such consuming tool (Tony: "in this model, the 4-lane gpr is a
  \"consuming tool\"").
- The every-lane annotation table stays as an optional consuming tool (Tony: "we can also have a
  full 20 lane aggregate transform as an optional \"consuming tool\"").
- Each lane outputs its rows in the standard annotation-table schema as its own product (Tony:
  "as long as the lanes themselves output some standard format..."). A standardizing step after the
  lane's search keeps cached raw results valid. Consumers concatenate or filter lane tables and parse
  no raw tool output.
- The standard schema is a minimum. Lanes add their useful native fields as extra columns (Tony:
  "the standard is a minimum, but lanes can give more columns if they are useful, and I think many
  should. kofam should have the dynamic threshold and score columns, for example").
- A lane whose prediction is a motif or region within the protein also outputs a hits table in one
  shared format: one row per hit (ORF, lane, term, start, end, score, extras). This covers
  InterProScan, the HMM lanes, SignalP, DeepTMHMM and Phobius. Whole-sequence predictions get none.
  Tony: "yes, that is sensible.. only when the predicted features are a motif within the protein
  sequence rather than of the sequence itself".
- Deselecting a lane deselects only the tools that read it.

Landed: template-build worker on feat/engine/template-build.

## deeplocpro-licensed — 2026-11-22

Question: DeepLocPro is under licence. How does its lane work?

Tony: "DeepLocPro is under license..." / "but it is installable from git? that is simpler..."

Design: a licensed lane as in licensed-tools (off by default, nothing published), but its build step
installs from the public GitHub repository (docker/deeplocpro, pinned commit) and needs no user
download. The image is built and cached on the user's side. The lab copy is the GitHub build in
~/licensed-staging/sif/. The quay image pushed by mistake stays private and is to be deleted.

Landed: stack item deeplocpro-licensed (template-build worker told).

## value-identity — 2026-11-22

Question: the leaf-identity fix (fdd1630c) gives a GUI value row a new ID when its text changes,
reversing the July rule "identity here is provenance, not bytes". Keep it, and how is the ID formed?

Tony: "for the values, we should just calculate the ID by hashing the value itself" and, on hashing
it with its row name so two rows with equal text stay separate: "Okay, that sounds good."

Design: a value item's ID is a hash of its value and its minted path. An edit gives a new ID.
Undoing the edit restores the old ID and its cache. Equal text in two rows gives two IDs. File,
folder and pool leaves keep the stamp rule from fdd1630c (path + mtime, with a content check for
small files).

Landed: 948f6929 on feat/engine/dev-bugs, merged into rc as 5d03cd86. Values minted before it keep
their old ID until first edited, so existing caches survive.

## template-names — 2026-11-22

Tony: "Let's just call the template engcyc_metagenomics." / "maybe we run a download version first
to set up databases? engcyc_prep_references template?"

Design: the template is `engcyc_metagenomics`. A second template, `engcyc_prep_references`, holds
only its reference downloads and builds, with no samples, so a user sets up the databases once
before a run. Both share one target source, and the references have the same identity in both, so
the main run reuses them from cache. Run 4 tests the full engcyc_metagenomics, preceded by prep.

Landed: 8827fddb (rename), 9bef7c5b (prep: 29 targets, 40 steps, ~680 GB, every step identical to the
main plan's), merged into rc as 891e4c34.

## metabuli-size — 2026-11-22

Question: Metabuli's prebuilt gtdb232 database is 744 GB (every species representative, MAGs
included). Cyanoverse used the R214.1 database, 101 GB (HQ complete/chromosome genomes only). How
does Metabuli join engcyc?

Tony: "I don't believe it's real" (the jump is the selection, not GTDB growth); then "Yes, and we can
run downloads on fir compute."

Design: build our own r232 database with the R214.1 rule (Complete Genome/Chromosome, CheckM
>90%/<5%, plus human T2T) from GTDB's bulk species-representative archive and GTDB r232 taxonomy,
so it matches GTDB-Tk r232. Build once on fir and host it. The library step downloads the hosted
database. The GTDB-Tk package holds only skani sketches, not genomes, so it cannot be the source.

Landed: worker metabuli-r232 (build and measure). Hosting site still open.

## solver-investigation (2026-11-22)

Question: why engcyc_metagenomics needed 255/256 iterations for short reads and failed for hybrid.

Tony: "If the solver should work, something is going wrong. Can we investigate what is going wrong?" / "run targetted tests on the Fable fixes. if looks good, lets accept it"

Decided design: three solver fixes, not a template split.
- The binding DFS forward-checks anchored requirements.
- The target transform mints one application, binding the oldest candidates; the refiner is unaffected.
- Applications fed by a dead anchor are demoted through their own prior channel. A dead anchor is a product type that can never satisfy a target's anchored slots, such as MEGAHIT's assembly in a hybrid case. SOLVER_RNG_VERSION is 4; fingerprints did not move.
- The default search budget is 512, because hybrid needs 313 iterations and a failed solve ends in under a second.

Result: short 172 steps in 2.4 s, down from 80 s and 5.7 GB; hybrid 173 steps; isolate unchanged.

Landed: feat/engine/solver-fixes (a0e6507a, 7e444744, 8e3ae62c, cd485ff0), merged as 1e3f3ae7; budget change 911a5e4a.

## read-taxonomy-ensemble (2026-11-23) — DISCARDED

Tony: "disregard aggregators. they prevent optional outputs, discard idea". Replaced by split SRA accessions plus long-read taxonomy lanes (sra-split). The long-read sources below still decide which tools get long-read lanes.


Question: how should a template reach read-level taxonomy when a sample may carry short or long reads, as an SRA accession's fetch can return either?

Tony: "short reads should reallly be SR -> read level taxonomy tools -> read-level taxonomy aggregator that asks for the ensemble. then we need a similar one for long reads." / "That's why I proposed a separate read-level taxonomy aggregator. This way we can have separate aggregators for short read and long reads that target the specific set of tools. Then when EngSec does the target, it just asks for read-level taxonomy."

Decided design:
- A read-level taxonomy supertype is the target. Templates such as engcyc target only that type, not the individual tools.
- Two aggregators produce it, one per read class. Each requires its own tool set: short reads -> short-read tools, long reads -> long-read-capable tools with their long-read settings.
- Short-read-only tools (inStrain, StrainPhlAn) sit under the short-read aggregator only. A long-read outcome reaches the target through its own aggregator, so the getSraReads fork routes every outcome, and the SRA accession solves without typed accessions.

Membership. Tony: "If they're documented to support long reads, go ahead. If not, then ... try and see. ... find some online source or literature citation" / "Maybe we should take the intersection rather than the union. Like that way we can also have the exact targets be optional so that we can drop tools." Metabuli is assembly-level here, so it is not in the ensemble.

- Both aggregators require the same tools: the intersection of short- and long-read-capable read-level tools, each run in its read class's mode.
  - MetaPhlAn 4.2 `--long_reads` (minimap2) and StrainPhlAn 4.2 (detects minimap2 alignments): MetaPhlAn 4.2.1 and 4.2.2 release notes, zenodo 15591132 and 15594111.
  - sylph: sylph-docs "can/cannot", nanopore and PacBio.
  - Kraken2: Portik et al. 2022 (PMC9749362) benchmarked it on HiFi and ONT. It works, but short-read methods there needed heavy filtering for false positives.
  - ganon2: release notes, "better support for long reads".
- Each tool's own output is an optional target, so a user can drop a tool.
- Outside the intersection:
  - Centrifuger: no long-read documentation found; its parent Centrifuge was benchmarked in Portik 2022. Trial it later.
  - inStrain: the maintainer (inStrain issue #104) says HiFi (~Q30) works and error-prone long reads inflate SNV calls.

Where it landed: pending (worker).

## plan-cases (2026-11-23)

Question: how does one template plan short reads and long reads with different target sets?

Tony: "first we just have 2 normal solves from given and targets. these are short reads to all relavent engcyc targets, then another solve from long reads to a similar but modified set of targets. the solver should be handed these 2 together and it should merge steps when appropriate. This is the base case. this also lends itself well to tabs when dealing with grouped solves" / "template doesnt hold a set of named solves. it holds a list of specs that read like individual solve requests, but is solved once" / "inputs are given per solver spec/case ... This allows runs where each input can be funnelled into a specific case". Naming agreed: an entry is a **case**, the action is **plan**.

Decided design:
- A case is one classical request: its own inputs plus its own targets. `Plan([case, ...])` hands every case to the solver once and returns one merged plan (the engine's existing `WorkflowPlan.Generate(cases=...)` and case_merge). "Solve" stays an engine word.
- A template holds a list of cases. The GUI shows one tab per case, each with its own samples and targets.
- Inputs are given per case. The target `when` condition and the per-shape target callable are removed (they reversed this: one target list split by shape).
- AsSamples is fixed so a sample is its own index item, its descendants and its ancestors, not its siblings (multi-sample-collapse). Needed so several samples inside one case fan out.
- engcyc becomes two cases: short reads to the engcyc targets; long reads to a modified set with the long-read taxonomy lanes (sra-split).
- Known limit: each case picks its tools independently; steps merge only where the picks agree.

Supersedes the `when` design from shape-targets (5155d9ce).

Where it landed: worker plan-cases (feat/engine/plan-cases).

## sra-split (2026-11-23)

Question: how does an SRA accession plan when its fetch can return short or long reads?

Tony: "just splitting the long and short read accession is the simplest move here" / "add in the long read level taxonomy tools that are known to be supported" / "disregard aggregators. they prevent optional outputs, discard idea".

Decided design:
- Two accession types, short-read and long-read, each with its own fetch transform. getSraReads' three-group fork and the short_reads_pe/se types go.
- Long-read read-level taxonomy lanes for documented tools only: MetaPhlAn 4.2 (--long_reads), StrainPhlAn 4.2, sylph, Kraken2, ganon2 (sources in read-taxonomy-ensemble). Not Metabuli, Centrifuger or inStrain.
- Each lane is an optional target of engcyc's long-read case (plan-cases).

Where it landed: worker sra-split (feat/engine/sra-split); engcyc's two cases after plan-cases lands.
