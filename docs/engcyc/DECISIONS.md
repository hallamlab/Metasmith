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
