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
