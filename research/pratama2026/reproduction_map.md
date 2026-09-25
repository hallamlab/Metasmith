# Pratama et al. 2026 vs. the library, step by step

CAUTION: this map predates the pinned `e3` transform library. For what E3's plan runs today, read
`research/metasmith_benchmark/findings/E3_PARITY.md`.

Authority order per `data/docs/pratama2026/SOURCES.txt` and `.awm/context.md`: the
authors' own workflow repository (`data/docs/pratama2026/Groundwater_virome/Workflows/`)
over the Methods prose (`PMC12960796.xml` / `s41467-026-68914-2.pdf`) whenever the two
disagree. Both are read here; disagreements are recorded, not resolved.

Modelled on `research/viromics/pipeline_steps.yml`. Same status vocabulary:

    available   a transform runs the row's own tool
    substitute  a transform reaches the same contract by a different tool, or a
                different parameterisation of the same tool, or part of the output
    mock        a transform declares the row's inputs/products for real and its
                protocol touches its outputs but has not been run against real data
    dropped     deliberately not planned; the note says what answers it instead
                (usually a join/predicate over a table this library already writes)
    missing     nothing in the library produces it and nothing is planned
    conflict    two things this campaign wants cannot both hold; not a status this
                library can resolve by picking a transform

`run 2` below means `research/cami/run_cami_metag.py build_targets_pratama()` after
this session's revision. Its default plan is 48 steps (60 runs, key `1jRmANwM`);
`--with-zenodo-comparison` adds T17's two skani-dist tables for 49; `--with-gpr-panel`
adds the chosen-4 functional panel for 54; both together, 57. All four solved cleanly
with `--dry-run` (no dropped targets).

## A. `MetaG_and_MAGs_bioinformatics.md`

| # | step | Pratama's command | run 2 | note |
|---|------|--------------------|-------|------|
| A1 | QC | `bbduk.sh -Xmx1g ktrim=r qtrim=rl trimq=20 minlen=50 k=23 mink=11 hdist=1` | substitute — `assembly/bbduk.py` | Same tool, different settings: our `qtrim=r trimq=0` (not `rl`/`20`) plus `tpe tbo maxns=4 usejni=t maq=3`, and `minlen` is derived from the sample's own N50 (capped 20-51) rather than fixed at 50. No BBMap version stated in the repo to check against the paper's "BBMap version 39.01" (our `bbtools.env` pins 39.49). |
| A2 | QC reporting | `fastp -R -j -h` (report only, not a filter) | substitute — `assembly/seqkit_reads.py` | Same role as Antonio's row 1 (FastQC): read statistics, not fastp's own report format. Nothing downstream reads either. |
| A3 | assembly | `spades.py --meta -k 21,33,55,77 -m 190`, no error correction | **conflict** — `assembly/spades.py` | See "The assembly conflict" below. Batch 1 keeps the DOE JGI protocol (bbcms correction, `--only-assembler -k 33,55,77,99,127`, seqkit 200 bp filter) for the citable-core framing; Pratama's own parameters are not run. |
| A4 | assembly (viral recovery only) | `megahit -1 -2 -t` (module `MEGAHIT/1.2.9-Python-2.7.18`) | missing | `_assembly_without("megahit")` masks this out for run 2's whole target set, not just the MAG lane. Pratama ran it *in addition to* metaSPAdes specifically so the viral caller lane sees contigs from both assemblers; see "The MEGAHIT gap" below. |
| A5 | binning (40 markers) | `metawrap binning --maxbin2 --metabat2 --concoct` | available — `metagenomics/binning/metawrap.py` | Exact match: same three binners, same flags. |
| A6 | binning (107 markers) | `metawrap binning --maxbin2` alone, second pass | missing | Not run; no transform emits a second, differently-parameterised MaxBin2 pass. |
| A7 | binning | BinSanity (`Binsanity-profile` + `Binsanity-wf`) | missing | No transform, no container anywhere in `src/metasmith_libraries`. |
| A8 | binning | abawaca (ESOM prep + `abawaca` binary) | missing | No transform, no container. |
| A9 | refinement round 1 | `metawrap bin_refinement -c 50 -x 10 -A abawaca -B abawaca -C binsanity` | missing | Depends on A7/A8's missing outputs. |
| A10 | refinement round 2 (final) | `metawrap bin_refinement -c 50 -x 10 -A metabat2 -B concoct -C <round-1 output>` | substitute — `metagenomics/binning/metawrap.py` | One refinement pass over metabat2+maxbin2+concoct directly, not two. The transform's own header says so: "the paper additionally runs two more binners and a dereplication step... none of that is here; it is the next increment." Same `-c 50 -x 10` thresholds. |
| A11 | dereplication | `dRep dereplicate -pa 0.90 -sa 0.99 -comp 50 -con 10` | **missing for this lane** — see "The dereplication gap" below | `skani_dedup.py` exists and is the library's dRep substitute, but it is wired only to `binning_local::quality_bin_fasta` (the aggregator's metabat2/SemiBin2/COMEBin output), never to `sequences::metawrap_bin_fasta`. Run 2's own MetaWRAP MAGs are never dereplicated at all. |
| A12 | MAG read-mapping | `coverm genome -m trimmed_mean --min-read-percent-identity 0.95 --min-read-aligned-percent 0.75 --min-covered-fraction 0.25` | dropped, substitute — `assembly/assembly_stats.py`'s per-contig coverage, joined | Same arithmetic one level up (bin depth from contig depth), computed against each sample's own assembly rather than cross-mapped, so a MAG absent from a sample's own assembly contributes zero instead of being caught by that sample's reads. |
| A13 | taxonomy | `gtdbtk classify_wf` (module `GTDB-Tk/1.4.1`) against GTDB release 202 | available, with a reference-release caveat — `metagenomics/taxonomy/gtdbtk.py` | Tool matches; the reference does not. `logistics/downloadGtdbDB.py` pins release232, thirty releases newer than the paper's r202 — GTDB renames and re-splits lineages between releases (see A15 below), so classifications will differ from Pratama's for reasons of database vintage, independent of the tool. |
| A14 | MAG annotation | `DRAM.py annotate -i MAG_INPUT.fasta --min_contig_size 1000` then `DRAM.py distill` (module `DRAM/1.3.6`) | substitute — `functionalAnnotation/dram_annotate_genes.py` + `merge_dram_annotate_genes.py` | Annotates the whole assembly's ORF chunks, not each dereplicated MAG fasta individually, and there is no distill step and no per-MAG rollup. Same caveat already on record for Antonio's own pipeline (row 40). |
| A15 | long-read processing | `guppy_basecaller` then hybrid `spades.py --nanopore` | out of scope, by design | `enumerate_pratama_runs()` filters the 6 SINGLE (MinION) runs out before samples are ever registered. This is a scope decision already made by the driver, not a library gap -- 66 of 72 runs are short-read paired anyway. |

## B. `Virus_bioinformatics.md`

| # | step | Pratama's command | run 2 | note |
|---|------|--------------------|-------|------|
| B1 | virus ID | DeepVirFinder `dvf.py -l 1000 -c` | missing | No transform, no container. One of the four identification callers the paper actually ran; entirely absent here. |
| B2 | virus ID | VIBRANT `VIBRANT_run.py -f nucl -virome -d DB` | substitute (parameter) — `viromics/vibrant.py` | Runs VIBRANT, but without `-virome`. That flag raises VIBRANT's sensitivity for exactly this kind of low-complexity, high-virus-fraction metagenome; omitting it is a real parameter gap, not a version pin. |
| B3 | virus ID | geNomad `end-to-end --cleanup --splits 48 --min-virus-marker-enrichment 1 --min-virus-hallmarks 1` | substitute (parameter) — `metagenomics/taxonomy/genomad.py` | Runs `genomad end-to-end ... --cleanup` with neither sensitivity flag. Both flags lower geNomad's detection thresholds; without them our geNomad calls are stricter (fewer, more conservative candidates) than Pratama's. Also: the repo states no geNomad tool version at all, but our `genomad.env` pins 1.11.0 against the paper's stated 1.5.1 -- a library-vs-paper gap the repo itself is silent on. |
| B4 | virus ID | VirSorter2 `--include-groups dsDNAphage,ssDNA --keep-original-seq --min-score 0.5 --min-length 5000` | substitute (parameter) — `functionalAnnotation/virsorter2.py` | `--min-score`/`--min-length`/`--keep-original-seq` match. `--include-groups` does not: ours is `dsDNAphage,NCLDV,RNA,ssDNA,lavidaviridae`, a superset that will call viruses (NCLDV, RNA phages, lavidaviridae) Pratama's own run structurally could not. |
| B5 | curation | manual filter on caller output + gene content + host-region evidence (paper's Methods, not a single command) | dropped, note | Same shape as Antonio's rows 8/12: a predicate over tables this library already writes (`viromics::contig_length_table`, each caller's gene/interval table via `viromics::candidate_call_provenance`). No transform runs Pratama's literal filter; it is a join someone runs after the DAG. |
| B6 | clustering (vOTU definition) | `mmseqs easy-cluster --min-seq-id 0.95 -c 0.8` (no `--cov-mode`, so MMseqs2's default of 0) | **see "The vOTU-table mismatch" below** | Both `viromics/mmseqs_precluster.py` (cov-mode 0) and `viromics/mmseqs_votu.py` (cov-mode 1) exist, but the one this library's own convention calls "the vOTU definition" is the cov-mode-1 table -- a choice inherited from Antonio's pipeline, not from Pratama's. Pratama's own literal command is the cov-mode-0 table. T17's `pratama::votu_recovery_table` sidesteps this (it skani-compares the raw frozen FASTA directly, not either cluster table), but anyone citing "our vOTU count" from `votu_cluster_table` would be answering Antonio's question, not Pratama's. |
| B7 | genome quality | CheckV `end_to_end`, default settings | available — `viromics/checkv.py` | Matches; neither side states a database version to check against the other. |
| B8 | vOTU abundance | CoverM `contig --coupled -m trimmed_mean --min-read-percent-identity 0.95 --min-read-aligned-percent 0.75 --min-covered-fraction 0.7` | dropped, substitute — per-contig coverage join through `viromics::candidate_call_provenance` and `viromics::votu_cluster_table` | Same reasoning as A12/Antonio's row 16. Note Pratama used a *different* `--min-covered-fraction` for virus (0.7) than MAG (0.25) mapping -- both are collapsed to the same join-based substitute here, which does not distinguish the two thresholds at all. |
| B9 | taxonomy | vConTACT3 `--nucleotide --db-domain prokaryotes --db-version 220 --exports cytoscape` | substitute (parameter) — `viromics/vcontact3.py` | Runs `vcontact3 run -n ... -e cosmograph`: `-n` calls genes internally via pyrodigal-gv rather than taking `--nucleotide`'s externally-supplied route, and the export is `cosmograph` (node/edge CSVs) rather than `cytoscape` -- deliberate, per the transform's own comment (the image has no `vclust`, so the ANI export vConTACT3's cytoscape path also touches is unavailable regardless). `--db-version 220` vs the paper's stated "NCBI Virus RefSeq release 230" is one of the four version disagreements below. |
| B10 | AMG identification | VirSorter2 `--prep-for-dramv` then `DRAM-v.py annotate --min_contig_size 1000` then `distill`, restricted to vOTUs >=10 kb | substitute (scope) — `functionalAnnotation/dramv.py` | Mechanically the same tool chain and the same `--min_contig_size 1000`. Missing the >=10 kb restriction (runs on every per-sample viral contig VirSorter2 called) -- same pattern as Antonio's row 24. More importantly for T17: the Zenodo data (`AMG_filtered_manual.csv`) shows Pratama's published AMG count is the output of a *manual* curation pass after DRAM-v -- excluding genes flanked by non-metabolic categories (M/A/P/B) -- that has no equivalent step here at all. T17's AMG comparison will be comparing DRAM-v's raw calls against Pratama's manually-curated ones unless that join is added. |
| B11 | microdiversity | MetaPop `--min_cov 70`, mean π over 100 vOTUs x 1000 subsamplings | missing | Same blocker as Antonio's row 18: MetaPop needs every BAM against ONE shared reference, and this library's BAMs are per-sample against that sample's own assembly. Not needed for T17 (recovery/AMG only), so lower priority. |
| B12 | host prediction | `gtdbtk de_novo_wf` (bacteria, archaea) -> `iphop add_to_db` (against `Sept_2021_pub_rw`, GTDB r202-based) -> `iphop predict` **twice**, once against the default db and once against the augmented db | substitute (scope + database) — `metagenomics/taxonomy/gtdbtk_de_novo.py` + `viromics/iphop_add_to_db.py` + `viromics/iphop_predict.py` | Runs the augmented-db predict only; the paper's second pass against the plain default database is not run, so nothing here can distinguish "predicted only because of our own added MAGs" from "predicted anyway." Also a database-vintage gap independent of that: `iphop.env` is pinned to 1.3.3 for `iPHoP_db_Aug23_rw` (GTDB r214), not Pratama's `Sept_2021_pub_rw` (GTDB r202) -- host calls differ for reasons of reference age even where the augmentation logic is faithful. |
| B13 | CRISPR spacers | minced `-minNR 3 -spacers`, then BLASTn `-task blastn-short -word_size 7 -evalue 1e-5 -reward 1 -penalty -1 -ungapped -dust no`, filtered to <=1 mismatch over >=95% length | substitute — `viromics/cctyper.py` + `viromics/blast_spacers_to_contigs.py` | The BLASTn call matches exactly, flag for flag. The spacer *caller* does not: CCTyper's array detection (its own CRT-based logic inside a 705-profile Cas-typing pipeline) stands in for raw minced, and its minimum-repeat behaviour is not minced's `-minNR 3` -- spacer/array counts can differ before the identical BLAST step ever runs. The mismatch/coverage acceptance rule is, as with Antonio's pipeline, deliberately left as a downstream join rather than applied in the search. |

## The assembly conflict

Pratama's own metaSPAdes call is `spades.py --meta -k 21,33,55,77 -m 190`, with no
error-correction pass. Batch 1's citable core is the DOE JGI Metagenome Workflow
(Clum et al. 2021) instead: `bbcms.sh mincount=2 highcountfraction=0.6` error
correction, then `spades.py --meta --only-assembler -k 33,55,77,99,127`, then a
200 bp `seqkit seq --min-len` filter (`transforms/assembly/spades.py`, just changed
in commit `33fd6123` to use seqkit rather than BBTools' `reformat.sh`, which was
shredding its own output on assembly-shaped FASTA -- read that commit before touching
this transform again). This library carries exactly one SPAdes transform on purpose:
`src/metasmith_libraries/AGENTS.md` and this project's history record a second one
being rejected before, on the same "an unpinned interior slot runs both" reasoning
that already governs the spades/megahit choice everywhere else in this campaign.

**Faithful-to-Pratama and citable-core cannot both hold for this step, and this
session does not pick.** What each choice costs:

- Keeping the JGI protocol (what run 2 does today) means every downstream number --
  contig count, N50, which contigs a viral caller ever sees, MAG count -- is being
  measured off an assembly Pratama did not produce. T17's recovery comparisons are
  then partly a test of "does the JGI protocol recover what Pratama's own metaSPAdes
  call recovered," which is a different question from "does this library reproduce
  Pratama."
- Running Pratama's own parameters instead would mean batch 1 is no longer one
  citable protocol across its whole corpus, and either a second SPAdes transform gets
  written (against the standing rejection) or this one run gets a bespoke one-off
  invocation outside the transform library entirely, which is also not free: nothing
  else in the campaign tooling expects a bespoke per-run assembly step.
- The middle path -- adding a *second parameterisation* of the existing transform,
  selected by a flag rather than a second file -- is the one option that does not
  reopen the rejected design. It was not attempted in this session because it changes
  a shared transform every other template in this library also uses, which is a
  decision for whoever owns that transform's roadmap, not a target-set edit.

## The MEGAHIT gap

Pratama's Methods state the rationale directly: "we additionally assembled the
metagenomic reads using MEGAHIT v1.1.3... as different assemblers can yield
complementary viral contigs due to distinct assembly heuristics." Their viral
identification callers therefore see contigs from BOTH assemblies, pooled before
curation. `viromics/merge_candidate_calls.py` pins its callers to exactly ONE
`sequences::assembly` ancestor per contig study (`asm = model.AddRequirement(assembly,
parents={pair})`), which is what keeps this library's own two-assembler ambiguity
trap (`metagenomics_from_paired_reads` running both assemblers and splitting binning
across them) from recurring in the viral lane. Pooling both assemblers' calls into one
frozen set the way Pratama did would need that transform's model changed to accept two
assembly lineages -- a transform change, not a target-set change, and out of scope for
this revision. Run 2's viral candidate set is therefore built from metaSPAdes contigs
only, and will structurally under-call relative to Pratama's dual-assembler union for
reasons that have nothing to do with caller sensitivity.

## The dereplication gap, and why T17's MAG comparison isn't where you'd expect it

`binning::derep_mag_ref` -- the type `pratama_mag_recovery.py` needs to compare against
Pratama's 1275 published MAGs -- is produced by `derep_mag_reference.py`, which needs
`binning_local::quality_bin_fasta` and `binning_local::cluster_table`. Both of those
come from `aggregator.py` (hardcoded to MetaBAT2 + SemiBin2 + COMEBin) and
`skani_dedup.py`, which is the library's dRep substitute -- but that chain has no path
from `sequences::metawrap_bin_fasta` at all. Run 2's own core lane (MetaWRAP over
metabat2/maxbin2/concoct, matching Pratama's actual binners) is never dereplicated, and
its bins can never become a `binning::derep_mag_ref`.

The twist: run 2's viral block already targets `viromics::host_prediction_genome`,
which needs `viromics::iphop_augmented_db`, which needs exactly that aggregator/
skani_dedup/GTDB-Tk-de-novo chain. **Run 2 already runs a second, undocumented binning
ensemble -- MetaBAT2 + SemiBin2 + COMEBin, not MetaWRAP -- purely to get host
predictions for the viral lane**, and it is *that* ensemble's output, not MetaWRAP's,
that T17's MAG-recovery table (now wired in behind `--with-zenodo-comparison`, see
below) actually measures. Reported here because it changes what the campaign is
entitled to claim: "our MAG recovery vs. Pratama's" is really "MetaBAT2+SemiBin2+COMEBin
dereplicated by skani vs. Pratama's MetaWRAP-refined, dRep-dereplicated MAGs," a
different binning method on both sides of the comparison, layered on top of run 2's own
metawrap lane which was believed to be the thing being compared and in fact answers no
target that reaches a recovery table at all.

## The vOTU-table mismatch

See B6. Nothing in this session changes it -- it is a naming/interpretation hazard, not
a solver problem, and T17's own comparison transform bypasses both cluster tables by
comparing the frozen FASTA to Pratama's published vOTU FASTA directly via skani. Flagged
so nobody later cites `votu_cluster_table`'s row count as "our vOTUs, Pratama's
definition" -- it is Antonio's cov-mode convention, not Pratama's literal command.

## Four tool-version disagreements, repository vs. Methods prose

Per the standing constraint, the repository is authoritative. All four are places
where BOTH the repo's literal command/comment and the paper's Methods state an
explicit version and the two differ (checked against `PMC12960796.xml`, the Europe
PMC full text):

| tool | repo says | paper Methods says |
|------|-----------|---------------------|
| MEGAHIT | `MEGAHIT/1.2.9-Python-2.7.18` (module load line, `MetaG_and_MAGs_bioinformatics.md`) | "MEGAHIT v1.1.3 (default)" |
| GTDB-Tk | `GTDB-Tk/1.4.1` (both workflow files) | "GTDB-Tk (version 1.5.1)" |
| abawaca | "Running abawaca=1.0.7" (comment, `MetaG_and_MAGs_bioinformatics.md`) | "abawaca (version 1.0.0)" |
| vConTACT3 reference | `--db-version 220` (`Virus_bioinformatics.md`) | "vConTACT3... uses NCBI Virus RefSeq release 230" |

Not counted as one of the four, but adjacent and worth carrying: `abawaca`, `BinSanity`,
`MaxBin2`, `CONCOCT`, `metabat2`, `dRep`, `MetaWRAP` and `iPHoP` all have versions stated
in the paper's Methods (0.2.7/2.2.6/1.0.0/2.12.1/3.4.0/1.3.2/1.3.2 respectively) that the
repository never states a number for at all, so there is nothing on the repo side to
disagree with numerically -- these are gaps in the repo's own documentation, not
resolvable disagreements. `MetaWRAP refinement (version 1.3.2)` in particular reuses the
same number the paper gives for iPHoP a few paragraphs later, which reads like it could
be a copy-paste artifact in the paper itself; noted, not adjudicated.

The repository and the published abstract also disagree with each other on at least
two headline numbers (vOTU/MAG counts cited in `Groundwater_virome/README.md` vs. the
paper's own abstract) -- this was flagged before this session and is not re-litigated
here; state which source a downstream claim uses.

## What T17 can and cannot compare, today

`--with-zenodo-comparison` (new this session; off by default, mirroring
`--with-gpr-panel`'s exact reasoning) adds `pratama::mag_recovery_table` and
`pratama::votu_recovery_table` to run 2's targets. Both solve cleanly in `--dry-run`
(49 steps against the default 48). Neither can be *staged* yet:
`pratama::published_mags` and `pratama::published_votus` are registered `DEFERRED`
because neither is sourced -- the Zenodo record is on fir
(`/scratch/phyberos/pratama2026/zenodo_17897233/`) only as the zip files it arrived in
(`Filtered_dereplicated_genomes_part{1,2,3}.zip`, `Groundwater-votu-5k.fasta.zip`).
Someone needs to unzip the three MAG parts into one pooled directory and the vOTU zip
into a FASTA, then point real paths at `pratama::published_mags` /
`pratama::published_votus` in `build_inputs_pratama`, before this campaign can actually
run T17 -- not attempted here per "do not launch anything on the cluster."

Once staged, what the comparison actually measures is qualified by everything above:

- **vOTU recovery** compares run 2's frozen candidate set (metaSPAdes-only, missing
  DeepVirFinder as a fourth caller, VIBRANT without `-virome`, geNomad without its two
  sensitivity flags, VirSorter2 with a wider `--include-groups`) against Pratama's
  published >=5 kb catalogue. Every one of those four deviations plausibly moves the
  count in a different direction; the comparison cannot currently attribute a gap to
  any one of them.
- **MAG recovery** compares MetaBAT2+SemiBin2+COMEBin (dereplicated by skani), not
  MetaWRAP, against Pratama's MetaWRAP-refined, dRep-dereplicated, BinSanity/abawaca-
  augmented 1275. Two different binning methods, and the one this campaign believed it
  was running (MetaWRAP) contributes nothing to the number.
- **AMG calls** compares DRAM-v's raw per-sample annotation (no >=10 kb restriction)
  against Pratama's manually-curated, >=10-kb-restricted published set. The manual
  curation step has no equivalent here at all -- not a substitute, a straightforwardly
  missing step -- so a gap in this comparison could be either identification/annotation
  fidelity or simply the absence of that curation pass, and nothing here can tell them
  apart.
