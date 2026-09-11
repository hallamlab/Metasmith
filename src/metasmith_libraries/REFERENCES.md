# `REFERENCES.md` — tool citations for the standard library's transforms

## Purpose & Contents

One entry per third-party tool a transform in this library runs, so the pipeline this
library assembles can be described in a methods section rather than defended from
memory. Each entry states what the tool is, the version this library pins, the
parameters the transform actually passes it (read from the transform's source, not
recalled), the citation, and any deviation between what we run and what the cited
source describes. A deviation is recorded here even when it is a defect to be fixed
elsewhere — this file states what ran, not what should have run.

Scope is the CAMI II / Pratama 2026 campaign's two pipelines: the **core** pipeline
(metaSPAdes assembly, MetaWRAP binning — the combination Juan named as citable) and the
**variant** pipeline (MEGAHIT assembly, MetaBAT2/SemiBin2/COMEBin binning, skani
dereplication). Add an entry here whenever a transform this library ships starts citing
a tool that isn't yet named below; do not let a citation live only in a docstring or a
Slack thread.

---

## The core pipeline

### Read quality trimming — BBDuk (BBTools)
**Transform:** `transforms/assembly/bbduk.py`. **Version:** container
`staphb/bbtools:39.49` (`conda: bbtools`).

**Parameters run:** `ktrim=r k=23 mink=11 hdist=1 tpe tbo maxns=4 qtrim=r trimq=0
usejni=t minlen=<N> maq=3`, plus `int=t` when the read metadata says `parity=paired`,
`ref=/bbmap/resources/adapters.fa` for adapter trimming, and `qin=<33|64> qout=33`
selected from the upstream FastQC/seqkit-reported Phred encoding. `<N>` (`minlen`) is
computed per sample from the read N50 rather than fixed: `min(51, max(N50//3 + 1, 20))`,
so it only ever reaches 51 and can be as low as 20 on a short-read pool.

**Citation.** BBDuk has no dedicated peer-reviewed publication; it is cited through the
protocol that specifies it:

> Clum A, Huntemann M, Bushnell B, Foster B, Foster B, Roux S, Hajek PP, Varghese N,
> Mukherjee S, Reddy TBK, Daum C, Yoshinaga Y, O'Malley R, Seshadri R, Kyrpides NC,
> Eloe-Fadrosh EA, Chen I-MA, Copeland A, Ivanova NN. 2021. DOE JGI Metagenome Workflow.
> mSystems 6:e00804-20. doi 10.1128/mSystems.00804-20, PMID 34006627.

BBTools' own field-standard citation, used when a tool is cited on its own rather than
through the workflow that specifies it, is a technical report rather than a
peer-reviewed paper:
> Bushnell B. 2014. BBMap: A Fast, Accurate, Splice-Aware Aligner. Lawrence Berkeley
> National Laboratory Technical Report LBNL-7065E (9th Annual Genomics of Energy &
> Environment Meeting). No DOI/PMID.

Juan (jcsm2010673) gave the parameter string this transform's `k=23 mink=11 hdist=1 tpe
tbo maxns=4 qtrim=r trimq=0 maq=3 minlen=51 usejni=t` is built from, in direct answer to
a request for something citable — see the journal entry for that exchange.

**Deviations, stated openly:**
- **Version.** The protocol specifies BBDuk 38.79; this library runs BBTools 39.49.
  Newer, not pinned to the cited release.
- **`mlf=33` is dropped.** Juan's parameter string included `mlf=33` (minimum length as
  a fraction of the original read); this transform's `setting` string does not pass it.
  A read can pass BBDuk's length filter here in a case `mlf=33` would have rejected.
- **`minlen` is dynamic, not fixed at 51.** Juan's string fixes `minlen=51`; this
  transform derives it from the sample's own N50 and only caps at 51, so most real
  pools run a shorter minimum length than the cited parameter.
- The protocol's own sentence, quoted directly, names what BBDuk does in the JGI
  workflow and nothing more:
  > BBDuk version 38.79 from the BBTools package is used to remove contamination, trim
  > reads that contain an adapter sequence, and quality trim reads where quality drops
  > to zero.

### Read statistics — seqkit
**Transforms:** `transforms/assembly/seqkit_reads.py` (read QC stats feeding BBDuk's
`minlen` and Phred-encoding choice) and `transforms/assembly/assembly_stats.py`
(post-assembly contig stats). **Version:** container `staphb/seqkit:2.13.0` (`conda:
seqkit`).

**Parameters run:** `seqkit convert --dry-run` (Phred-encoding guess only, discarded
after parsing), `seqkit stat --all --tabular` over reads or the assembly, optionally
`--threads <cpus>`.

**Citation.** Not part of the JGI protocol; this is the library's own QC/statistics
tool, cited on its own terms:
> Shen W, Le S, Li Y, Hu F. 2016. SeqKit: a cross-platform and ultrafast toolkit for
> FASTA/Q file manipulation. PLoS ONE 11(10):e0163962. doi
> 10.1371/journal.pone.0163962, PMID 27706213.

No deviation — seqkit is not the protocol's tool, so there is nothing to deviate from.

### Assembly — metaSPAdes under the DOE JGI Metagenome Workflow
**Transform:** `transforms/assembly/spades.py`. **Versions:** SPAdes container
`quay.io/biocontainers/spades:3.15.5--h95f258a_1` (`conda: spades`); BBTools container
`staphb/bbtools:39.49` (`conda: bbtools`, the same image `bbduk.py` uses).

**Parameters run**, three steps in order:
1. `bbcms.sh mincount=2 highcountfraction=0.6 in=<clean reads> [interleaved=t]
   out=corrected.fastq.gz`
2. `spades.py --meta --only-assembler -k 33,55,77,99,127 -t <cpus>
   -m <0.95 * memory_gb> --12 corrected.fastq.gz -o spades_ws`
3. `reformat.sh in=spades_ws/contigs.fasta out=filtered_contigs.fasta minlength=200`

Single-end reads take `-s` in place of `--meta` and `--12`. Two runtime requirements
are not protocol but are load-bearing, and the reasoning is in the transform's own
comments: `-m` is a hard `setrlimit` on SPAdes' allocation rather than a hint, and
`export OMP_NUM_THREADS=<cpus>` must precede each call because every metasmith runtime
otherwise pins SPAdes' OpenMP hot phases to one thread.

**Citations.** The assembler:
> Nurk S, Meleshko D, Korobeynikov A, Pevzner PA. 2017. metaSPAdes: a new versatile
> metagenomic assembler. Genome Research 27(5):824-834. doi 10.1101/gr.213959.116,
> PMID 28298430.

The protocol this transform implements:
> Clum A, Huntemann M, Bushnell B, Foster B, Foster B, Roux S, Hajek PP, Varghese N,
> Mukherjee S, Reddy TBK, Daum C, Yoshinaga Y, O'Malley R, Seshadri R, Kyrpides NC,
> Eloe-Fadrosh EA, Chen I-MA, Copeland A, Ivanova NN. 2021. DOE JGI Metagenome Workflow.
> mSystems 6:e00804-20. doi 10.1128/mSystems.00804-20, PMID 34006627.

Its three sentences, verbatim, one per step above:
> Filtered reads are error corrected using bbcms version 38.44 from BBTools with a
> minimum count of 2 and a high-count fraction of 0.6.

> These split-error-corrected files are assembled with metaSPAdes version 3.13.0 using
> the "metagenome" flag, running the assembly module only (i.e., without error
> correction) with kmer sizes of 33, 55, 77, 99, and 127.

> Contigs that are smaller than 200 bp are discarded.

**Deviations, stated openly:**
- **Versions.** Protocol: metaSPAdes 3.13.0 and bbcms (BBTools) 38.44. This library:
  SPAdes 3.15.5 and BBTools 39.49. The five flags this entry depends on
  (`--only-assembler`, `-k`, `mincount`, `highcountfraction`, `minlength`) are stable
  across those ranges, but the releases are a stated deviation, not a match.
- **The graph and `contigs.paths` are not length-filtered.** The 200 bp floor names
  contigs. `assembly_graph_with_scaffolds.gfa` and `contigs.paths` describe the full
  assembly and are kept whole, because a filtered contig set would no longer index
  against them.

**This transform changed on 2026-09-11 and the change is invisible to the template
fingerprint.** It was previously a hand-rolled metaSPAdes call: no bbcms, no fixed
kmers, no length floor. The transform's name and its three product types are unchanged,
so `template_fingerprint.py` reports no difference — that tool catches a re-route, not a
behaviour change inside one transform. Two consequences. Every cache shard `spades.py`
wrote is retired, because a transform's identity hashes its whole source file. And every
template that names `spades_assembly` now assembles under this protocol, including
`ecspr_survey_from_pooled_reads`, which was written against the hand-rolled behaviour;
reconciling that is deliberately out of scope here.

### Gene calling — Prodigal, via pprodigal
**Transform:** `transforms/metagenomics/prodigal.py`. **Version:** container
`quay.io/biocontainers/pprodigal:1.0.1--pyhdfd78af_0` (`conda: pprodigal`). pprodigal
(Sebastian Jaenicke, github.com/sjaenick/pprodigal) is a parallelizing wrapper that
forks Prodigal itself across chunks of the input and passes every Prodigal flag through
unchanged; it adds exactly two flags of its own (`-T` worker count, `-C` chunk size),
neither of which touches gene-calling behaviour.

**Parameters run:** `-p meta -f gff -C 100 -T <cpus>` — `-p meta` is Prodigal's own
metagenome mode (no training on a single genome), `-f gff` selects GFF output, `-C 100`
is pprodigal's chunk size (100 sequences per parallel chunk, not a Prodigal parameter),
`-T <cpus>` is pprodigal's worker count.

**Citation.** The underlying gene-calling algorithm is Prodigal's; pprodigal has no
citation of its own beyond its repository:
> Hyatt D, Chen GL, Locascio PF, Land ML, Larimer FW, Hauser LJ. 2010. Prodigal:
> prokaryotic gene recognition and translation initiation site identification. BMC
> Bioinformatics 11:119. doi 10.1186/1471-2105-11-119, PMID 20211023.

No deviation — `-p meta` is exactly Prodigal's metagenome mode and pprodigal changes
nothing about how a gene is called, only how the input is split across workers.

### Post-assembly mapping and stats — minimap2, samtools, bedtools
**Transform:** `transforms/assembly/assembly_stats.py`. **Versions:** `minimap2`
container `biocontainers/minimap2:v2.15dfsg-1-deb_cv1`; `samtools` container
`staphb/samtools:1.23`; `bedtools` container
`biocontainers/bedtools:v2.27.1dfsg-4-deb_cv1` (each also has a matching `conda:` arm).

**Parameters run:**
- `minimap2 -x sr -a -2 <-t cpus> <assembly> <reads>` for short reads (the sole
  case in scope here — the long-read presets `asm5`/`asm10`/`asm20`, chosen by mapped
  read quality, apply only when the sample's `read_metadata.length_class` is `long`).
- `samtools view -b | samtools sort -o <bam> -O bam`, `samtools index -c`, `samtools
  flagstat -O tsv`, each with `-@ <cpus>`.
- `bedtools genomecov -ibam <bam> -bg` for per-bp coverage.

**Citations.** Not JGI-protocol tools; standard mapping/alignment utilities, cited on
their own terms:
> Li H. 2018. Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics
> 34(18):3094-3100. doi 10.1093/bioinformatics/bty191, PMID 29750242.

> Danecek P, Bonfield JK, Liddle J, Marshall J, Ohan V, Pollard MO, Whitwham A, Keane T,
> McCarthy SA, Davies RM, Li H. 2021. Twelve years of SAMtools and BCFtools.
> GigaScience 10(2):giab008. doi 10.1093/gigascience/giab008, PMID 33590861.

> Quinlan AR, Hall IM. 2010. BEDTools: a flexible suite of utilities for comparing
> genomic features. Bioinformatics 26(6):841-842. doi 10.1093/bioinformatics/btq033,
> PMID 20110278.

No deviation — none of the three are protocol-cited tools; there is no source to deviate
from beyond their own papers.

### Binning and refinement — MetaWRAP
**Transform:** `transforms/metagenomics/binning/metawrap.py`. **Version:** container
`quay.io/biocontainers/metawrap-mg:1.3.0--hdfd78af_1`, digest
`sha256:41c69171237bb16e83cfc137f36457c22e19d867336d8723ab1cdc8aa2721c84`, verified
2026-09-10 by running `metawrap --version` (`metaWRAP v=1.3.0`) and `checkm taxon_list`
(confirms CheckM **1**'s 1.4 GB reference tree is present at
`/usr/local/etc/checkm`, the package's own `DATA_CONFIG` dataRoot).

**Parameters run:**
- `metawrap binning -o binning -t <cpus> -m <mem_gb> -a <assembly> --metabat2
  --maxbin2 --concoct reads_1.fastq reads_2.fastq` — the three binners MetaWRAP's own
  binning module wraps, over an interleaved read pair split to two files because
  MetaWRAP's binning module refuses any other input shape.
- `metawrap bin_refinement -o refinement -t <cpus> -m <refine_mem> [--quick] -A
  binning/metabat2_bins -B binning/maxbin2_bins -C binning/concoct_bins -c 50 -x 10` —
  `-c 50 -x 10` (`MIN_COMPLETION`, `MAX_CONTAMINATION` in the transform) are pinned
  rather than exposed as parameters, because `bin_refinement` interpolates both into its
  output directory name (`metawrap_50_10_bins`) and a transform that let them vary would
  have to glob for its own products. `refine_mem` is `max(mem_gb, 40)` — `bin_refinement`
  turns `-m` into a CheckM placement thread count by integer division against 40, so
  anything below 40 asks CheckM for zero placement threads; `--quick` (CheckM's reduced
  reference tree) is added whenever the sample's memory allocation is under CheckM's
  documented 40 GB floor for the full tree, and the transform logs when it does because
  completeness/contamination shift slightly on the reduced tree.

**Citation:**
> Uritskiy GV, DiRuggiero J, Taylor J. 2018. MetaWRAP—a flexible pipeline for
> genome-resolved metagenomic data analysis. Microbiome 6:158. doi
> 10.1186/s40168-018-0541-1, PMID 30219103.

**Deviation:** none against the cited pipeline's own defaults — `-c 50 -x 10` are
`bin_refinement`'s published default thresholds, and `--metabat2 --maxbin2 --concoct` is
its own three-binner set. The one thing to flag is scope, not deviation: this transform
runs MetaWRAP's binning and bin_refinement modules only. MetaWRAP's own paper and the
Pratama groundwater pipeline both continue past refinement into reassembly and
additional binners/dereplication; that extension is not implemented here (noted as
`# None of that is here; it is the next increment.` in the transform's own header
comment).

---

## The variant pipeline

Swaps the core pipeline's assembler and binners; MetaWRAP's binning module is not run in
this arm; the JGI protocol still governs read trimming (BBDuk, above) up to the
assembly step.

### Assembly — MEGAHIT
**Transform:** `transforms/assembly/megahit.py`. **Version:** container
`biocontainers/megahit:1.2.9_cv1` (`conda: megahit`).

**Parameters run:** `megahit --num-cpu-threads <cpus> --memory <0.85 * mem_bytes>
--12 <interleaved reads> -o megahit_ws` for paired reads (`-r <reads>` for single-end).
No k-mer list is set — MEGAHIT's own default multi-k iteration (21 through 141, step
12) runs unmodified.

**Citation:**
> Li D, Liu CM, Luo R, Sadakane K, Lam TW. 2015. MEGAHIT: an ultra-fast single-node
> solution for large and complex metagenomics assembly via succinct de Bruijn graph.
> Bioinformatics 31(10):1674-1676. doi 10.1093/bioinformatics/btv033, PMID 25609793.

**Not a JGI-protocol substitution, and that distinction matters.** The JGI paper's own
text on assembler choice is descriptive, not comparative:
> ... many tools are available for each of the previously described steps, including
> metaSPAdes and MEGAHIT for assembly ...

That sentence names MEGAHIT as one tool among several the workflow *could* use; it
states no benchmark and gives no rationale for choosing one assembler over the other.
It supports "MEGAHIT is a tool the DOE JGI workflow acknowledges" — it does **not**
support "MEGAHIT was chosen because it beats metaSPAdes" or any other comparative claim.
The variant pipeline's citation for running MEGAHIT is Li et al. 2015 above, not the JGI
paper.

### Binning — MetaBAT2, SemiBin2, COMEBin
**Transforms:** `transforms/metagenomics/binning/{metabat2,semibin2,comebin}.py`, each
independent (unlike MetaWRAP, there is no refinement step consolidating their outputs
in this library yet — that is `skani_dedup.py`, below, plus whatever T6's scoring
transform reads).

**MetaBAT2.** Version: container `quay.io/biocontainers/metabat2:2.17--h6f16272_1`
(`conda: metabat2`). Parameters run: `jgi_summarize_bam_contig_depths --outputDepth
depth.txt <bam>` then `metabat2 -i <assembly> -a depth.txt -o <bin_prefix> -t <cpus>` —
depth-file generation and clustering both at MetaBAT2's own defaults.
> Kang DD, Li F, Kirton E, Thomas A, Egan R, An H, Wang Z. 2019. MetaBAT 2: an adaptive
> binning algorithm for robust and efficient genome reconstruction from metagenome
> assemblies. PeerJ 7:e7359. doi 10.7717/peerj.7359, PMID 31388474.

**SemiBin2.** Version: container `quay.io/biocontainers/semibin:2.1.0--pyhdfd78af_0`
(`conda: semibin`). Parameters run: `SemiBin2 single_easy_bin -i <assembly> -b <bam> -o
semibin_out --environment global -t <cpus>` — `--environment global` selects SemiBin2's
pretrained global model over training a sample-specific one.
> Pan S, Zhao XM, Coelho LP. 2023. SemiBin2: self-supervised contrastive learning leads
> to better MAGs for short- and long-read sequencing. Bioinformatics
> 39(Suppl 1):i21-i29. doi 10.1093/bioinformatics/btad209, PMID 37387171.

**COMEBin.** Version: container `quay.io/hallamlab/external_comebin:gpu-1.0.4`
(`conda: comebin`; `hardware: gpu` in `data_types/env.yml`). Parameters run:
`run_comebin.sh -a <assembly> -o comebin_out -p bam_input -t <cpus> -b <batch_size>`,
where `batch_size = min(usable_contigs, 1024)` (`usable_contigs` counts contigs
&ge;1000 bp; below 2 usable contigs the transform refuses rather than invoking
COMEBin at all) and `--nv`/`CUDA_VISIBLE_DEVICES` pass the GPU through the container
runtime.
> Wang Z, You R, Han H, Liu W, Sun F, Zhu S. 2024. Effective binning of metagenomic
> contigs using contrastive multi-view representation learning. Nature Communications
> 15:585. doi 10.1038/s41467-023-44290-z, PMID 38233391.

No deviations — none of the three transforms fix a threshold or flag the tool's own
published defaults don't already set; `--environment global` and the depth-summary step
are each the tool's own documented entry point for this input shape.

### Dereplication — skani
**Transform:** `transforms/metagenomics/binning/skani_dedup.py`. **Version:** container
`staphb/skani:0.2.2` (`conda: skani`).

**Parameters run:** `skani triangle -l bins.list --sparse -o skani_ani.tsv -t <cpus>`
over every quality-passing bin, then two independent single-linkage clusterings of the
resulting all-vs-all ANI matrix at 95% and 99% ANI (`ANI_THRESHOLDS = [95.0, 99.0]` in
the transform), each cluster's centroid taken as the member with the highest mean
intra-cluster ANI. The two thresholds and the medoid-selection rule are this library's
own clustering policy layered on top of skani's pairwise ANI output — skani itself
computes ANI only, and does not cluster.
> Shaw J, Yu YW. 2023. Fast and robust metagenomic sequence comparison through sparse
> chaining with skani. Nature Methods 20:1661-1665. doi 10.1038/s41592-023-02018-3,
> PMID 37814184.

### Bin quality — CheckM2
**Transform:** `transforms/metagenomics/binning/checkm.py`. **Version:** container
`quay.io/hallamlab/external_checkm2:1.1.0` (`conda: checkm`).

**Parameters run:** `checkm2 predict --threads <cpus> --genes -x faa --input ./faa
--output-directory ./checkm2_out`, over per-bin protein FASTA the transform generates
itself with `prodigal -i <bin> -a <faa> -o /dev/null -p meta -q` ahead of CheckM2, one
bin at a time in an isolated subprocess. That two-stage shape is a workaround, not a
parameter choice: CheckM2 1.1.0 has a pyrodigal-gv heap bug
(github.com/chklovski/CheckM2 issue #149) that truncates the whole batch's shared FAA
mid-write when one bin trips it, failing every bin in the batch. Running `--genes`
mode (skip CheckM2's own gene calling) against a pre-computed, per-bin FAA keeps one
bin's crash from taking down the batch; a bin whose isolated prodigal call itself aborts
gets a sentinel row (`Completeness=0, Contamination=0`, `Additional_Notes` naming the
failure) instead of no row at all.
> Chklovski A, Parks DH, Woodcroft BJ, Tyson GW. 2023. CheckM2: a rapid, scalable and
> accurate tool for assessing microbial genome quality using machine learning. Nature
> Methods 20:1203-1212. doi 10.1038/s41592-023-01940-w, PMID 37500759.

**Note the version split with the core pipeline.** MetaWRAP's `bin_refinement`
(core pipeline, above) shells out to **CheckM 1**, bundled inside the
`metawrap-mg:1.3.0` image; this standalone transform runs **CheckM2**, a different tool
from the same lab with a different scoring model, in its own `external_checkm2:1.1.0`
image. A completeness/contamination pair from one is not directly comparable to the
other without saying which version produced it.
> Parks DH, Imelfort M, Skennerton CT, Hugenholtz P, Tyson GW. 2015. CheckM: assessing
> the quality of microbial genomes recovered from isolates, single cells, and
> metagenomes. Genome Research 25(7):1043-1055. doi 10.1101/gr.186072.114,
> PMID 25977477.

---

## Scoring

Not yet built as of this entry (T6 in the campaign plan); named here so the tools they
will cite are recorded before the transforms exist, per this campaign's own rule that a
citation should not live only in a Slack thread.

### CAMI — AMBER
The CAMI II scoring transform will take a contig-to-bin table plus the CAMI
gold-standard bin assignment and emit AMBER's metrics.
> Meyer F, Hofmann P, Belmann P, Garrido-Oter R, Fritz A, Sczyrba A, McHardy AC. 2018.
> AMBER: Assessment of Metagenome BinnERs. GigaScience 7(6):giy069. doi
> 10.1093/gigascience/giy069, PMID 29893851.

The gold standard and participant submissions AMBER scores against are CAMI II's. The
author list runs past sixty names; cited here by its lead authors plus "et al." rather
than transcribed in full, since a wrong middle name in a sixty-author list is a worse
failure mode than an honest truncation:
> Meyer F, Fritz A, Deng ZL, Koslicki D, Lesker TR, Gurevich A, et al. 2022. Critical
> assessment of metagenome interpretation: the second round of challenges. Nature
> Methods 19:429-440. doi 10.1038/s41592-022-01431-4, PMID 35396482.

### Pratama 2026 — Zenodo recovery comparison
The Pratama scoring transform will take the recovered vOTUs and MAGs plus the authors'
own Zenodo products and emit a recovery table (vOTU recovery, MAG recovery, AMG calls).
> Pratama AA, Pérez-Carrascal O, Sullivan MB, Küsel K. 2026. Diversity and ecological
> roles of hidden viral players in groundwater microbiomes. Nature Communications
> 17:2179. doi 10.1038/s41467-026-68914-2, PMCID PMC12960796.

Zenodo record: 10.5281/zenodo.17897233.

**The published GitHub workflow repository and the paper's Methods prose disagree on
four tool versions.** Where they conflict, the repository is authoritative for this
campaign's reproduction, because it is the thing that actually ran — a Methods section
is written prose about a pipeline, and the repository is the pipeline. Which four tools
and which two version numbers apply belongs beside the transform that reads the
Zenodo products, not duplicated here; record it there when that transform is built.

---

## Deferred, not yet in this library

**MetaPathways** (functional annotation for the variant pipeline's GEM-drafting
extension) has no transform, no environment, and no container entry yet — it appears
only in prose and one old notebook as of this entry. When it is built (campaign task
T18), its citation is a chain of four rather than one, because the version that runs
(2.5), the pipeline paper, the method behind pathway inference, and the taxonomy
algorithm are each the lab's own separate publication:

> Konwar KM, Hanson NW, Bhatia MP, Kim D, Wu SJ, Hahn AS, Morgan-Lang C, Cheung HK,
> Hallam SJ. 2015. MetaPathways v2.5: quantitative functional, taxonomic and usability
> improvements. Bioinformatics 31(20):3345-3347. doi 10.1093/bioinformatics/btv361,
> PMID 26076725, PMCID PMC4595896.

> Konwar KM, Hanson NW, Pagé AP, Hallam SJ. 2013. MetaPathways: a modular pipeline for
> constructing pathway/genome databases from environmental sequence information. BMC
> Bioinformatics 14:202. doi 10.1186/1471-2105-14-202, PMID 23800136.

> Hanson NW, Konwar KM, Hawley AK, Altman T, Karp PD, Hallam SJ. 2014. Metabolic
> pathways for the whole community. BMC Genomics 15:619. doi 10.1186/1471-2164-15-619,
> PMID 25048541.

> Hanson NW, Konwar KM, Hallam SJ. 2016. LCA*: an entropy-based measure for taxonomic
> assignment within assembled metagenomes. Bioinformatics 32(23):3535-3542. doi
> 10.1093/bioinformatics/btw400, PMID 27515739.

**GEM drafting** is deferred past this campaign entirely (explicitly out of scope for
this run) and has no transform to cite yet.

---

## The JGI paper is a workflow description, not a benchmark

Stated plainly because it is easy to over-read: the DOE JGI Metagenome Workflow paper
(Clum et al. 2021, cited throughout this file) documents which tools JGI's own
production pipeline runs and with what settings. It reports no comparison between
assemblers, no accuracy or recovery numbers for metaSPAdes against alternatives, and no
argument for why metaSPAdes was chosen over MEGAHIT or any other assembler — its own
text on the matter is descriptive ("many tools are available … including metaSPAdes and
MEGAHIT for assembly"), not comparative. Citing it supports "this is the DOE JGI
reference workflow, run with these tools at these settings." It does **not** support
"metaSPAdes beats MEGAHIT," "metaSPAdes recovers more genomes," or any other performance
claim — the paper makes none. Anyone citing this file's core pipeline for reproducibility
should cite it for exactly the first claim and not lean on it for the second.
