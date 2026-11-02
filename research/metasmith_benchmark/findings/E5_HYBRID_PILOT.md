# E5 hybrid pilot: graph resolution against polishing

## Purpose & Contents

This file records the pilot that chose E5's hybrid assembly lane, and the evidence behind the choice. It holds findings only. The driver `drivers/e5_hybrid.py` and the library `library/transforms/e5/` are the source of truth for the commands. The scores and costs are in `results/e5_hybrid/`. Paths below are relative to `research/metasmith_benchmark/`.

It covers the decision, the datasets and lanes, the results per metric, the checks that the numbers are believable, the attempts to join Flye with MEGAHIT, and the open follow-ups.

## Decision

E5 adopts graph resolution: MEGAHIT, then OPERA-MS scaffolds and gap-fills its contigs with the long reads. It replaces hybrid metaSPAdes, which took 48 cores, up to 384 GB and more than 11 h per pairing in E3.

The reasons, from the tables below:
1. OPERA-MS holds the most sequence in contigs of 50 kb or more on every CAMI PacBio dataset, and the most on toy humangut.
2. It is the cheapest hybrid lane on every CAMI dataset, at 7–35 CPU-hours per sample.
3. Its recovery stays within 3 points of the best method on every scored dataset.
4. It extends the MEGAHIT assembly E5 already makes, so it adds one step rather than a second assembler.

CAUTION: Pratama, the one real dataset, favours polishing. There, Flye + POLCA costs 45 CPU-hours against OPERA-MS's 88, and its N50 is 26.7 kb against 4.3 kb. E5's long reads are real Nanopore. Re-test the choice when E5 runs on Pratama at scale.

## Datasets and lanes

A dataset qualifies when its long reads come from the same sample as a short-read set. The pilot draws 10% of each, at least one sample, with a fixed seed: 19 samples in all.

| Study | Long reads | Samples drawn |
|---|---|---|
| CAMI II marine | PacBio CLR (simulated) | 1 |
| CAMI II plant_associated | Nanopore (simulated) | 2 |
| CAMI II plant_associated | PacBio CLR (simulated) | 2 |
| CAMI II strain | PacBio CLR (simulated) | 10 |
| CAMI III toy_humangut | Nanopore R10 (simulated) | 2 |
| Pratama 2026 | Nanopore R9.4.1 (real) | 2 |

Each sample assembles every read it has. The 10% applies to samples, not to reads.

Every lane starts from QC'd short reads and raw long reads:
- **QC.** CAMI uses the standard library's bbduk on JGI's settings (Clum 2021). Pratama uses bbduk on the paper's settings. CAUTION: Pratama's `trimq=20` keeps 14% of CAMI II's bases, because CAMI II simulates Q3 base calls. JGI's settings keep 99.97%.
- **MEGAHIT.** Short reads only. It is the baseline for graph resolution.
- **OPERA-MS 0.9.0.** MEGAHIT's contigs plus the long reads, with `--no-ref-clustering` and `--no-polishing` (Bertrand 2019).
- **Flye 2.9.6.** Long reads only, `--meta`, in the platform's mode (Kolmogorov 2020). It is the baseline for polishing.
- **Flye + POLCA.** Flye's contigs corrected with the short reads by POLCA from MaSuRCA 4.1.4 (Zimin & Salzberg 2020).

## Scoring

metaQUAST scores each CAMI sample against the source genomes present in that sample (Mikheenko 2016). The present set comes from the sample's gold-standard assembly mapping (`results/e5_hybrid/sample_genomes.tsv`). Reference-free QUAST scores Pratama, which has no references.

CAUTION: metaQUAST's combined-reference run counts every reference a contig matches, so near-identical strains inflate its error rates past 100%. The error rates below pool the per-reference runs instead (`results/e5_hybrid/score.py`).

Every figure below is the median over a dataset's samples.

## Results

### Contiguity

NA50 against the sample's genomes. Pratama has no references, so its row is N50.

| Dataset | MEGAHIT | OPERA-MS | Flye | Flye + POLCA |
|---|---|---|---|---|
| marine PacBio | 1.6 kb | **7.2 kb** | 4.3 kb | 4.2 kb |
| plant PacBio | 3.4 kb | **15.8 kb** | 4.3 kb | 4.3 kb |
| strain PacBio | 4.4 kb | **8.4 kb** | 4.5 kb | 4.5 kb |
| plant Nanopore | 3.3 kb | 20.2 kb | **91.8 kb** | **91.8 kb** |
| toy humangut Nanopore | 14.9 kb | 66.0 kb | **69.2 kb** | 69.0 kb |
| Pratama Nanopore (N50) | 1.5 kb | 4.3 kb | **26.8 kb** | 26.7 kb |

Sequence in contigs of 50 kb or more, in Mb:

| Dataset | MEGAHIT | OPERA-MS | Flye | Flye + POLCA |
|---|---|---|---|---|
| marine PacBio | 19.7 | **41.8** | 0.0 | 0.0 |
| plant PacBio | 25.4 | **37.1** | 0.9 | 0.9 |
| strain PacBio | 4.2 | **7.8** | 0.2 | 0.2 |
| plant Nanopore | 26.6 | 49.3 | **64.6** | 64.3 |
| toy humangut Nanopore | 66.4 | **107.2** | 101.8 | 101.6 |
| Pratama Nanopore | 14.1 | 93.4 | **125.4** | 125.2 |

Flye fragments on CAMI's simulated PacBio CLR. OPERA-MS gains over MEGAHIT on every dataset.

### Recovery

Mean genome fraction over the genomes present in the sample, in percent. The ceiling is the gold-standard assembly's own fraction, from `results/e5_hybrid/gsa_ceiling.tsv`. No assembly of that sample can beat it.

| Dataset | Ceiling | MEGAHIT | OPERA-MS | Flye | Flye + POLCA |
|---|---|---|---|---|---|
| marine PacBio | 60.0 | 21.1 | **21.9** | 9.5 | 9.5 |
| plant PacBio | 30.8–37.5 | 10.0 | **10.2** | 5.5 | 5.5 |
| plant Nanopore | 33.5–35.5 | 10.6 | **11.1** | 9.8 | 9.9 |
| toy humangut Nanopore | 46.1–54.2 | 37.1 | **38.5** | 31.3 | 31.2 |
| strain PacBio | 25.7–34.0 | (58.0) | (67.5) | (81.2) | (81.9) |

CAUTION: strain recovery exceeds its ceiling. metaQUAST credits one contig to every near-identical sibling strain it matches. Strain recovery and strain error rates are therefore not used.

### Error rate

Mismatches and indels per 100 kb aligned, pooled over the per-reference runs. Misassemblies per Mb aligned, from the combined run.

| Dataset | Metric | MEGAHIT | OPERA-MS | Flye | Flye + POLCA |
|---|---|---|---|---|---|
| marine PacBio | mismatches | 1,112 | 1,454 | 674 | **649** |
| | indels | **15** | 604 | 2,251 | 1,665 |
| | misassemblies | 35.1 | 30.0 | **0.4** | **0.4** |
| plant PacBio | mismatches | 887 | 1,145 | 460 | **458** |
| | indels | **17** | 482 | 1,996 | 1,644 |
| | misassemblies | 20.7 | 18.1 | **0.3** | **0.3** |
| plant Nanopore | mismatches | **908** | 1,614 | 1,279 | 1,291 |
| | indels | **20** | 651 | 1,208 | 601 |
| | misassemblies | 20.1 | 19.1 | **3.9** | 4.0 |
| toy humangut Nanopore | mismatches | **191** | 656 | 855 | 567 |
| | indels | **6** | 419 | 659 | 312 |
| | misassemblies | 5.2 | 5.2 | **1.4** | **1.4** |

MEGAHIT's mismatches and misassemblies are real, not a scoring artifact. CAMI II simulates many close strains, and a short-read assembler builds strain consensus and chimeras from them. OPERA-MS inherits them, and adds 400–650 indels per 100 kb from gap fills of raw long reads. POLCA cuts Flye's indels by 18–53%.

### Cost

CPU-hours per sample for the whole lane, QC included, from `sacct` (`results/e5_hybrid/cost_lanes.tsv`).

| Dataset | MEGAHIT | OPERA-MS | Flye | Flye + POLCA |
|---|---|---|---|---|
| marine PacBio | 21 | **35** | 60 | 65 |
| plant PacBio | 11 | **17** | 56 | 62 |
| strain PacBio | 4 | **7** | 28 | 30 |
| plant Nanopore | 9 | **16** | 10 | 17 |
| toy humangut Nanopore | 7 | **15** | 10 | 15 |
| Pratama Nanopore | 50 | 88 | 33 | **45** |

Bold marks the cheaper of the two hybrid lanes. Peak memory stays under 64 GB in every lane. fir's MaxRSS counts page cache, so it overstates the need.

## Believability checks

The CAMI II numbers sit well below the CAMI II paper's, which scored pooled co-assemblies (Meyer 2022). These checks confirm the numbers anyway:
1. **The scorer.** The gold-standard assembly of marine sample 3, scored the same way, gives 0 mismatches, 0 indels and 0 misassemblies. Its length-weighted genome fraction is 31.7%, against MEGAHIT's 14.6%.
2. **Multi-mapping.** `--unique-mapping` moves marine's error rates by 10–30% and keeps every method in the same order.
3. **The ceiling.** A single sample covers only 26–60% of its genomes, by its own gold standard. MEGAHIT reaches 29–35% of that ceiling on CAMI II and 74% on toy humangut. On pooled marine reads, CAMI II's MEGAHIT reached 41.1% against a 76.9% ceiling, or 53%.
4. **The literature.** CAMI II also found Flye weak on marine and strong on plant Nanopore. OPERA-MS's marine NGA50 there was 28 kb on the pool.

## Joining Flye with MEGAHIT

Two joins were scored on the seven non-strain CAMI samples (`results/e5_hybrid/hybmerge/`). Neither changes the decision.

1. **`flye --subassemblies`** on Flye + POLCA plus all of MEGAHIT keeps 8–26% of the input sequence, and recovery falls to 3–17%. Flye treats each input assembly as one fold of coverage and drops what only one input holds, so the output is the shared regions. Flye's docs mark the mode deprecated. CAUTION: it also refuses `.fna` input names.
2. **The unaligned merge** keeps every Flye + POLCA contig and adds each MEGAHIT contig that minimap2 (`asm20`) does not cover at 90% identity over 90% of its length (Li 2018). It has the best recovery on every scored dataset: 23.7, 11.2, 13.9 and 40.0%. On Nanopore it keeps Flye's long contigs. On PacBio it drops to 2.2–6.4 Mb in contigs of 50 kb or more, because a long MEGAHIT contig covered by several short Flye pieces counts as held. It costs 1.3–1.6× OPERA-MS on Nanopore and 2.4–4.3× on CAMI PacBio.

## Follow-ups

1. Enable OPERA-MS's own Pilon polishing, which the pilot switched off, and measure the indel rate and the cost it adds.
2. Re-score the lanes on more Pratama pairings, the only real long reads in the campaign.
3. Score strain with references restricted to one strain per species, or by a strain-aware method, before using its recovery.

## References

- Bertrand D et al. (2019). Hybrid metagenomic assembly enables high-resolution analysis of resistance determinants and mobile elements in human microbiomes. Nat Biotechnol 37:937–944. https://doi.org/10.1038/s41587-019-0191-2
- Clum A et al. (2021). DOE JGI Metagenome Workflow. mSystems 6:e00804-20. https://doi.org/10.1128/mSystems.00804-20
- Kolmogorov M et al. (2019). Assembly of long, error-prone reads using repeat graphs. Nat Biotechnol 37:540–546. https://doi.org/10.1038/s41587-019-0072-8
- Kolmogorov M et al. (2020). metaFlye: scalable long-read metagenome assembly using repeat graphs. Nat Methods 17:1103–1110. https://doi.org/10.1038/s41592-020-00971-x
- Li D et al. (2015). MEGAHIT: an ultra-fast single-node solution for large and complex metagenomics assembly via succinct de Bruijn graph. Bioinformatics 31:1674–1676. https://doi.org/10.1093/bioinformatics/btv033
- Li H (2018). Minimap2: pairwise alignment for nucleotide sequences. Bioinformatics 34:3094–3100. https://doi.org/10.1093/bioinformatics/bty191
- Meyer F et al. (2022). Critical Assessment of Metagenome Interpretation: the second round of challenges. Nat Methods 19:429–440. https://doi.org/10.1038/s41592-022-01431-4
- Mikheenko A, Saveliev V, Gurevich A (2016). MetaQUAST: evaluation of metagenome assemblies. Bioinformatics 32:1088–1090. https://doi.org/10.1093/bioinformatics/btv697
- Pratama AA et al. (2026). Nat Commun 17:2179. https://doi.org/10.1038/s41467-026-68914-2
- Zimin AV, Salzberg SL (2020). The genome polishing tool POLCA makes fast and accurate corrections in genome assemblies. PLoS Comput Biol 16:e1007981. https://doi.org/10.1371/journal.pcbi.1007981
- Flye usage, `--subassemblies`: https://github.com/mikolmogorov/Flye/blob/flye/docs/USAGE.md
