# The new pipeline against the original MetaPathways: what changed

15 assemblies compared. Each assembly ran through the original MetaPathways (4.0.3) and through
the new pipeline. Numbers below are ranges across the assemblies.

## What you need to know

- **The genes are the same.** Both pipelines use the same gene finder (Prodigal, metagenome
  mode). 98.3 to 99.8% of the original's genes are called with identical
  coordinates by the new pipeline, so gene-level results can be matched one to one. Most of the
  rest are weak, low-confidence calls that only one pipeline makes; a few share the stop codon
  but start elsewhere.
- **The original leaves short proteins unannotated.** It skips proteins under 60 amino
  acids (3.8 to 9.9% of its genes). The new
  pipeline annotates every protein.
- **Named functions are close.** The share of genes with a named (not "hypothetical") function
  is 61 to 85% in the original and 73 to 95% in the new pipeline,
  partly because of the skipped short proteins. Where both searched UniRef50, the top hit has the same name for
  79 to 84% of genes. The UniRef50 release and the search program both
  differ, so this residue cannot be split between them.
- **Enzyme (EC) calls differ much more, and that is a method change.** The original gives an EC
  number to 16 to 25% of genes; the new pipeline's Pathway Tools input gives one
  to 23 to 30%. The new pipeline predicts enzymes with machine-learning
  models rather than only by similarity to known proteins. Where both pipelines give the same
  gene an EC number and at least two of the new pipeline's seven methods agree, it matches the
  original for 90% of genes. Counting calls that rest on a single method, the
  agreement is 79%. **Use the calls that two or more methods agree on.**
- **The ESM-C second opinion is a separate column, not part of the consensus.** It transfers
  reactions from the most similar reviewed protein. At its default cut-off (similarity 0.97) it
  matches the original for 61% of genes; at 0.985 it matches 96%. Checked
  against KofamScan and UniRef50, its calls are right 51 to 75% of the time at 0.97 (lowest in
  the cu6 and cu9_11 assemblies) and 88 to 96% at 0.985. Filter it at 0.985 unless you
  need the extra calls.
- **Pathway lists overlap but are not the same.** The community databases hold
  836 to 1044 pathways (original) and 931 to 1127 (new); the share of
  pathways in common (shared / either) is 0.52 to 0.57. Per MAG it is
  0.46 to 0.60. Both used the same Pathway Tools and MetaCyc release
  (27.0), so the difference comes from what each pipeline feeds Pathway Tools: product names
  plus EC numbers in the original, EC numbers from one predictor (CLEAN) in the new pipeline.
- **The new pipeline adds annotation types the original lacks**: mobile elements (plasmids,
  viruses, insertion sequences, integrons), non-coding RNAs, antibiotic and metal resistance
  genes, biosynthetic gene clusters, protein domains (InterProScan, 72 to 88%
  of genes), transcription factors and promoters.

## Per assembly

Pairs are original / new. "Exact" is the share of the original's genes called identically.
"Pathway overlap" is shared pathways divided by pathways in either database.

| Assembly | Genes | Exact | Named function | With EC | Community pathways | Pathway overlap | MAG overlap (mean) |
|---|---|---|---|---|---|---|---|
| hybridspades_cpe3_r2a | 50,978 / 50,866 | 99.7% | 77% / 93% | 24% / 30% | 988 / 1053 | 0.57 | 0.54 |
| hybridspades_cpe3_r2a_cu1_5 | 32,148 / 32,085 | 99.5% | 74% / 86% | 21% / 29% | 928 / 1027 | 0.56 | 0.50 |
| hybridspades_cpe3_r2a_cu6 | 46,684 / 46,447 | 99.0% | 67% / 76% | 18% / 26% | 977 / 1077 | 0.54 | 0.49 |
| hybridspades_cpe3_r2a_cu7_8 | 26,369 / 26,330 | 99.8% | 84% / 94% | 23% / 29% | 836 / 931 | 0.54 | 0.49 |
| hybridspades_cpe3_r2a_cu9_11 | 51,772 / 51,444 | 99.0% | 65% / 77% | 18% / 24% | 931 / 998 | 0.52 | 0.49 |
| operams_cpe3_r2a | 52,192 / 52,137 | 99.6% | 81% / 94% | 25% / 30% | 975 / 1037 | 0.56 | 0.55 |
| operams_cpe3_r2a_cu1_5 | 39,153 / 39,010 | 99.0% | 73% / 84% | 21% / 28% | 1030 / 1085 | 0.57 | 0.52 |
| operams_cpe3_r2a_cu6 | 68,156 / 67,689 | 98.6% | 62% / 73% | 16% / 24% | 1033 / 1127 | 0.57 | 0.49 |
| operams_cpe3_r2a_cu7_8 | 37,138 / 37,085 | 99.5% | 85% / 95% | 24% / 29% | 888 / 962 | 0.55 | 0.49 |
| operams_cpe3_r2a_cu9_11 | 67,460 / 67,022 | 98.7% | 64% / 73% | 16% / 23% | 1001 / 1078 | 0.55 | 0.49 |
| shortspades_cpe3_r2a | 49,253 / 49,165 | 99.7% | 78% / 94% | 25% / 30% | 968 / 1033 | 0.56 | 0.54 |
| shortspades_cpe3_r2a_cu1_5 | 33,321 / 33,225 | 99.2% | 78% / 88% | 23% / 30% | 985 / 1048 | 0.57 | 0.51 |
| shortspades_cpe3_r2a_cu6 | 66,947 / 66,466 | 98.4% | 63% / 73% | 17% / 24% | 1044 / 1121 | 0.56 | 0.50 |
| shortspades_cpe3_r2a_cu7_8 | 35,319 / 35,270 | 99.7% | 82% / 94% | 23% / 28% | 876 / 972 | 0.55 | 0.49 |
| shortspades_cpe3_r2a_cu9_11 | 66,480 / 66,023 | 98.3% | 61% / 73% | 16% / 24% | 998 / 1066 | 0.55 | 0.50 |

## EC agreement by method, on genes both pipelines called identically

"Agree" counts genes where both sides gave an EC number and at least one matches exactly.
"Close" means only the first three EC digits match.

| New-pipeline method | Genes with EC on both sides | Agree | Close | Original only | New only |
|---|---|---|---|---|---|
| CLEAN | 98,536 | 81% | 9% | 45,455 | 101,310 |
| DeepEC | 54,831 | 82% | 6% | 89,160 | 10,483 |
| EZpred | 42,023 | 87% | 5% | 101,968 | 32,788 |
| KofamScan | 85,162 | 93% | 4% | 58,829 | 26,725 |
| ProteinBERT | 98,610 | 78% | 7% | 45,381 | 224,890 |
| ESM-C (consensus vote) | 36,592 | 96% | 1% | 107,399 | 18,910 |
| 7-method consensus | 123,722 | 79% | 7% | 20,269 | 280,657 |
| 7-method consensus, 2+ methods agree | 95,741 | 90% | 4% | 48,250 | 57,042 |
| ESM-C second opinion (not in the consensus) | 81,308 | 61% | 5% | 62,683 | 161,400 |
| ESM-C second opinion, strict cut-off | 36,565 | 96% | 1% | 107,426 | 18,610 |
| Pathway Tools input | 96,346 | 82% | 8% | 47,645 | 94,011 |

Two methods were corrected before this comparison. DeepEC had been dropping every protein whose
sequence ends in a stop-codon mark, and ESM-C voted in the consensus without a similarity
cut-off; it now votes only above 0.985.

## Why they differ

- **Gene finder.** Same program and mode; the new pipeline runs it on batches of contigs in
  parallel. The genes only one side calls are mostly low-confidence calls.
- **Reference release.** The original searched UniProt 2025_04 (Swiss-Prot, UniRef50, UniRef90).
  The new pipeline's UniRef50 file is a later download (release not recorded), and its
  language-model methods use Swiss-Prot 2026_02. This affects named functions a little.
- **Method.** Most EC differences are here: the new pipeline predicts EC numbers with models
  (CLEAN, DeepEC, EZpred, ProteinBERT, ESM-C) and KEGG profiles, and these predict for proteins
  with no close known relative. They therefore call far more genes than the original.
- **Pathway Tools input.** The original passes product names and lets Pathway Tools match them
  to reactions; the new pipeline passes only CLEAN's EC numbers. This explains most of the
  pathway differences. The original also merges community genes that share a product name; this
  changes gene counts, not the pathway list.

Tables: `per_assembly.tsv`, `ec_agreement.tsv` and `pathways_overlap.tsv` next to this file.

## Recheck after the Pathway Tools input fix (2026-11-21)

Three assemblies were rerun from the gene annotations onward with two changes to what the new
pipeline feeds Pathway Tools. First, every gene gets its best named UniRef50 hit as a product
name, as the original does. Second, CLEAN's EC numbers are kept only at confidence 0.5 or
higher (CLEAN's own high-confidence cut), not 0.02. Overlap is shared pathways divided by
pathways in either database. Pathway counts are original / new before / names only / names
and the 0.5 cut.

| Assembly | MAGs | MAG overlap before | names only | names + 0.5 | Community overlap before | names only | names + 0.5 | Community pathways | Mean MAG pathways |
|---|---|---|---|---|---|---|---|---|---|
| hybridspades_cpe3_r2a_cu7_8 | 4 | 0.49 | 0.57 | 0.63 | 0.54 | 0.59 | 0.63 | 836 / 931 / 1096 / 946 | 472 / 498 / 636 / 537 |
| operams_cpe3_r2a_cu7_8 | 5 | 0.49 | 0.57 | 0.62 | 0.55 | 0.60 | 0.63 | 888 / 962 / 1139 / 992 | 497 / 521 / 662 / 558 |
| shortspades_cpe3_r2a_cu1_5 | 5 | 0.51 | 0.59 | 0.66 | 0.57 | 0.64 | 0.69 | 985 / 1048 / 1256 / 1086 | 465 / 505 / 622 / 526 |

- **Names recover most of the original's pathways, but alone they overshoot.** With names, the
  new pipeline finds about 84% of the original's MAG pathways, up from 68%. But it predicts
  30% more pathways than the original. About three quarters of those extra pathways were
  already present before the change, so the low-confidence CLEAN calls cause them, not the
  names.
- **The 0.5 cut halves CLEAN's EC calls and brings pathway counts back near the original.**
  Per-MAG overlap is 0.62 to 0.66.
- **What still differs (about 100 pathways only in the original and 150 only in the new
  pipeline, per MAG).** The original also searches MetaCyc's own protein database and passes
  the reactions of the hit directly (METACYC lines). It also takes EC numbers from Swiss-Prot
  hits. The new pipeline does neither. It still uses CLEAN's model calls, and its UniRef50
  release is newer.

Rerun outputs are in `data/beth_mp/pffix/outputs/` (`names/` and `floor05/`,
one directory per assembly), with the two driver runs in `data/beth_mp/pffix/runs/`. `pathway_recheck.py` and `pathway_recheck.sbatch` produce them.

## Recheck with MetaPathways 4's own Pathway Tools input (2026-11-22)

The new pipeline now builds the Pathway Tools input by MetaPathways 4.0.3's rules, from the same
four protein searches MetaPathways ran, at the same releases. CLEAN is no longer an input.

- **Searches.** UniRef50, UniRef90 and Swiss-Prot from UniProt 2025_04 (the FASTA files behind
  the original's own indexes), and the MetaCyc 27.0 protein sequences from the lab's licensed
  Pathway Tools. Each is an ordinary annotation lane with up to five hits per gene.
- **Rules, applied when the PathoLogic input is built.** Genes under 60 amino acids are left
  out. A hit counts when its bit score ratio is at least 0.4, its e-value at most 1e-6 and its
  alignment at least 45 amino acids. The product name is the best of the four databases' top
  hits by MetaPathways' annotation score, which counts MetaCyc hits double. EC numbers come from
  the UniProt EC maps for the Swiss-Prot and UniRef hits. The MetaCyc hit's reactions are passed
  directly. Community databases keep one gene per product name, and each MAG gene takes its
  representative's annotation, as in the original.
- **Pathway Tools.** Taxonomic pruning is now on, as in the original. The pathway table has the
  original's columns, read from the database the way the original reads it. On one MAG
  database of the original it reproduces the original's table exactly (357 pathways).

Overlap is shared pathways divided by pathways in either database. Pathway counts are
original / names + 0.5 / faithful.

| Assembly | MAGs | MAG overlap before | names + 0.5 | faithful | Community overlap before | names + 0.5 | faithful | Community pathways | Mean MAG pathways |
|---|---|---|---|---|---|---|---|---|---|
| hybridspades_cpe3_r2a_cu7_8 | 4 | 0.49 | 0.63 | 0.94 | 0.54 | 0.63 | 0.96 | 836 / 946 / 847 | 472 / 537 / 472 |
| operams_cpe3_r2a_cu7_8 | 5 | 0.49 | 0.62 | 0.94 | 0.55 | 0.63 | 0.96 | 888 / 992 / 902 | 497 / 558 / 504 |
| shortspades_cpe3_r2a_cu1_5 | 5 | 0.51 | 0.66 | 0.92 | 0.57 | 0.69 | 0.95 | 985 / 1086 / 985 | 465 / 526 / 467 |

- **The input now matches gene by gene.** On genes both pipelines call identically, the MAG
  inputs carry the same EC numbers for 97 to 98% of genes and the same MetaCyc reactions for 97
  to 98%. The same product name holds for 80 to 94%. Totals agree within 2%: 1,587 / 1,576 EC
  lines and 1,960 / 1,919 MetaCyc lines in hybridspades_cpe3_r2a_cu7_8's community input.
- **What still differs (10 to 20 pathways only in the original and 15 to 22 only in the new
  pipeline, per MAG).**
  - The search program. The original uses FAST, the new pipeline DIAMOND, and the bit score
    ratio's denominator is estimated rather than aligned. The top hit, and so the product name,
    changes for 6 to 20% of genes.
  - The original also names genes from eggNOG and CAZy hits (under 1% of names in
    hybridspades_cpe3_r2a_cu7_8). The new pipeline does not search them for this input.
  - The original's identity cut-off (20%) is not applied, because the annotation table does
    not carry identity. The bit score ratio cut-off makes it bind rarely.
  - The Pathway Tools image. Both are release 27.0, but the original ran its own build with
    SRI's 27.0 patches and attached contig sequences to the database.

The PathoLogic half of this recheck ran outside the workflow engine, through
`faithful_pathologic.sbatch`, with the library's own scripts. In the engine the PathoLogic input
reads the full annotation table, which on these assemblies would rerun lanes this input never
reads. The four searches ran in the engine (`run_port.py --group mp4lanes`). `pf_agreement.py`
compares the two pipelines' inputs gene by gene. Outputs are in
`data/beth_mp/pffix/outputs/faithful/`, one directory per assembly.
