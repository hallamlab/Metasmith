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
