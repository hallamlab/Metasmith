#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(vegan)
  library(permute)
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: Rscript run_lung_status_analysis.R <metadata.tsv> <asv_table.tsv> <outdir> [permutations] [seed]")
}

metadata_file <- args[1]
asv_file <- args[2]
outdir <- args[3]
n_permutations <- if (length(args) >= 4) as.integer(args[4]) else 9999L
analysis_seed <- if (length(args) >= 5) as.integer(args[5]) else 1L
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

empty_result <- function(contrast, comparison_type, n_patients, n_samples, reason) {
  tibble(
    contrast = contrast,
    comparison_type = comparison_type,
    status = "not_estimable",
    reason = reason,
    n_patients = n_patients,
    n_samples = n_samples,
    permanova_R2 = NA_real_,
    permanova_F = NA_real_,
    permanova_p = NA_real_,
    permdisp_F = NA_real_,
    permdisp_p = NA_real_,
    alpha_statistic = NA_real_,
    alpha_p = NA_real_,
    alpha_median_group1 = NA_real_,
    alpha_median_group2 = NA_real_
  )
}

safe_permdisp <- function(distance, groups) {
  tryCatch({
    fit <- betadisper(distance, groups)
    test <- permutest(fit, permutations = n_permutations, pairwise = FALSE)
    c(F = unname(test$tab$F[1]), p = unname(test$tab$`Pr(>F)`[1]))
  }, error = function(e) c(F = NA_real_, p = NA_real_))
}

build_profiles <- function(meta, abundance, patient_col) {
  profile_meta <- meta %>%
    group_by(!!sym(patient_col), lung_status) %>%
    summarise(
      n_samples = n(),
      samples = list(sample),
      shannon_mean = mean(shannon, na.rm = TRUE),
      .groups = "drop"
    )
  profile_matrix <- matrix(0, nrow = nrow(profile_meta), ncol = ncol(abundance))
  rownames(profile_matrix) <- paste(profile_meta[[patient_col]], profile_meta$lung_status, sep = "_")
  colnames(profile_matrix) <- colnames(abundance)
  for (i in seq_len(nrow(profile_meta))) {
    sample_ids <- profile_meta$samples[[i]]
    profile_matrix[i, ] <- colMeans(abundance[sample_ids, , drop = FALSE], na.rm = TRUE)
  }
  list(metadata = profile_meta, matrix = profile_matrix)
}

run_between_contrast <- function(profile_meta, profile_matrix, patient_col, group1, group2, label) {
  subset_meta <- profile_meta %>% filter(lung_status %in% c(group1, group2))
  group_counts <- table(factor(subset_meta$lung_status, levels = c(group1, group2)))
  cat(sprintf("\n=== %s ===\n", label))
  cat(sprintf("  %s: %d; %s: %d\n", group1, group_counts[1], group2, group_counts[2]))
  if (any(group_counts < 2)) {
    reason <- sprintf("Requires at least two patient profiles in each group; observed %s=%d and %s=%d",
                      group1, group_counts[1], group2, group_counts[2])
    cat(sprintf("  SKIPPED: %s\n", reason))
    return(empty_result(label, "between_patient", nrow(subset_meta), nrow(subset_meta), reason))
  }

  keys <- paste(subset_meta[[patient_col]], subset_meta$lung_status, sep = "_")
  distance <- vegdist(profile_matrix[keys, , drop = FALSE], method = "bray")
  set.seed(analysis_seed)
  permanova <- adonis2(distance ~ lung_status, data = subset_meta,
                       permutations = n_permutations, method = "bray")
  dispersion <- safe_permdisp(distance, subset_meta$lung_status)
  alpha <- subset_meta %>% select(lung_status, shannon_mean)
  alpha_test <- tryCatch(
    wilcox.test(shannon_mean ~ lung_status, data = alpha, exact = FALSE),
    error = function(e) NULL
  )
  tibble(
    contrast = label,
    comparison_type = "between_patient",
    status = "estimated",
    reason = NA_character_,
    n_patients = nrow(subset_meta),
    n_samples = nrow(subset_meta),
    permanova_R2 = permanova$R2[1],
    permanova_F = permanova$F[1],
    permanova_p = permanova$`Pr(>F)`[1],
    permdisp_F = dispersion["F"],
    permdisp_p = dispersion["p"],
    alpha_statistic = if (is.null(alpha_test)) NA_real_ else unname(alpha_test$statistic),
    alpha_p = if (is.null(alpha_test)) NA_real_ else alpha_test$p.value,
    alpha_median_group1 = median(alpha$shannon_mean[alpha$lung_status == group1], na.rm = TRUE),
    alpha_median_group2 = median(alpha$shannon_mean[alpha$lung_status == group2], na.rm = TRUE)
  )
}

cat("Loading data...\n")
metadata <- read_tsv(metadata_file, show_col_types = FALSE)
asv_table <- read_tsv(asv_file, show_col_types = FALSE) %>% column_to_rownames("sample")
required_metadata <- c("sample", "lung_status")
missing_metadata <- setdiff(required_metadata, colnames(metadata))
if (length(missing_metadata) > 0) stop(sprintf("Missing metadata columns: %s", paste(missing_metadata, collapse = ", ")))

patient_col <- intersect(c("Participant_ID", "patient_code"), colnames(metadata))[1]
if (is.na(patient_col)) stop("No patient ID column found")
shared_samples <- intersect(metadata$sample, rownames(asv_table))
if (length(shared_samples) == 0) stop("No overlapping samples between metadata and ASV table")
metadata <- metadata %>% filter(sample %in% shared_samples) %>% distinct(sample, .keep_all = TRUE)
metadata <- metadata[match(shared_samples, metadata$sample), , drop = FALSE]
asv_table <- as.matrix(asv_table[shared_samples, , drop = FALSE])
storage.mode(asv_table) <- "numeric"
cat(sprintf("  %d samples in analysis\n", nrow(metadata)))

row_totals <- rowSums(asv_table)
row_totals[row_totals == 0] <- 1
asv_rel <- sweep(asv_table, 1, row_totals, "/")
metadata$shannon <- diversity(asv_table, index = "shannon")

# Contrast A: fully paired status profiles within participant.
paired_meta <- metadata %>% filter(lung_status %in% c("TumorSide", "Contralateral"))
paired_ids <- paired_meta %>%
  group_by(!!sym(patient_col)) %>%
  summarise(has_both = all(c("TumorSide", "Contralateral") %in% lung_status), .groups = "drop") %>%
  filter(has_both) %>% pull(!!sym(patient_col))
cat(sprintf("\n=== A_TumorSide_vs_Contralateral ===\n  %d fully paired patients\n", length(paired_ids)))

pairdist_a <- tibble(patient = character(), sample1 = character(), sample2 = character(),
                     bray = double(), comparison = character())
if (length(paired_ids) < 2) {
  reason_a <- sprintf("Requires at least two fully paired patients; observed %d", length(paired_ids))
  cat(sprintf("  SKIPPED: %s\n", reason_a))
  results_a <- empty_result("A_TumorSide_vs_Contralateral", "paired", length(paired_ids), 0, reason_a)
} else {
  paired_profiles <- build_profiles(
    paired_meta %>% filter(!!sym(patient_col) %in% paired_ids), asv_rel, patient_col
  )
  paired_level <- paired_profiles$metadata
  asv_paired <- paired_profiles$matrix
  paired_distance <- vegdist(asv_paired, method = "bray")
  set.seed(analysis_seed)
  blocked_permutations <- how(nperm = n_permutations, blocks = paired_level[[patient_col]])
  permanova_a <- adonis2(paired_distance ~ lung_status, data = paired_level,
                         permutations = blocked_permutations, method = "bray")
  dispersion_a <- safe_permdisp(paired_distance, paired_level$lung_status)
  alpha_wide <- paired_level %>%
    select(!!sym(patient_col), lung_status, shannon_mean) %>%
    pivot_wider(names_from = lung_status, values_from = shannon_mean)
  alpha_a <- tryCatch(
    wilcox.test(alpha_wide$TumorSide, alpha_wide$Contralateral, paired = TRUE, exact = FALSE),
    error = function(e) NULL
  )
  results_a <- tibble(
    contrast = "A_TumorSide_vs_Contralateral",
    comparison_type = "paired",
    status = "estimated",
    reason = NA_character_,
    n_patients = length(paired_ids),
    n_samples = nrow(paired_level),
    permanova_R2 = permanova_a$R2[1],
    permanova_F = permanova_a$F[1],
    permanova_p = permanova_a$`Pr(>F)`[1],
    permdisp_F = dispersion_a["F"],
    permdisp_p = dispersion_a["p"],
    alpha_statistic = if (is.null(alpha_a)) NA_real_ else unname(alpha_a$statistic),
    alpha_p = if (is.null(alpha_a)) NA_real_ else alpha_a$p.value,
    alpha_median_group1 = median(alpha_wide$TumorSide, na.rm = TRUE),
    alpha_median_group2 = median(alpha_wide$Contralateral, na.rm = TRUE)
  )
  distance_matrix_a <- as.matrix(paired_distance)
  for (participant in paired_ids) {
    key_a <- paste(participant, "TumorSide", sep = "_")
    key_b <- paste(participant, "Contralateral", sep = "_")
    pairdist_a <- bind_rows(pairdist_a, tibble(
      patient = participant, sample1 = key_a, sample2 = key_b,
      bray = distance_matrix_a[key_a, key_b], comparison = "TumorSide_vs_Contralateral"
    ))
  }
}
write_tsv(pairdist_a, file.path(outdir, "contrast_A_pairwise_distances.tsv"))

patient_profiles <- build_profiles(metadata, asv_rel, patient_col)
patient_level <- patient_profiles$metadata
asv_patient <- patient_profiles$matrix
results_b <- run_between_contrast(patient_level, asv_patient, patient_col,
                                  "Contralateral", "Healthy", "B_Contralateral_vs_Healthy")
results_c <- run_between_contrast(patient_level, asv_patient, patient_col,
                                  "TumorSide", "Healthy", "C_TumorSide_vs_Healthy")

all_results <- bind_rows(results_a, results_b, results_c) %>%
  mutate(
    permanova_q = p.adjust(permanova_p, method = "fdr"),
    alpha_q = p.adjust(alpha_p, method = "fdr")
  )
write_tsv(all_results, file.path(outdir, "lung_status_contrasts_summary.tsv"))
write_tsv(patient_level, file.path(outdir, "patient_level_metadata.tsv"))

patient_distance <- vegdist(asv_patient, method = "bray")
write.table(as.matrix(patient_distance), file.path(outdir, "patient_level_bray_distances.tsv"),
            sep = "\t", quote = FALSE)
cat("\n=== Summary ===\n")
print(all_results)
cat(sprintf("\nAnalysis complete. Results saved to %s\n", outdir))
