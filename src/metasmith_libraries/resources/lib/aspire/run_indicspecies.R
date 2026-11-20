#!/usr/bin/env Rscript
# ASPIRE's indicator species analysis, one multipatt run per label of the sample sheet.
#
# Upstream's run_indicspecies.R (research/aspire/upstream/ASPIRE/processes/indicspecies) with
# only its generic per-label path kept. Its status analysis, which pooled a case/control
# label within each sample type and patient, and its contralateral exclusions are removed:
# both keyed on one study's column names. The helper functions, the output file names and
# the per-label test are upstream's, unchanged. A label with fewer than two levels of
# --min-n samples each is skipped, and the skip is recorded in skipped_labels.tsv.
#
# --exhaustive-levels is new. duleg=FALSE tests every combination of a label's levels,
# 2^k - 1 of them, so a label with many levels does not finish: 15 levels over 189 samples
# outran a 12-hour task. Past --exhaustive-levels levels a label gets the single-group test,
# linear in k, under both output names. Unset keeps upstream's combination test.
suppressPackageStartupMessages({
  library(optparse)
  library(dplyr)
  library(purrr)
  library(readr)
  library(tibble)
  library(tidyr)
  library(indicspecies)
  library(permute)  # for how()
})

option_list <- list(
  make_option("--data-wide",  type="character", help="ASV count table (rows=ASVs, cols=samples). TSV."),
  make_option("--data-long",  type="character", help="Sample metadata, one row per sample. TSV."),
  make_option("--sample-col", type="character", default="sample",
              help="Sample id column in --data-long [default: %default]"),
  make_option("--group-cols", type="character",
              help="Comma-separated label columns to test. '+' inside one entry combines columns into one factor."),
  make_option("--block-col", type="character", default=NULL,
              help="Optional column whose values block the permutations, e.g. a subject with repeated samples"),
  make_option("--transform",  type="character", default="none",
              help="Matrix transform before ISA: none or rclr [default: %default]"),
  make_option("--perms",      type="integer",   default=9999,
              help="Number of permutations [default: %default]"),
  make_option("--seed",       type="integer",   default=42,
              help="Random seed [default: %default]"),
  make_option("--q-threshold", type="double", default=0.05,
              help="FDR threshold for the significant flag [default: %default]"),
  make_option("--min-n",      type="integer",   default=2,
              help="Minimum samples per level; smaller levels are dropped [default: %default]"),
  make_option("--exhaustive-levels", type="integer", default=NA,
              help="Most levels a label may have and still be tested in level combinations [default: any]"),
  make_option("--outdir",     type="character", help="Output directory")
)

parser <- OptionParser(
  usage = "%prog --data-wide ASV_wide.tsv --data-long metadata.tsv --sample-col sample --group-cols culture --outdir out",
  description = "Run indicspecies multipatt once per label.",
  option_list = option_list
)
opt <- parse_args(parser)

required <- c("data-wide", "data-long", "group-cols", "outdir")
missing <- required[sapply(required, function(x) is.null(opt[[x]]))]
if (length(missing)) {
  cat("Missing required option(s):", paste(missing, collapse=", "), "\n\n", file=stderr())
  print_help(parser)
  quit(status=2)
}
if (!is.finite(opt$`q-threshold`) || opt$`q-threshold` < 0 || opt$`q-threshold` > 1) {
  stop("--q-threshold must be a finite value between 0 and 1")
}
set.seed(opt$seed)

outdir <- opt$outdir
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

parse_cli_csv <- function(x) {
  if (is.null(x) || !nzchar(x)) {
    return(character(0))
  }
  strsplit(x, ",", fixed = TRUE)[[1]] |>
    trimws() |>
    discard(~ .x == "")
}

make_grouping_factor <- function(meta_df, spec) {
  cols <- strsplit(spec, "\\+", perl = TRUE)[[1]] |>
    trimws() |>
    discard(~ .x == "")

  if (length(cols) == 0) {
    stop("Empty grouping spec: ", spec)
  }

  missing_cols <- setdiff(cols, colnames(meta_df))
  if (length(missing_cols) > 0) {
    stop(
      "Grouping spec '", spec, "' refers to missing metadata column(s): ",
      paste(missing_cols, collapse = ", ")
    )
  }

  if (length(cols) == 1) {
    return(as.factor(meta_df[[cols]]))
  }

  parts <- meta_df %>%
    select(all_of(cols)) %>%
    mutate(across(everything(), as.character))

  ok <- complete.cases(parts)
  combined <- rep(NA_character_, nrow(parts))

  combined[ok] <- do.call(
    interaction,
    c(
      as.data.frame(parts[ok, , drop = FALSE]),
      list(sep = "__", drop = TRUE, lex.order = TRUE)
    )
  ) |> as.character()

  factor(combined)
}

make_group_slug <- function(spec) {
  gsub("[^A-Za-z0-9]+", "_", spec)
}

make_group_slug <- function(spec) {
  gsub("[^A-Za-z0-9]+", "_", spec)
}

group_specs <- parse_cli_csv(opt$`group-cols`)
group_cols <- unique(unlist(strsplit(group_specs, "\\+", perl = TRUE)))
block_col <- if (!is.null(opt$`block-col`) && nzchar(opt$`block-col`)) opt$`block-col` else NULL

long_df <- read_tsv(opt$`data-long`, show_col_types = FALSE)
all_cols <- unique(c(opt$`sample-col`, group_cols, block_col))
missing_cols <- setdiff(all_cols, names(long_df))
if (length(missing_cols) > 0) {
  stop("Missing columns in metadata: ", paste(missing_cols, collapse = ", "))
}
meta <- long_df %>%
  select(all_of(all_cols)) %>%
  distinct() %>%
  tibble::column_to_rownames(opt$`sample-col`)

asv <- read_tsv(opt$`data-wide`, show_col_types = FALSE)
stopifnot(ncol(asv) >= 2)
asv <- asv %>% rename(ASV = 1)
asv_mat <- asv %>% column_to_rownames("ASV") %>% as.matrix()
mode(asv_mat) <- "numeric"

common <- intersect(colnames(asv_mat), rownames(meta))
if (length(common) == 0) stop("No overlapping samples between ASV table columns and metadata rows.")
asv_mat <- asv_mat[, common, drop = FALSE]
meta    <- meta[common, , drop = FALSE]
message("ASV table dimensions: ", paste(dim(asv_mat), collapse = " x "))

if (!(opt$transform %in% c("none", "rclr"))) {
  stop("--transform must be one of: none, rclr")
}

apply_matrix_transform <- function(mat, method = "none") {
  method <- tolower(method)
  if (method == "none") {
    return(mat)
  }
  if (method != "rclr") {
    stop("Unsupported transform: ", method)
  }
  out <- matrix(0, nrow = nrow(mat), ncol = ncol(mat), dimnames = dimnames(mat))
  for (i in seq_len(nrow(mat))) {
    v <- as.numeric(mat[i, ])
    pos <- !is.na(v) & v > 0
    if (any(pos)) {
      lv <- log(v[pos])
      out[i, pos] <- lv - mean(lv)
    }
  }
  out
}

run_indics <- function(X_samples_by_features, grouping, perms = 9999, duleg = FALSE, patient_blocks = NULL) {
  # indicspecies::multipatt expects samples in rows, species/features in columns
  # If patient_blocks provided, use blocked permutations (for within-patient comparisons)
  if (!is.null(patient_blocks)) {
    message("  Using blocked permutations (patient as blocking factor)")
    ctrl <- how(nperm = perms, blocks = patient_blocks)
  } else {
    ctrl <- how(nperm = perms)
  }
  suppressWarnings({
    multipatt(x = X_samples_by_features, cluster = grouping, duleg = duleg, control = ctrl)
  })
}

summarize_multipatt <- function(fit) {
  # Build a tidy data.frame with sign + A + B + q-values if p.value present
  sign_df <- as.data.frame(fit$sign)
  sign_df <- sign_df %>%
    rownames_to_column("ASV")

  A_df <- as.data.frame(fit$A) %>% rownames_to_column("ASV")
  B_df <- as.data.frame(fit$B) %>% rownames_to_column("ASV")

  out <- sign_df %>%
    left_join(A_df, by = "ASV", suffix = c("", ".A")) %>%
    left_join(B_df, by = "ASV", suffix = c("", ".B"))

  # If p.value present, add FDR (q) and significance flag
  if ("p.value" %in% names(out)) {
    out <- out %>%
      mutate(q.value = p.adjust(.data[["p.value"]], method = "fdr"),
             significant = q.value < opt$`q-threshold`)
  }
  out
}

write_tables <- function(df_sign_only, df_full, base) {
  drop_full_union_patterns <- function(df) {
    s_cols <- grep("^s\\.", names(df), value = TRUE)
    if (length(s_cols) == 0 || nrow(df) == 0) {
      return(df)
    }
    # Non-informative union pattern: feature associated with all groups.
    is_full_union <- rowSums(df[, s_cols, drop = FALSE], na.rm = TRUE) == length(s_cols)
    df[!is_full_union, , drop = FALSE]
  }

  df_sign_only <- drop_full_union_patterns(df_sign_only)
  df_full <- drop_full_union_patterns(df_full)

  out_results <- file.path(outdir, paste0(base, "_results.tsv"))
  out_summary <- file.path(outdir, paste0(base, "_summary.tsv"))
  readr::write_tsv(df_sign_only, out_results)
  readr::write_tsv(df_full, out_summary)

  # Backward-compatible alias for historical DULEG naming: *_results_DULEG.tsv
  if (grepl("_DULEG$", base)) {
    base_legacy <- sub("_DULEG$", "", base)
    out_results_legacy <- file.path(outdir, paste0(base_legacy, "_results_DULEG.tsv"))
    out_summary_legacy <- file.path(outdir, paste0(base_legacy, "_summary_DULEG.tsv"))
    readr::write_tsv(df_sign_only, out_results_legacy)
    readr::write_tsv(df_full, out_summary_legacy)
  }
}

skipped <- tibble(label = character(0), reason = character(0))

for (gcol in group_specs) {
  grouping <- make_grouping_factor(meta, gcol)
  gcol_slug <- make_group_slug(gcol)

  keep_idx <- !is.na(grouping)
  grouping <- droplevels(grouping[keep_idx])
  X <- t(asv_mat[, keep_idx, drop = FALSE])
  meta_keep <- meta[keep_idx, , drop = FALSE]

  tab <- table(grouping)
  small <- names(tab[tab < opt$`min-n`])
  if (length(small) > 0) {
    message("Dropping levels of '", gcol, "' with < ", opt$`min-n`, " samples: ",
            paste(small, collapse = ", "))
    keep_idx2 <- !(grouping %in% small)
    grouping <- droplevels(grouping[keep_idx2])
    X <- X[keep_idx2, , drop = FALSE]
    meta_keep <- meta_keep[keep_idx2, , drop = FALSE]
  }

  if (length(unique(grouping)) < 2) {
    message("Label '", gcol, "' has fewer than two levels after filtering; skipping.")
    skipped <- bind_rows(skipped, tibble(label = gcol, reason = "fewer than two levels"))
    next
  }

  blocks <- NULL
  if (!is.null(block_col)) {
    blocks <- droplevels(factor(as.character(meta_keep[[block_col]])))
  }
  X <- apply_matrix_transform(X, opt$transform)

  single <- !is.na(opt$`exhaustive-levels`) && nlevels(grouping) > opt$`exhaustive-levels`
  if (single) {
    message("'", gcol, "' has ", nlevels(grouping), " levels; single groups only (duleg=TRUE)")
    fit2 <- run_indics(X, grouping, perms = opt$perms, duleg = TRUE, patient_blocks = blocks)
    fit1 <- fit2
  } else {
    message("Running multipatt for '", gcol, "' (duleg=FALSE)")
    fit1 <- run_indics(X, grouping, perms = opt$perms, duleg = FALSE, patient_blocks = blocks)
    message("Running multipatt for '", gcol, "' (duleg=TRUE)")
    fit2 <- run_indics(X, grouping, perms = opt$perms, duleg = TRUE, patient_blocks = blocks)
  }
  write_tables(as.data.frame(fit1$sign) %>% rownames_to_column("ASV"), summarize_multipatt(fit1),
               paste0(gcol_slug, "_indicator_species"))
  write_tables(as.data.frame(fit2$sign) %>% rownames_to_column("ASV"), summarize_multipatt(fit2),
               paste0(gcol_slug, "_indicator_species_DULEG"))
}

readr::write_tsv(skipped, file.path(outdir, "skipped_labels.tsv"))
message("Done. Results in: ", outdir)
