#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(dplyr)
  library(tidyr)
  library(vegan)
  library(permute)
})

option_list <- list(
  make_option("--data-wide", type = "character", help = "Wide ASV table (rows=ASVs, columns=samples)"),
  make_option("--data-long", type = "character", help = "Long metadata table"),
  make_option("--sample-col", type = "character", default = "sample", help = "Sample ID column in long table"),
  make_option("--patient-col", type = "character", default = "Participant_ID", help = "Patient ID column"),
  make_option("--case-col", type = "character", default = "Case", help = "Case/control column"),
  make_option("--type-col", type = "character", default = "type_group", help = "Sample type column"),
  make_option("--sample-types", type = "character", default = "Oral Rinse,BAL,Lung Brush",
              help = "Comma-separated sample types to include"),
  make_option("--exclude-contralateral-in-cancer", type = "logical", default = TRUE,
              help = "For case/control analyses, exclude contralateral cancer samples [default: %default]"),
  make_option("--contralateral-col", type = "character", default = "lung_status",
              help = "Metadata column identifying contralateral samples [default: %default]"),
  make_option("--cancer-site-col", type = "character", default = "Cancer_Site",
              help = "Metadata column for cancer side (used if contralateral column missing) [default: %default]"),
  make_option("--lung-side-col", type = "character", default = "lung_code",
              help = "Metadata column for sample lung side (used if contralateral column missing) [default: %default]"),
  make_option("--contralateral-value", type = "character", default = "Contralateral",
              help = "Value in contralateral column marking contralateral samples [default: %default]"),
  make_option("--contralateral-sample-types", type = "character", default = "Lung Brush,BAL",
              help = "Comma-separated sample types where contralateral exclusion applies [default: %default]"),
  make_option("--transform", type = "character", default = "none",
              help = "Distance input transform: none (TSS+Bray) or rclr (Euclidean) [default: %default]"),
  make_option("--permutations", type = "integer", default = 9999, help = "Number of permutations"),
  make_option("--seed", type = "integer", default = 42, help = "Random seed"),
  make_option("--require-complete-types", action = "store_true", default = FALSE,
              help = "For sample-type analysis, keep only patients with all listed sample types"),
  make_option("--outdir", type = "character", help = "Output directory")
)

parser <- OptionParser(option_list = option_list)
args <- parse_args(parser)

# Handle optparse name variants across environments (underscore, hyphen, dot)
resolve_arg <- function(arg_list, key) {
  candidates <- c(
    key,
    gsub("_", "-", key),
    gsub("_", ".", key)
  )
  for (nm in candidates) {
    if (!is.null(arg_list[[nm]])) {
      return(arg_list[[nm]])
    }
  }
  NULL
}

args$data_wide <- resolve_arg(args, "data_wide")
args$data_long <- resolve_arg(args, "data_long")
args$sample_col <- resolve_arg(args, "sample_col")
args$patient_col <- resolve_arg(args, "patient_col")
args$case_col <- resolve_arg(args, "case_col")
args$type_col <- resolve_arg(args, "type_col")
args$sample_types <- resolve_arg(args, "sample_types")
args$contralateral_value <- resolve_arg(args, "contralateral_value")
args$contralateral_sample_types <- resolve_arg(args, "contralateral_sample_types")
args$lung_side_col <- resolve_arg(args, "lung_side_col")
args$cancer_site_col <- resolve_arg(args, "cancer_site_col")
args$contralateral_col <- resolve_arg(args, "contralateral_col")
args$exclude_contralateral_in_cancer <- resolve_arg(args, "exclude_contralateral_in_cancer")
args$transform <- resolve_arg(args, "transform")
args$permutations <- resolve_arg(args, "permutations")
args$seed <- resolve_arg(args, "seed")
args$require_complete_types <- resolve_arg(args, "require_complete_types")
args$outdir <- resolve_arg(args, "outdir")

required <- c("data_wide", "data_long", "outdir")
missing <- required[sapply(required, function(x) is.null(args[[x]]))]
if (length(missing) > 0) {
  stop(sprintf("Missing required args: %s", paste(missing, collapse = ", ")))
}
if (!(args$transform %in% c("none", "rclr"))) {
  stop("--transform must be one of: none, rclr")
}

set.seed(args$seed)
dir.create(args$outdir, showWarnings = FALSE, recursive = TRUE)

sample_types_keep_raw <- trimws(strsplit(args$sample_types, ",")[[1]])

canonicalize_sample_type <- function(x) {
  x_clean <- tolower(trimws(x))
  if (x_clean %in% c("oral", "oral rinse", "oral_rinse")) {
    return("Oral Rinse")
  }
  if (x_clean %in% c("bal", "bronchoalveolar lavage")) {
    return("BAL")
  }
  if (x_clean %in% c("lung brush", "bronchial brush", "brochial brush", "brush")) {
    return("Lung Brush")
  }
  x
}

sample_types_keep <- unique(vapply(sample_types_keep_raw, canonicalize_sample_type, character(1)))

normalize_tss <- function(mat) {
  rs <- rowSums(mat)
  rs[rs == 0] <- 1
  mat / rs
}

aggregate_group_means <- function(mat, group_ids) {
  group_ids <- as.character(group_ids)
  keys <- unique(group_ids)
  out <- matrix(0, nrow = length(keys), ncol = ncol(mat))
  rownames(out) <- keys
  colnames(out) <- colnames(mat)

  for (i in seq_along(keys)) {
    idx <- group_ids == keys[[i]]
    out[i, ] <- colMeans(mat[idx, , drop = FALSE], na.rm = TRUE)
  }

  out
}

rclr_transform <- function(mat) {
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

prepare_dist_input <- function(mat, transform) {
  if (transform == "rclr") {
    return(rclr_transform(mat))
  }
  normalize_tss(mat)
}

tidy_adonis <- function(adonis_obj, model_label) {
  out <- as.data.frame(adonis_obj)
  out$term <- rownames(out)
  rownames(out) <- NULL
  out$model <- model_label
  out
}

extract_adonis_term <- function(adonis_obj, term, model_label, extra_cols = list()) {
  tbl <- as.data.frame(adonis_obj)
  if (!(term %in% rownames(tbl))) {
    return(NULL)
  }
  row <- tbl[term, , drop = FALSE]
  out <- data.frame(
    model = model_label,
    term = term,
    Df = row$Df,
    SumOfSqs = row$SumOfSqs,
    R2 = row$R2,
    F = row$F,
    p_value = row[["Pr(>F)"]],
    stringsAsFactors = FALSE
  )
  if (length(extra_cols) > 0) {
    for (nm in names(extra_cols)) {
      out[[nm]] <- extra_cols[[nm]]
    }
  }
  out
}

run_betadisper <- function(dist_obj, grouping, permutations, label, metric_label) {
  grp <- droplevels(as.factor(grouping))
  if (nlevels(grp) < 2) {
    return(data.frame(
      model = label,
      statistic = NA_real_,
      p_value = NA_real_,
      note = "<2 groups",
      stringsAsFactors = FALSE
    ))
  }
  bd <- betadisper(dist_obj, grp)
  ptest <- permutest(bd, permutations = permutations)

  f_col <- grep("^F", colnames(ptest$tab), value = TRUE)
  p_col <- grep("Pr", colnames(ptest$tab), value = TRUE)

  data.frame(
    model = label,
    statistic = as.numeric(ptest$tab[1, f_col[1]]),
    p_value = as.numeric(ptest$tab[1, p_col[1]]),
    note = paste0("PERMDISP on ", metric_label),
    stringsAsFactors = FALSE
  )
}

dist_to_long <- function(dist_obj, meta_df, key_col, vars) {
  m <- as.matrix(dist_obj)
  ids <- rownames(m)
  idx <- which(upper.tri(m), arr.ind = TRUE)
  out <- data.frame(
    id1 = ids[idx[, 1]],
    id2 = ids[idx[, 2]],
    bray = as.numeric(m[idx]),
    stringsAsFactors = FALSE
  )

  map <- meta_df %>%
    select(all_of(c(key_col, vars))) %>%
    distinct()

  i1 <- match(out$id1, map[[key_col]])
  i2 <- match(out$id2, map[[key_col]])

  for (v in vars) {
    out[[paste0(v, "_1")]] <- map[[v]][i1]
    out[[paste0(v, "_2")]] <- map[[v]][i2]
  }
  out
}

cat("============================================================\n")
cat("Bray-Curtis PERMANOVA (Patient-Aware)\n")
cat("============================================================\n")
cat(sprintf("Seed: %d | Permutations: %d\n", args$seed, args$permutations))
cat(sprintf("Requested sample types: %s\n", paste(sample_types_keep_raw, collapse = ", ")))
cat(sprintf("Using sample types: %s\n", paste(sample_types_keep, collapse = ", ")))
metric_method <- ifelse(args$transform == "rclr", "euclidean", "bray")
metric_label <- ifelse(args$transform == "rclr", "Euclidean (rCLR)", "Bray-Curtis (TSS)")
cat(sprintf("Distance metric: %s\n\n", metric_label))

cat("Loading data...\n")
long_df <- read.delim(args$data_long, stringsAsFactors = FALSE, check.names = FALSE)
wide_df <- read.delim(args$data_wide, stringsAsFactors = FALSE, check.names = FALSE)

if ("ASV_ID" %in% colnames(wide_df)) {
  rownames(wide_df) <- wide_df$ASV_ID
  wide_df$ASV_ID <- NULL
} else {
  rownames(wide_df) <- wide_df[[1]]
  wide_df[[1]] <- NULL
}

meta_cols <- unique(c(args$sample_col, args$patient_col, args$case_col, args$type_col,
                     args$contralateral_col, args$cancer_site_col, args$lung_side_col))
meta_cols <- meta_cols[meta_cols %in% colnames(long_df)]

meta <- long_df %>%
  select(all_of(meta_cols)) %>%
  distinct() %>%
  mutate(
    sample_type_raw = .data[[args$type_col]],
    sample_type = vapply(sample_type_raw, canonicalize_sample_type, character(1))
  ) %>%
  filter(sample_type %in% sample_types_keep)

sample_ids <- intersect(meta[[args$sample_col]], colnames(wide_df))
if (length(sample_ids) == 0) {
  stop("No overlapping sample IDs between metadata and wide table.")
}

meta <- meta %>%
  filter(.data[[args$sample_col]] %in% sample_ids) %>%
  mutate(
    case_status = ifelse(.data[[args$case_col]] %in% c("Control", "Non-Cancer"), "Control", "Cancer"),
    patient_id = .data[[args$patient_col]],
    sample_id = .data[[args$sample_col]]
  )

# Keep one metadata row per sample and align to count matrix
meta <- meta %>%
  group_by(sample_id) %>%
  slice(1) %>%
  ungroup()

sample_ids <- intersect(meta$sample_id, colnames(wide_df))
meta <- meta %>%
  filter(sample_id %in% sample_ids)

contra_types <- trimws(strsplit(args$contralateral_sample_types, ",", fixed = TRUE)[[1]])
contra_types <- contra_types[contra_types != ""]

if (isTRUE(args$exclude_contralateral_in_cancer)) {
  contralateral_col_eff <- args$contralateral_col
  if (!(contralateral_col_eff %in% colnames(meta)) &&
      all(c(args$cancer_site_col, args$lung_side_col) %in% colnames(meta))) {
    meta <- meta %>%
      mutate(
        .derived_lung_status = case_when(
          as.character(.data[[args$case_col]]) %in% c("Control", "Non-Cancer") ~ "Healthy",
          !is.na(.data[[args$cancer_site_col]]) & !is.na(.data[[args$lung_side_col]]) &
            toupper(substr(as.character(.data[[args$cancer_site_col]]), 1, 1)) ==
              toupper(substr(as.character(.data[[args$lung_side_col]]), 1, 1)) ~ "TumorSide",
          TRUE ~ "Contralateral"
        )
      )
    contralateral_col_eff <- ".derived_lung_status"
  }

  if (contralateral_col_eff %in% colnames(meta)) {
    is_cancer_row <- !as.character(meta[[args$case_col]]) %in% c("Control", "Non-Cancer")
    is_contra_row <- as.character(meta[[contralateral_col_eff]]) == as.character(args$contralateral_value)
    in_target_type <- as.character(meta[[args$type_col]]) %in% contra_types
    keep_case_rows <- !(is_cancer_row & is_contra_row & in_target_type)
    removed_n <- sum(!keep_case_rows, na.rm = TRUE)
    if (removed_n > 0) {
      cat(sprintf("Excluding %d contralateral cancer sample(s) for case/control analyses.\n", removed_n))
    }
    meta <- meta[keep_case_rows, , drop = FALSE]
    sample_ids <- intersect(meta$sample_id, colnames(wide_df))
    meta <- meta %>% filter(sample_id %in% sample_ids)
  } else {
    warning("Contralateral exclusion requested but no contralateral metadata available.")
  }
}

count_matrix <- t(as.matrix(wide_df[, sample_ids, drop = FALSE]))
storage.mode(count_matrix) <- "numeric"
rownames(count_matrix) <- sample_ids
rel_matrix <- normalize_tss(count_matrix)

meta <- meta[match(sample_ids, meta$sample_id), ]
if (!all(rownames(count_matrix) == meta$sample_id)) {
  stop("Sample alignment failed.")
}

cat(sprintf("Input samples: %d\n", nrow(count_matrix)))
cat(sprintf("Patients: %d\n", length(unique(meta$patient_id))))
cat(sprintf("Cases: %d Cancer / %d Control\n\n",
            sum(meta$case_status == "Cancer"), sum(meta$case_status == "Control")))

# Aggregate repeated samples within patient x sample_type
pt_key <- paste(meta$patient_id, meta$sample_type, sep = "||")
counts_patient_type <- aggregate_group_means(rel_matrix, pt_key)

pt_meta <- meta %>%
  mutate(pt_key = pt_key) %>%
  group_by(pt_key, patient_id, sample_type, case_status) %>%
  summarise(n_samples_aggregated = n(), .groups = "drop")

pt_meta <- pt_meta[match(rownames(counts_patient_type), pt_meta$pt_key), ]
if (!all(rownames(counts_patient_type) == pt_meta$pt_key)) {
  stop("Patient-type alignment failed.")
}

# -------------------- Part A: Sample type effect (within-patient) --------------------
cat("Running sample-type PERMANOVA (within-patient constrained permutations)...\n")

pt_for_type <- pt_meta
counts_for_type <- counts_patient_type

if (args$require_complete_types) {
  eligible <- pt_for_type %>%
    group_by(patient_id) %>%
    summarise(n_types = n_distinct(sample_type), .groups = "drop") %>%
    filter(n_types == length(sample_types_keep)) %>%
    pull(patient_id)

  keep <- pt_for_type$patient_id %in% eligible
  pt_for_type <- pt_for_type[keep, ]
  counts_for_type <- counts_for_type[keep, , drop = FALSE]
} else {
  eligible <- pt_for_type %>%
    group_by(patient_id) %>%
    summarise(n_types = n_distinct(sample_type), .groups = "drop") %>%
    filter(n_types >= 2) %>%
    pull(patient_id)

  keep <- pt_for_type$patient_id %in% eligible
  pt_for_type <- pt_for_type[keep, ]
  counts_for_type <- counts_for_type[keep, , drop = FALSE]
}

if (nrow(counts_for_type) < 4) {
  stop("Too few rows for sample-type analysis after filtering.")
}

counts_for_type_dist <- prepare_dist_input(counts_for_type, args$transform)
dist_type <- vegdist(counts_for_type_dist, method = metric_method)

perm_type <- how(nperm = args$permutations, blocks = pt_for_type$patient_id)
adonis_type <- adonis2(dist_type ~ sample_type, data = pt_for_type, permutations = perm_type, by = "margin")

global_type_tidy <- tidy_adonis(adonis_type, "sample_type_within_patient")
write.table(global_type_tidy,
            file = file.path(args$outdir, "bray_permanova_sample_type_global.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

pairwise_type <- list()
all_pairs <- combn(sort(unique(pt_for_type$sample_type)), 2, simplify = FALSE)

for (pair in all_pairs) {
  pair_name <- paste(pair, collapse = "_vs_")
  sub_meta <- pt_for_type %>% filter(sample_type %in% pair)

  keep_patients <- sub_meta %>%
    group_by(patient_id) %>%
    summarise(n_types = n_distinct(sample_type), .groups = "drop") %>%
    filter(n_types == 2) %>%
    pull(patient_id)

  sub_meta <- sub_meta %>% filter(patient_id %in% keep_patients)
  if (nrow(sub_meta) < 4 || length(unique(sub_meta$sample_type)) < 2) {
    next
  }

  sub_counts <- counts_for_type[sub_meta$pt_key, , drop = FALSE]
  sub_dist_input <- prepare_dist_input(sub_counts, args$transform)
  sub_dist <- vegdist(sub_dist_input, method = metric_method)
  sub_perm <- how(nperm = args$permutations, blocks = sub_meta$patient_id)

  sub_adonis <- adonis2(sub_dist ~ sample_type, data = sub_meta, permutations = sub_perm, by = "margin")
  row <- extract_adonis_term(
    sub_adonis,
    term = "sample_type",
    model_label = "sample_type_pairwise",
    extra_cols = list(
      group1 = pair[1],
      group2 = pair[2],
      n_patients = length(unique(sub_meta$patient_id))
    )
  )
  if (!is.null(row)) {
    pairwise_type[[pair_name]] <- row
  }
}

pairwise_type_df <- bind_rows(pairwise_type)
if (nrow(pairwise_type_df) > 0) {
  pairwise_type_df$q_value <- p.adjust(pairwise_type_df$p_value, method = "BH")
}
write.table(pairwise_type_df,
            file = file.path(args$outdir, "bray_permanova_sample_type_pairwise.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

sample_type_dist_long <- dist_to_long(
  dist_type,
  pt_for_type %>% distinct(pt_key, patient_id, sample_type, case_status),
  key_col = "pt_key",
  vars = c("patient_id", "sample_type", "case_status")
) %>%
  filter(patient_id_1 == patient_id_2, sample_type_1 != sample_type_2) %>%
  mutate(
    type_a = pmin(sample_type_1, sample_type_2),
    type_b = pmax(sample_type_1, sample_type_2),
    comparison = paste(type_a, "vs", type_b)
  )

write.table(sample_type_dist_long,
            file = file.path(args$outdir, "bray_sample_type_within_patient_pairdist.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

# -------------------- Part B: Cancer vs control effects (between-patient) --------------------
cat("Running cancer/control PERMANOVA at patient level (pooled and by sample type)...\n")

# Pooled patient profile across all included sample types
counts_patient <- aggregate_group_means(rel_matrix, meta$patient_id)
patient_case <- meta %>%
  select(patient_id, case_status) %>%
  distinct() %>%
  group_by(patient_id) %>%
  slice(1) %>%
  ungroup()

patient_case <- patient_case[match(rownames(counts_patient), patient_case$patient_id), ]
if (!all(rownames(counts_patient) == patient_case$patient_id)) {
  stop("Patient-level alignment failed.")
}
rownames(patient_case) <- patient_case$patient_id

counts_patient_dist <- prepare_dist_input(counts_patient, args$transform)
dist_patient <- vegdist(counts_patient_dist, method = metric_method)
adonis_case_pooled <- adonis2(dist_patient ~ case_status,
                              data = patient_case,
                              permutations = args$permutations,
                              by = "margin")

case_pooled_df <- extract_adonis_term(
  adonis_case_pooled,
  term = "case_status",
  model_label = "case_status_patient_pooled",
  extra_cols = list(
    n_patients = nrow(patient_case),
    n_cancer = sum(patient_case$case_status == "Cancer"),
    n_control = sum(patient_case$case_status == "Control")
  )
)
write.table(case_pooled_df,
            file = file.path(args$outdir, "bray_permanova_case_patient_pooled.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

case_pooled_dist_long <- dist_to_long(
  dist_patient,
  patient_case %>% distinct(patient_id, case_status),
  key_col = "patient_id",
  vars = c("case_status")
) %>%
  mutate(
    group_pair = ifelse(
      case_status_1 == case_status_2,
      paste0(case_status_1, "-", case_status_2),
      "Cancer-Control"
    )
  )

write.table(case_pooled_dist_long,
            file = file.path(args$outdir, "bray_case_patient_pairdist.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

# Per-sample-type patient-level case/control models
case_by_type <- list()
case_by_type_dist <- list()
for (stype in sort(unique(pt_meta$sample_type))) {
  sub_meta <- pt_meta %>% filter(sample_type == stype)
  sub_counts <- counts_patient_type[sub_meta$pt_key, , drop = FALSE]

  # Ensure one row per patient for this sample type
  sub_meta <- sub_meta %>%
    group_by(patient_id, sample_type, case_status) %>%
    summarise(pt_key = first(pt_key), .groups = "drop")
  sub_counts <- sub_counts[sub_meta$pt_key, , drop = FALSE]
  rownames(sub_counts) <- sub_meta$pt_key

  if (length(unique(sub_meta$case_status)) < 2) {
    next
  }

  sub_dist_input <- prepare_dist_input(sub_counts, args$transform)
  sub_dist <- vegdist(sub_dist_input, method = metric_method)
  sub_adonis <- adonis2(sub_dist ~ case_status,
                        data = sub_meta,
                        permutations = args$permutations,
                        by = "margin")

  row <- extract_adonis_term(
    sub_adonis,
    term = "case_status",
    model_label = "case_status_within_sample_type",
    extra_cols = list(
      sample_type = stype,
      n_patients = nrow(sub_meta),
      n_cancer = sum(sub_meta$case_status == "Cancer"),
      n_control = sum(sub_meta$case_status == "Control")
    )
  )
  if (!is.null(row)) {
    case_by_type[[stype]] <- row
  }

  dlong <- dist_to_long(
    sub_dist,
    sub_meta %>% distinct(pt_key, patient_id, case_status),
    key_col = "pt_key",
    vars = c("case_status", "patient_id")
  ) %>%
    mutate(
      sample_type = stype,
      group_pair = ifelse(
        case_status_1 == case_status_2,
        paste0(case_status_1, "-", case_status_2),
        "Cancer-Control"
      )
    )
  case_by_type_dist[[stype]] <- dlong
}

case_by_type_df <- bind_rows(case_by_type)
if (nrow(case_by_type_df) > 0) {
  case_by_type_df$q_value <- p.adjust(case_by_type_df$p_value, method = "BH")
}
write.table(case_by_type_df,
            file = file.path(args$outdir, "bray_permanova_case_by_sample_type.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

case_by_type_dist_df <- bind_rows(case_by_type_dist)
write.table(case_by_type_dist_df,
            file = file.path(args$outdir, "bray_case_by_sample_type_pairdist.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

# -------------------- Dispersion diagnostics --------------------
cat("Running dispersion diagnostics (PERMDISP)...\n")

dispersion_rows <- bind_rows(
  run_betadisper(dist_type, pt_for_type$sample_type, args$permutations, "sample_type_within_patient", metric_label),
  run_betadisper(dist_patient, patient_case$case_status, args$permutations, "case_status_patient_pooled", metric_label)
)

for (stype in sort(unique(pt_meta$sample_type))) {
  sub_meta <- pt_meta %>% filter(sample_type == stype)
  sub_counts <- counts_patient_type[sub_meta$pt_key, , drop = FALSE]
  sub_meta <- sub_meta %>%
    group_by(patient_id, sample_type, case_status) %>%
    summarise(pt_key = first(pt_key), .groups = "drop")
  sub_counts <- sub_counts[sub_meta$pt_key, , drop = FALSE]

  if (length(unique(sub_meta$case_status)) < 2) {
    next
  }

  sub_dist <- vegdist(prepare_dist_input(sub_counts, args$transform), method = metric_method)
  tmp <- run_betadisper(sub_dist, sub_meta$case_status, args$permutations,
                        paste0("case_status_", stype), metric_label)
  dispersion_rows <- bind_rows(dispersion_rows, tmp)
}

write.table(dispersion_rows,
            file = file.path(args$outdir, "bray_permdisp_diagnostics.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

# -------------------- Cohort summary --------------------
summary_df <- bind_rows(
  meta %>%
    distinct(sample_id, patient_id, case_status, sample_type) %>%
    count(sample_type, case_status, name = "n_samples") %>%
    mutate(level = "sample"),
  pt_meta %>%
    distinct(patient_id, case_status, sample_type) %>%
    count(sample_type, case_status, name = "n_patients") %>%
    mutate(level = "patient_type")
)

write.table(summary_df,
            file = file.path(args$outdir, "bray_analysis_counts_summary.tsv"),
            sep = "\t", quote = FALSE, row.names = FALSE)

cat("\nSaved outputs:\n")
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_permanova_sample_type_global.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_permanova_sample_type_pairwise.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_permanova_case_patient_pooled.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_permanova_case_by_sample_type.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_sample_type_within_patient_pairdist.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_case_patient_pairdist.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_case_by_sample_type_pairdist.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_permdisp_diagnostics.tsv")))
cat(sprintf("  - %s\n", file.path(args$outdir, "bray_analysis_counts_summary.tsv")))
cat("\nDone.\n")
