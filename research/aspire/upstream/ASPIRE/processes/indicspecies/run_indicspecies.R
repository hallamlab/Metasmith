#!/usr/bin/env Rscript
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

# ---------- CLI ----------
option_list <- list(
  make_option("--data-wide",  type="character", help="ASV count table (rows=ASVs, cols=samples). TSV."),
  make_option("--data-long",  type="character", help="Long-format data with metadata (for extracting sample metadata)."),
  # Backward-compatible aliases used by current ASPIRE invocations.
  make_option("--asv",        type="character", help="Alias of --data-wide (legacy)."),
  make_option("--meta",       type="character", help="Alias of --data-long (legacy)."),
  make_option("--sample-col", type="character", default="sample",
              help="Column in metadata matching sample IDs [default: %default]"),
  make_option("--patient-col", type="character", default="Participant_ID",
              help="Column for patient IDs (for blocked permutations) [default: %default]"),
  make_option("--block-col", type="character", default=NULL,
              help="Explicit blocking column for restricted permutations; overrides implicit patient blocking when provided [default: none]"),
  make_option("--group-cols", type="character", default="status,type_group",
              help="Comma-separated grouping analyses to run. Use '+' inside one entry to combine metadata columns into one grouping factor, e.g. 'status,type_group,Depth_bin+O2_bin' [default: %default]"),
  make_option("--blocked-cols", type="character", default="type_group",
              help="Comma-separated grouping analyses requiring blocked permutations. Entries must match --group-cols specs, including composite specs like 'Depth_bin+O2_bin' [default: %default]"),
  make_option("--stratified-isa", type="character", default="",
              help="Semicolon-separated stratified ISA specs as within_col::group_col or within_col::group_col::level1|level2. Example: Type_Group::Case::BAL|Bronchial Brush [default: none]"),
  make_option("--status-extra-no-contralateral", type="logical", default=TRUE,
              help="For Lung Brush status analysis, add extra run excluding contralateral cancer samples [default: %default]"),
  make_option("--status-exclude-contralateral", type="logical", default=TRUE,
              help="Exclude contralateral cancer samples for selected status sample types [default: %default]"),
  make_option("--status-contralateral-sites", type="character", default="Lung Brush,BAL",
              help="Comma-separated type_group values where contralateral exclusion applies [default: %default]"),
  make_option("--contralateral-col", type="character", default="lung_status",
              help="Metadata column identifying contralateral samples [default: %default]"),
  make_option("--cancer-site-col", type="character", default="Cancer_Site",
              help="Metadata column giving cancer side for cancer patients (used to derive contralateral if needed) [default: %default]"),
  make_option("--lung-side-col", type="character", default="lung_code",
              help="Metadata column giving sample lung side (used to derive contralateral if needed) [default: %default]"),
  make_option("--contralateral-value", type="character", default="Contralateral",
              help="Value in --contralateral-col marking contralateral samples [default: %default]"),
  make_option("--cancer-label", type="character", default="Cancer",
              help="Label in status column for cancer samples [default: %default]"),
  make_option("--lung-brush-label", type="character", default="Lung Brush",
              help="Value in type_group identifying Lung Brush samples [default: %default]"),
  make_option("--transform",  type="character", default="none",
              help="Abundance transform before multipatt: none|rclr [default: %default]"),
  make_option("--perms",      type="integer",   default=9999,
              help="Permutations for multipatt [default: %default]"),
  make_option("--seed",       type="integer",   default=42,
              help="Random seed for ISA permutation tests [default: %default]"),
  make_option("--q-threshold", type="double", default=0.05,
              help="FDR q-value threshold used to set the significant column [default: %default]"),
  make_option("--min-n",      type="integer",   default=2,
              help="Minimum samples per group to keep [default: %default]"),
  make_option("--type-group-require-complete", type="logical", default=FALSE,
              help="For type_group ISA, only include patients with all sample types (stricter within-patient analysis) [default: %default]"),
  make_option("--outdir",     type="character",
              help="Output directory (will create '<outdir>/indicspecies').")
)

parser <- OptionParser(
  usage = "%prog --data-wide ASV_wide.tsv --data-long ASV_long.tsv --sample-col sample --group-cols status,type_group --outdir out_dir",
  description = "Run indicspecies multipatt on ASV + metadata tables.",
  option_list = option_list
)

opt <- parse_args(parser)

# Support legacy flags used by existing ASPIRE workflows.
if (is.null(opt$`data-wide`) && !is.null(opt$asv)) {
  opt$`data-wide` <- opt$asv
}
if (is.null(opt$`data-long`) && !is.null(opt$meta)) {
  opt$`data-long` <- opt$meta
}

# Enforce required options
required <- c("data-wide", "data-long", "outdir")
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

outdir <- file.path(opt$outdir, "indicspecies")
dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

parse_cli_csv <- function(x) {
  if (is.null(x) || !nzchar(x)) {
    return(character(0))
  }
  strsplit(x, ",", fixed = TRUE)[[1]] |>
    trimws() |>
    discard(~ .x == "")
}

expand_group_spec_cols <- function(specs) {
  specs |>
    strsplit("\\+", perl = TRUE) |>
    unlist() |>
    trimws() |>
    discard(~ .x == "") |>
    unique()
}

parse_stratified_isa <- function(x) {
  if (is.null(x) || !nzchar(trimws(x))) {
    return(list())
  }

  specs <- strsplit(x, ";", fixed = TRUE)[[1]] |>
    trimws() |>
    discard(~ .x == "")

  lapply(specs, function(spec) {
    parts <- strsplit(spec, "::", fixed = TRUE)[[1]] |>
      trimws()
    if (!(length(parts) %in% c(2, 3)) || any(parts[1:2] == "")) {
      stop(
        "Invalid --stratified-isa spec '", spec,
        "'. Expected within_col::group_col or within_col::group_col::level1|level2."
      )
    }
    levels <- character(0)
    if (length(parts) == 3 && nzchar(parts[3])) {
      levels <- strsplit(parts[3], "|", fixed = TRUE)[[1]] |>
        trimws() |>
        discard(~ .x == "")
    }
    list(within_col = parts[1], group_col = parts[2], levels = levels)
  })
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

# ---------- IO ----------
message("Reading long format data: ", opt$`data-long`)
long_df <- read_tsv(opt$`data-long`, show_col_types = FALSE)

# Extract metadata (unique sample-level records)
group_specs <- parse_cli_csv(opt$`group-cols`)
blocked_specs <- parse_cli_csv(opt$`blocked-cols`)
group_cols <- expand_group_spec_cols(group_specs)
stratified_specs <- parse_stratified_isa(opt$`stratified-isa`)
stratified_cols <- unique(unlist(lapply(stratified_specs, function(spec) {
  c(spec$within_col, spec$group_col)
}), use.names = FALSE))

if ("status" %in% group_specs && !("type_group" %in% group_cols)) {
  # Needed for status stratification by sample type.
  group_cols <- c(group_cols, "type_group")
}

required_cols <- c(opt$`sample-col`, group_cols, stratified_cols)
if ("status" %in% group_specs) {
  # Status ISA aggregates within patient before testing between-status differences.
  required_cols <- c(required_cols, opt$`patient-col`)
}
if (length(stratified_specs) > 0) {
  # Stratified ISA aggregates replicate samples to the patient level when possible.
  required_cols <- c(required_cols, opt$`patient-col`)
}
if (!is.null(opt$`block-col`) && nzchar(opt$`block-col`)) {
  # Explicit blocking is a hard requirement when requested.
  required_cols <- c(required_cols, opt$`block-col`)
}

optional_cols <- c()
derive_contralateral_from_sides <- FALSE
if (isTRUE(opt$`status-extra-no-contralateral`) && ("status" %in% group_specs)) {
  if (opt$`contralateral-col` %in% names(long_df)) {
    optional_cols <- c(optional_cols, opt$`contralateral-col`)
  } else if (all(c(opt$`cancer-site-col`, opt$`lung-side-col`) %in% names(long_df))) {
    optional_cols <- c(optional_cols, opt$`cancer-site-col`, opt$`lung-side-col`)
    derive_contralateral_from_sides <- TRUE
    message(
      "Column '", opt$`contralateral-col`, "' not found; deriving contralateral status from ",
      opt$`cancer-site-col`, " and ", opt$`lung-side-col`, "."
    )
  } else {
    warning(
      "Optional column '", opt$`contralateral-col`, "' not found and cannot derive from ",
      opt$`cancer-site-col`, "/", opt$`lung-side-col`,
      "; extra no-contralateral status analysis will be skipped."
    )
  }
}
all_cols <- unique(c(required_cols, optional_cols))

# Check all required columns exist
missing_cols <- setdiff(all_cols, names(long_df))
if (length(missing_cols) > 0) {
  stop("Missing columns in long data: ", paste(missing_cols, collapse = ", "))
}

meta <- long_df %>%
  select(all_of(all_cols)) %>%
  distinct() %>%
  tibble::column_to_rownames(opt$`sample-col`)

effective_contralateral_col <- opt$`contralateral-col`
if (derive_contralateral_from_sides) {
  # Derive lung status:
  # - Cancer samples: TumorSide if lung side matches Cancer_Site, else Contralateral
  # - Non-Cancer samples: Healthy
  meta <- meta %>%
    mutate(
      .derived_lung_status = case_when(
        as.character(status) == as.character(opt$`cancer-label`) &
          !is.na(.data[[opt$`cancer-site-col`]]) &
          !is.na(.data[[opt$`lung-side-col`]]) &
          toupper(substr(as.character(.data[[opt$`cancer-site-col`]]), 1, 1)) ==
            toupper(substr(as.character(.data[[opt$`lung-side-col`]]), 1, 1)) ~ "TumorSide",
        as.character(status) == as.character(opt$`cancer-label`) &
          !is.na(.data[[opt$`cancer-site-col`]]) &
          !is.na(.data[[opt$`lung-side-col`]]) ~ "Contralateral",
        TRUE ~ "Healthy"
      )
    )
  effective_contralateral_col <- ".derived_lung_status"
}

# Read wide format ASV table
message("Reading wide format ASV table: ", opt$`data-wide`)
asv <- read_tsv(opt$`data-wide`, show_col_types = FALSE)
stopifnot(ncol(asv) >= 2)
# Expect first column to be ASV IDs
asv <- asv %>% rename(ASV = 1)
asv_mat <- asv %>% column_to_rownames("ASV") %>% as.matrix()
mode(asv_mat) <- "numeric"

# ---------- align samples ----------
common <- intersect(colnames(asv_mat), rownames(meta))
if (length(common) == 0) stop("No overlapping samples between ASV table columns and metadata rows.")
asv_mat <- asv_mat[, common, drop = FALSE]
meta    <- meta[common, , drop = FALSE]

message("ASV table dimensions: ", paste(dim(asv_mat), collapse = " x "))
message("Metadata dimensions: ", paste(dim(meta), collapse = " x "))

if (!(opt$transform %in% c("none", "rclr"))) {
  stop("--transform must be one of: none, rclr")
}

# ---------- helpers ----------
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

# Convert each sample to relative abundance, then average within groups.
aggregate_mean_relative <- function(X_samples_by_features, group_ids) {
  stopifnot(nrow(X_samples_by_features) == length(group_ids))
  X <- as.matrix(X_samples_by_features)
  mode(X) <- "numeric"
  rs <- rowSums(X, na.rm = TRUE)
  rs[rs == 0] <- 1
  X_rel <- X / rs

  df <- as.data.frame(X_rel)
  df$group_id___ <- as.character(group_ids)
  out <- df %>%
    group_by(group_id___) %>%
    summarise(across(everything(), mean), .groups = "drop")

  mat <- out %>%
    tibble::column_to_rownames("group_id___") %>%
    as.matrix()
  mode(mat) <- "numeric"
  mat
}

aggregate_to_patient_group <- function(X_samples_by_features, patient_ids, group_ids) {
  stopifnot(nrow(X_samples_by_features) == length(patient_ids))
  stopifnot(nrow(X_samples_by_features) == length(group_ids))

  keys <- paste(as.character(patient_ids), as.character(group_ids), sep = "|||")
  X_grouped <- aggregate_mean_relative(X_samples_by_features, keys)
  split_keys <- tibble(key = rownames(X_grouped)) %>%
    tidyr::separate(key, into = c("patient_id___", "group_id___"), sep = "\\|\\|\\|", remove = FALSE)

  list(
    X = X_grouped,
    patient = split_keys$patient_id___,
    group = split_keys$group_id___
  )
}

# ---------- main loop over grouping columns ----------
group_specs <- parse_cli_csv(opt$`group-cols`)
blocked_specs <- parse_cli_csv(opt$`blocked-cols`)
status_contralateral_sites <- parse_cli_csv(opt$`status-contralateral-sites`)

for (gcol in group_specs) {
  grouping <- make_grouping_factor(meta, gcol)
  gcol_slug <- make_group_slug(gcol)

  if (all(is.na(grouping))) {
    warning("Skipping grouping spec '", gcol, "' (all values are NA after combining columns).")
    next
  }

  if (grepl("\\+", gcol)) {
    message("Grouping spec '", gcol, "' will be analyzed as a combined factor.")
  }
  
  # Drop NAs and small groups
  keep_idx <- !is.na(grouping)
  grouping <- droplevels(grouping[keep_idx])
  X <- t(asv_mat[, keep_idx, drop = FALSE]) # samples x ASVs
  meta_keep <- meta[keep_idx, , drop = FALSE]

  # enforce min-n per group
  tab <- table(grouping)
  small <- names(tab[tab < opt$`min-n`])
  if (length(small) > 0) {
    message("Dropping groups in '", gcol, "' with < ", opt$`min-n`, " samples: ",
            paste(small, collapse = ", "))
    keep_idx2 <- !(grouping %in% small)
    grouping <- droplevels(grouping[keep_idx2])
    X <- X[keep_idx2, , drop = FALSE]
    meta_keep <- meta_keep[keep_idx2, , drop = FALSE]
  }

  if (length(unique(grouping)) < 2) {
    warning("Grouping column '", gcol, "' has <2 groups after filtering; skipping.")
    next
  }

  # Status is analyzed within each sample type (between-patient per site).
  if (gcol == "status") {
    if (!("type_group" %in% colnames(meta_keep))) {
      warning("Column 'type_group' not found; skipping stratified status analyses.")
      next
    }
    if (!(opt$`patient-col` %in% colnames(meta_keep))) {
      warning("Patient column '", opt$`patient-col`, "' not found; cannot run status analyses.")
      next
    }

    # Collect per-site results into legacy pooled status outputs expected by ASPIRE readers.
    pooled_status_sign <- list()
    pooled_status_full <- list()
    pooled_status_sign_duleg <- list()
    pooled_status_full_duleg <- list()

    site_levels <- unique(as.character(meta_keep$type_group))
    site_levels <- site_levels[!is.na(site_levels)]

    for (site in site_levels) {
      site_mask <- as.character(meta_keep$type_group) == site
      X_site <- X[site_mask, , drop = FALSE]
      meta_site <- meta_keep[site_mask, , drop = FALSE]

      if (isTRUE(opt$`status-exclude-contralateral`) && (as.character(site) %in% status_contralateral_sites)) {
        if (!(effective_contralateral_col %in% colnames(meta_site))) {
          warning("Cannot exclude contralateral samples for status analysis: column '",
                  effective_contralateral_col, "' not found.")
        } else {
          is_cancer_site <- as.character(meta_site[[gcol]]) == as.character(opt$`cancer-label`)
          is_contralateral_site <- as.character(meta_site[[effective_contralateral_col]]) ==
            as.character(opt$`contralateral-value`)
          keep_site_global <- !(is_cancer_site & is_contralateral_site)
          n_removed_site <- sum(!keep_site_global, na.rm = TRUE)
          if (n_removed_site > 0) {
            message("Status ISA within type_group='", site, "': excluding ", n_removed_site,
                    " contralateral cancer sample(s).")
          }
          X_site <- X_site[keep_site_global, , drop = FALSE]
          meta_site <- meta_site[keep_site_global, , drop = FALSE]
        }
      }

      grouping_site <- droplevels(as.factor(meta_site[[gcol]]))

      if (length(unique(grouping_site)) < 2) {
        warning("Skipping status ISA for type_group='", site, "' (<2 status groups).")
        next
      }

      tab_site <- table(grouping_site)
      small_site <- names(tab_site[tab_site < opt$`min-n`])
      if (length(small_site) > 0) {
        keep_site <- !(grouping_site %in% small_site)
        grouping_site <- droplevels(grouping_site[keep_site])
        X_site <- X_site[keep_site, , drop = FALSE]
        meta_site <- meta_site[keep_site, , drop = FALSE]
      }

      if (length(unique(grouping_site)) < 2) {
        warning("Skipping status ISA for type_group='", site, "' after min-n filtering.")
        next
      }

      X_pat <- aggregate_mean_relative(X_site, meta_site[[opt$`patient-col`]])
      X_pat <- apply_matrix_transform(X_pat, opt$transform)
      status_map <- meta_site %>%
        transmute(
          patient_id___ = as.character(.data[[opt$`patient-col`]]),
          status___ = as.character(.data[[gcol]])
        ) %>%
        distinct() %>%
        group_by(patient_id___) %>%
        summarise(status___ = first(status___), .groups = "drop")
      status_vec <- status_map$status___[match(rownames(X_pat), status_map$patient_id___)]
      grouping_pat <- droplevels(factor(status_vec))

      if (length(unique(grouping_pat)) < 2) {
        warning("Skipping status ISA for type_group='", site, "' after patient aggregation.")
        next
      }

      message("Running multipatt for 'status' within type_group='", site,
              "' (general multipatt, duleg=FALSE) …")
      fit1 <- run_indics(X_pat, grouping_pat, perms = opt$perms, duleg = FALSE, patient_blocks = NULL)
      res1_sign <- as.data.frame(fit1$sign) %>% rownames_to_column("ASV")
      res1_full <- summarize_multipatt(fit1)
      site_slug <- gsub("[^A-Za-z0-9]+", "_", site)
      write_tables(res1_sign, res1_full, paste0("status_", site_slug, "_indicator_species"))

      pooled_status_sign[[length(pooled_status_sign) + 1]] <- res1_sign %>% mutate(type_group = site)
      pooled_status_full[[length(pooled_status_full) + 1]] <- res1_full %>% mutate(type_group = site)

      message("Running multipatt for 'status' within type_group='", site,
              "' (DULEG-restricted mode, duleg=TRUE) …")
      fit2 <- run_indics(X_pat, grouping_pat, perms = opt$perms, duleg = TRUE, patient_blocks = NULL)
      res2_sign <- as.data.frame(fit2$sign) %>% rownames_to_column("ASV")
      res2_full <- summarize_multipatt(fit2)
      write_tables(res2_sign, res2_full, paste0("status_", site_slug, "_indicator_species_DULEG"))

      pooled_status_sign_duleg[[length(pooled_status_sign_duleg) + 1]] <- res2_sign %>% mutate(type_group = site)
      pooled_status_full_duleg[[length(pooled_status_full_duleg) + 1]] <- res2_full %>% mutate(type_group = site)

      # Extra status analysis for Lung Brush: remove contralateral samples from cancer patients.
      if (isTRUE(opt$`status-extra-no-contralateral`) && !isTRUE(opt$`status-exclude-contralateral`) &&
          identical(as.character(site), as.character(opt$`lung-brush-label`))) {
        if (!(effective_contralateral_col %in% colnames(meta_site))) {
          warning("Skipping extra Lung Brush no-contralateral analysis: column '",
                  effective_contralateral_col, "' not found.")
        } else {
          is_cancer <- as.character(meta_site[[gcol]]) == as.character(opt$`cancer-label`)
          is_contralateral <- as.character(meta_site[[effective_contralateral_col]]) ==
            as.character(opt$`contralateral-value`)
          keep_no_contra <- !(is_cancer & is_contralateral)

          n_removed <- sum(!keep_no_contra, na.rm = TRUE)
          message("Lung Brush extra status analysis: excluding ", n_removed,
                  " contralateral cancer sample(s) using ",
                  effective_contralateral_col, " == '", opt$`contralateral-value`, "'.")

          X_site_nc <- X_site[keep_no_contra, , drop = FALSE]
          meta_site_nc <- meta_site[keep_no_contra, , drop = FALSE]
          grouping_site_nc <- droplevels(as.factor(meta_site_nc[[gcol]]))

          if (length(unique(grouping_site_nc)) < 2) {
            warning("Skipping Lung Brush no-contralateral status ISA (<2 status groups after filtering).")
          } else {
            tab_site_nc <- table(grouping_site_nc)
            small_site_nc <- names(tab_site_nc[tab_site_nc < opt$`min-n`])
            if (length(small_site_nc) > 0) {
              keep_site_nc <- !(grouping_site_nc %in% small_site_nc)
              grouping_site_nc <- droplevels(grouping_site_nc[keep_site_nc])
              X_site_nc <- X_site_nc[keep_site_nc, , drop = FALSE]
              meta_site_nc <- meta_site_nc[keep_site_nc, , drop = FALSE]
            }

            if (length(unique(grouping_site_nc)) < 2) {
              warning("Skipping Lung Brush no-contralateral status ISA after min-n filtering.")
            } else {
              X_pat_nc <- aggregate_mean_relative(X_site_nc, meta_site_nc[[opt$`patient-col`]])
              X_pat_nc <- apply_matrix_transform(X_pat_nc, opt$transform)
              status_map_nc <- meta_site_nc %>%
                transmute(
                  patient_id___ = as.character(.data[[opt$`patient-col`]]),
                  status___ = as.character(.data[[gcol]])
                ) %>%
                distinct() %>%
                group_by(patient_id___) %>%
                summarise(status___ = first(status___), .groups = "drop")
              status_vec_nc <- status_map_nc$status___[
                match(rownames(X_pat_nc), status_map_nc$patient_id___)
              ]
              grouping_pat_nc <- droplevels(factor(status_vec_nc))

              if (length(unique(grouping_pat_nc)) < 2) {
                warning("Skipping Lung Brush no-contralateral status ISA after patient aggregation.")
              } else {
                message("Running multipatt for 'status' within type_group='", site,
                        "' excluding contralateral cancer samples (general multipatt, duleg=FALSE) …")
                fit1_nc <- run_indics(X_pat_nc, grouping_pat_nc, perms = opt$perms,
                                      duleg = FALSE, patient_blocks = NULL)
                res1_nc_sign <- as.data.frame(fit1_nc$sign) %>% rownames_to_column("ASV")
                res1_nc_full <- summarize_multipatt(fit1_nc)
                write_tables(
                  res1_nc_sign, res1_nc_full,
                  paste0("status_", site_slug, "_no_contralateral_indicator_species")
                )

                message("Running multipatt for 'status' within type_group='", site,
                        "' excluding contralateral cancer samples (DULEG-restricted mode, duleg=TRUE) …")
                fit2_nc <- run_indics(X_pat_nc, grouping_pat_nc, perms = opt$perms,
                                      duleg = TRUE, patient_blocks = NULL)
                res2_nc_sign <- as.data.frame(fit2_nc$sign) %>% rownames_to_column("ASV")
                res2_nc_full <- summarize_multipatt(fit2_nc)
                write_tables(
                  res2_nc_sign, res2_nc_full,
                  paste0("status_", site_slug, "_no_contralateral_indicator_species_DULEG")
                )
              }
            }
          }
        }
      }
    }

    # Emit legacy pooled files expected by existing ASPIRE plotting workflows.
    if (length(pooled_status_sign) > 0) {
      write_tables(bind_rows(pooled_status_sign), bind_rows(pooled_status_full), "status_indicator_species")
    }
    if (length(pooled_status_sign_duleg) > 0) {
      write_tables(bind_rows(pooled_status_sign_duleg), bind_rows(pooled_status_full_duleg), "status_indicator_species_DULEG")
    }

    next
  }

  # Blocking is analysis-specific: within-patient factors such as Type_Group
  # should be blocked, while between-patient factors such as Case must not be.
  use_blocking <- tolower(gcol) %in% tolower(blocked_specs)
  patient_blocks <- NULL
  blocking_ids <- NULL
  if (use_blocking && !is.null(opt$`block-col`) && nzchar(opt$`block-col`)) {
    if (!(opt$`block-col` %in% colnames(meta_keep))) {
      stop("Block column '", opt$`block-col`, "' not found in metadata. Available: ",
           paste(colnames(meta_keep), collapse = ", "))
    }
    blocking_ids <- as.character(meta_keep[[opt$`block-col`]])
    patient_blocks <- droplevels(factor(blocking_ids))
    message("Grouping '", gcol, "' uses explicit blocking column '", opt$`block-col`, "'.")
  } else if (use_blocking) {
    if (opt$`patient-col` %in% colnames(meta_keep)) {
      blocking_ids <- as.character(meta_keep[[opt$`patient-col`]])
      patient_blocks <- droplevels(factor(blocking_ids))
      message("Grouping '", gcol, "' uses BLOCKED permutations (within-patient design)")
    } else {
      warning("Patient column '", opt$`patient-col`, "' not found; using standard permutations")
    }
  }

  X_for_isa <- X
  grouping_for_isa <- grouping
  if (use_blocking && !is.null(patient_blocks)) {
    if (is.null(blocking_ids)) {
      stop("Blocking requested for grouping column '", gcol, "' but no blocking IDs were resolved.")
    }

    groups_per_block <- tapply(as.character(grouping), blocking_ids, function(values) {
      length(unique(values[!is.na(values)]))
    })
    if (all(groups_per_block < 2)) {
      warning(
        "Grouping '", gcol, "' is constant within every block; disabling blocked permutations."
      )
      use_blocking <- FALSE
      patient_blocks <- NULL
      blocking_ids <- NULL
    }
  }

  if (use_blocking && !is.null(patient_blocks)) {

    if (tolower(gcol) == "type_group" && isTRUE(opt$`type-group-require-complete`)) {
      needed_types <- unique(as.character(grouping))
      keep_patients <- meta_keep %>%
        transmute(
          patient_id___ = blocking_ids,
          group_id___ = as.character(.data[[gcol]])
        ) %>%
        distinct() %>%
        group_by(patient_id___) %>%
        summarise(n_types = n_distinct(group_id___), .groups = "drop") %>%
        filter(n_types == length(needed_types)) %>%
        pull(patient_id___)

      if (length(keep_patients) == 0) {
        warning("No patients have all sample types for '", gcol, "'; skipping.")
        next
      }

      keep_rows <- blocking_ids %in% keep_patients
      X <- X[keep_rows, , drop = FALSE]
      meta_keep <- meta_keep[keep_rows, , drop = FALSE]
      grouping <- droplevels(grouping[keep_rows])
      blocking_ids <- blocking_ids[keep_rows]
      patient_blocks <- droplevels(factor(blocking_ids))
    }

    collapsed <- aggregate_to_patient_group(X, blocking_ids, grouping)
    X_for_isa <- collapsed$X
    grouping_for_isa <- droplevels(factor(collapsed$group))
    patient_blocks <- droplevels(factor(collapsed$patient))

    tab_pat <- table(grouping_for_isa)
    small_pat <- names(tab_pat[tab_pat < opt$`min-n`])
    if (length(small_pat) > 0) {
      keep_pat <- !(grouping_for_isa %in% small_pat)
      grouping_for_isa <- droplevels(grouping_for_isa[keep_pat])
      X_for_isa <- X_for_isa[keep_pat, , drop = FALSE]
      patient_blocks <- droplevels(patient_blocks[keep_pat])
    }

    if (length(unique(grouping_for_isa)) < 2) {
      warning("Grouping column '", gcol, "' has <2 block-level groups after aggregation; skipping.")
      next
    }
  }
  X_for_isa <- apply_matrix_transform(X_for_isa, opt$transform)

  message("Running multipatt for '", gcol, "' (general multipatt, duleg=FALSE) …")
  fit1 <- run_indics(X_for_isa, grouping_for_isa, perms = opt$perms, duleg = FALSE, patient_blocks = patient_blocks)
  res1_sign <- as.data.frame(fit1$sign) %>% rownames_to_column("ASV")
  res1_full <- summarize_multipatt(fit1)
  write_tables(res1_sign, res1_full, paste0(gcol_slug, "_indicator_species"))

  message("Running multipatt for '", gcol, "' (DULEG-restricted mode, duleg=TRUE) …")
  fit2 <- run_indics(X_for_isa, grouping_for_isa, perms = opt$perms, duleg = TRUE, patient_blocks = patient_blocks)
  res2_sign <- as.data.frame(fit2$sign) %>% rownames_to_column("ASV")
  res2_full <- summarize_multipatt(fit2)
  write_tables(res2_sign, res2_full, paste0(gcol_slug, "_indicator_species_DULEG"))
  
  if (tolower(gcol) == "type_group") {
    write_tables(res1_sign, res1_full, "Type_Group_indicator_species")
    write_tables(res2_sign, res2_full, "Type_Group_indicator_species_DULEG")
  }
}

for (spec in stratified_specs) {
  within_col <- spec$within_col
  group_col <- spec$group_col
  within_slug <- make_group_slug(within_col)
  group_slug <- make_group_slug(group_col)

  if (!(within_col %in% colnames(meta))) {
    warning("Skipping stratified ISA: within_col '", within_col, "' not found.")
    next
  }
  if (!(group_col %in% colnames(meta))) {
    warning("Skipping stratified ISA: group_col '", group_col, "' not found.")
    next
  }

  within_levels <- spec$levels
  if (length(within_levels) == 0) {
    within_levels <- unique(as.character(meta[[within_col]]))
    within_levels <- within_levels[!is.na(within_levels) & within_levels != ""]
  }

  pooled_sign <- list()
  pooled_full <- list()
  pooled_sign_duleg <- list()
  pooled_full_duleg <- list()

  for (within_value in within_levels) {
    site_mask <- !is.na(meta[[within_col]]) & as.character(meta[[within_col]]) == as.character(within_value)
    if (!any(site_mask)) {
      warning("Skipping stratified ISA for ", group_col, " within ", within_col,
              "='", within_value, "': no matching samples.")
      next
    }

    X_site <- t(asv_mat[, site_mask, drop = FALSE])
    meta_site <- meta[site_mask, , drop = FALSE]
    grouping_site <- droplevels(as.factor(meta_site[[group_col]]))

    keep_site <- !is.na(grouping_site)
    grouping_site <- droplevels(grouping_site[keep_site])
    X_site <- X_site[keep_site, , drop = FALSE]
    meta_site <- meta_site[keep_site, , drop = FALSE]

    tab_site <- table(grouping_site)
    small_site <- names(tab_site[tab_site < opt$`min-n`])
    if (length(small_site) > 0) {
      message("Dropping groups in stratified ISA for ", group_col, " within ",
              within_col, "='", within_value, "' with < ", opt$`min-n`,
              " samples: ", paste(small_site, collapse = ", "))
      keep_min <- !(grouping_site %in% small_site)
      grouping_site <- droplevels(grouping_site[keep_min])
      X_site <- X_site[keep_min, , drop = FALSE]
      meta_site <- meta_site[keep_min, , drop = FALSE]
    }

    if (length(unique(grouping_site)) < 2) {
      warning("Skipping stratified ISA for ", group_col, " within ", within_col,
              "='", within_value, "' (<2 groups after filtering).")
      next
    }

    X_for_isa <- X_site
    grouping_for_isa <- grouping_site

    if (opt$`patient-col` %in% colnames(meta_site)) {
      X_for_isa <- aggregate_mean_relative(X_site, meta_site[[opt$`patient-col`]])
      group_map <- meta_site %>%
        transmute(
          patient_id___ = as.character(.data[[opt$`patient-col`]]),
          group_id___ = as.character(.data[[group_col]])
        ) %>%
        filter(!is.na(patient_id___), !is.na(group_id___)) %>%
        distinct() %>%
        group_by(patient_id___) %>%
        summarise(
          n_groups___ = n_distinct(group_id___),
          group_id___ = first(group_id___),
          .groups = "drop"
        )

      mixed_patients <- group_map$patient_id___[group_map$n_groups___ > 1]
      if (length(mixed_patients) > 0) {
        warning("Stratified ISA for ", group_col, " within ", within_col, "='",
                within_value, "' has patients with multiple group labels; using first label for: ",
                paste(mixed_patients, collapse = ", "))
      }

      group_vec <- group_map$group_id___[match(rownames(X_for_isa), group_map$patient_id___)]
      keep_pat <- !is.na(group_vec)
      X_for_isa <- X_for_isa[keep_pat, , drop = FALSE]
      grouping_for_isa <- droplevels(factor(group_vec[keep_pat]))

      tab_pat <- table(grouping_for_isa)
      small_pat <- names(tab_pat[tab_pat < opt$`min-n`])
      if (length(small_pat) > 0) {
        keep_pat_min <- !(grouping_for_isa %in% small_pat)
        grouping_for_isa <- droplevels(grouping_for_isa[keep_pat_min])
        X_for_isa <- X_for_isa[keep_pat_min, , drop = FALSE]
      }
    }

    if (length(unique(grouping_for_isa)) < 2) {
      warning("Skipping stratified ISA for ", group_col, " within ", within_col,
              "='", within_value, "' after patient aggregation.")
      next
    }

    X_for_isa <- apply_matrix_transform(X_for_isa, opt$transform)
    within_value_slug <- make_group_slug(within_value)
    base <- paste0(
      "stratified_", group_slug, "_within_", within_slug, "_",
      within_value_slug, "_indicator_species"
    )

    message("Running stratified multipatt for '", group_col, "' within '",
            within_col, "'='", within_value, "' (general multipatt, duleg=FALSE) …")
    fit1 <- run_indics(X_for_isa, grouping_for_isa, perms = opt$perms, duleg = FALSE, patient_blocks = NULL)
    res1_sign <- as.data.frame(fit1$sign) %>%
      rownames_to_column("ASV") %>%
      mutate(stratified_within_col = within_col, stratified_within_value = within_value,
             stratified_group_col = group_col)
    res1_full <- summarize_multipatt(fit1) %>%
      mutate(stratified_within_col = within_col, stratified_within_value = within_value,
             stratified_group_col = group_col)
    write_tables(res1_sign, res1_full, base)
    pooled_sign[[length(pooled_sign) + 1]] <- res1_sign
    pooled_full[[length(pooled_full) + 1]] <- res1_full

    message("Running stratified multipatt for '", group_col, "' within '",
            within_col, "'='", within_value, "' (DULEG-restricted mode, duleg=TRUE) …")
    fit2 <- run_indics(X_for_isa, grouping_for_isa, perms = opt$perms, duleg = TRUE, patient_blocks = NULL)
    res2_sign <- as.data.frame(fit2$sign) %>%
      rownames_to_column("ASV") %>%
      mutate(stratified_within_col = within_col, stratified_within_value = within_value,
             stratified_group_col = group_col)
    res2_full <- summarize_multipatt(fit2) %>%
      mutate(stratified_within_col = within_col, stratified_within_value = within_value,
             stratified_group_col = group_col)
    write_tables(res2_sign, res2_full, paste0(base, "_DULEG"))
    pooled_sign_duleg[[length(pooled_sign_duleg) + 1]] <- res2_sign
    pooled_full_duleg[[length(pooled_full_duleg) + 1]] <- res2_full
  }

  if (length(pooled_sign) > 0) {
    pooled_base <- paste0("stratified_", group_slug, "_within_", within_slug, "_indicator_species")
    write_tables(bind_rows(pooled_sign), bind_rows(pooled_full), pooled_base)
    write_tables(bind_rows(pooled_sign_duleg), bind_rows(pooled_full_duleg), paste0(pooled_base, "_DULEG"))
  }
}

message("Done. Results in: ", outdir)
