#!/usr/bin/env Rscript
#
# power_indicspecies.R
#
# Indicator Species Analysis (ISA) power analysis for ASPIRE workflow.
# Matches run_indicspecies.R but adds patient-level bootstrap for power analysis.
# Properly handles hierarchical structure: multiple samples per patient.
#
# Key features:
# 1. Bootstrap patients (not samples) to preserve within-patient correlation
# 2. Assign unique bootstrap IDs to handle duplicate patients in permutations
# 3. Aggregate to patient level before running multipatt
# 4. Test multiple spike-in scenarios (Null, Weak, Moderate, Strong)
# 5. Matches run_indicspecies.R grouping approach (--group-cols)
# 6. Tests both duleg=FALSE and duleg=TRUE (like run_indicspecies.R)
#

suppressPackageStartupMessages({
  library(optparse)
  library(dplyr)
  library(purrr)
  library(readr)
  library(tibble)
  library(tidyr)
  library(indicspecies)
  library(permute)
})

# ============================================================================
# Command line options
# ============================================================================

option_list <- list(
  make_option("--data-long", type="character",
              help="Long format data file (ASV_master_long.tsv)"),
  make_option("--data-wide", type="character",
              help="Wide count matrix (ASV_master_count_wide.tsv)"),
  make_option("--sample-col", type="character", default="sample",
              help="Column in long data for sample IDs [default: %default]"),
  make_option("--patient-col", type="character", default="Participant_ID",
              help="Column for patient IDs [default: %default]"),
  make_option("--group-cols", type="character", default="status,type_group",
              help="Comma-separated grouping columns to analyze [default: %default]"),
  make_option("--status-sites", type="character", default="",
              help="Optional comma-separated type_group levels for status power (default: all sites)"),
  make_option("--status-extra-no-contralateral", type="logical", default=TRUE,
              help="For Bronchial Brush status power, exclude contralateral cancer samples [default: %default]"),
  make_option("--status-exclude-contralateral", type="logical", default=TRUE,
              help="Exclude contralateral cancer samples for selected status sample types [default: %default]"),
  make_option("--status-contralateral-sites", type="character", default="Bronchial Brush,BAL",
              help="Comma-separated type_group values where contralateral exclusion applies [default: %default]"),
  make_option("--contralateral-col", type="character", default="lung_status",
              help="Metadata column identifying contralateral samples [default: %default]"),
  make_option("--cancer-site-col", type="character", default="Cancer_Site",
              help="Metadata column giving cancer side for cancer patients (used to derive contralateral if needed) [default: %default]"),
  make_option("--lung-side-col", type="character", default="lung_code",
              help="Metadata column giving sample lung side (used to derive contralateral if needed) [default: %default]"),
  make_option("--contralateral-value", type="character", default="Contralateral",
              help="Value in contralateral column marking contralateral samples [default: %default]"),
  make_option("--cancer-label", type="character", default="Cancer",
              help="Label in status column for cancer samples [default: %default]"),
  make_option("--lung-brush-label", type="character", default="Bronchial Brush",
              help="Value in type_group identifying Bronchial Brush samples [default: %default]"),
  make_option("--sample-sizes", type="character", default="5,8,10,15,20,25,30,40,50,60,70,80,90,100",
              help="Fallback sample sizes if cancer/stype grids are not provided [default: %default]"),
  make_option("--sample-sizes-cancer", type="character", default="",
              help="Comma-separated sample sizes for status (cancer vs control) ISA power"),
  make_option("--sample-sizes-stype", type="character", default="",
              help="Comma-separated sample sizes for type_group ISA power"),
  make_option("--scenarios", type="character", default="observed,null",
              help="Comma-separated: observed, null, weak, moderate, strong [default: %default]"),
  make_option("--transform", type="character", default="none",
              help="Abundance transform before multipatt: none|rclr [default: %default]"),
  make_option("--n-simulations", type="integer", default=1000,
              help="Number of bootstrap simulations [default: %default]"),
  make_option("--perms", type="integer", default=199,
              help="Permutations for multipatt [default: %default]"),
  make_option("--alpha", type="double", default=0.05,
              help="Significance threshold [default: %default]"),
  make_option("--seed", type="integer", default=42,
              help="Random seed [default: %default]"),
  make_option("--test-duleg", type="logical", default=TRUE,
              help="Test both duleg=FALSE and duleg=TRUE [default: %default]"),
  make_option("--outdir", type="character",
              help="Output directory.")
)

parser <- OptionParser(
  usage = "%prog --data-long ASV_master_long.tsv --data-wide ASV_master_count_wide.tsv --group-cols status,type_group --outdir out_dir",
  description = "ISA power analysis with patient-level bootstrap (matches run_indicspecies.R).",
  option_list = option_list
)

args <- parse_args(parser)

# Enforce required options
required <- c("data-long", "data-wide", "outdir")
missing <- required[sapply(required, function(x) is.null(args[[x]]))]
if (length(missing)) {
  cat("Missing required option(s):", paste(missing, collapse=", "), "\n\n", file=stderr())
  print_help(parser)
  quit(status=2)
}

# Handle defaults for optional parameters
if (is.null(args$seed)) args$seed <- 42
if (is.null(args$`n-simulations`)) args$`n-simulations` <- 1000
if (is.null(args$perms)) args$perms <- 199
if (is.null(args$alpha)) args$alpha <- 0.05
if (is.null(args$`test-duleg`)) args$`test-duleg` <- TRUE
if (is.null(args$`sample-sizes`)) args$`sample-sizes` <- "5,8,10,15,20,25,30,40,50,60,70,80,90,100"
if (is.null(args$`sample-sizes-cancer`)) args$`sample-sizes-cancer` <- ""
if (is.null(args$`sample-sizes-stype`)) args$`sample-sizes-stype` <- ""
if (is.null(args$scenarios)) args$scenarios <- "observed,null"
if (is.null(args$`group-cols`)) args$`group-cols` <- "status,type_group"
if (is.null(args$`status-sites`)) args$`status-sites` <- ""
if (is.null(args$`status-contralateral-sites`)) args$`status-contralateral-sites` <- "Bronchial Brush,BAL"

if (!(args$transform %in% c("none", "rclr"))) {
  stop("--transform must be one of: none, rclr")
}

set.seed(args$seed)

cat("================================================================\n")
cat("Indicator Species Analysis (ISA) Power Analysis - ASPIRE\n")
cat("================================================================\n")
cat("Patient-level bootstrap preserving hierarchical structure\n")
cat("================================================================\n\n")

# ============================================================================
# Load and prepare data
# ============================================================================

cat("Loading data...\n")
cat("  Long format:", args$`data-long`, "\n")
cat("  Wide format:", args$`data-wide`, "\n\n")

# Read long format data to get metadata
message("Reading long format data: ", args$`data-long`)
long_df <- read_tsv(args$`data-long`, show_col_types = FALSE)
sample_col <- args$`sample-col`
if (!(sample_col %in% names(long_df)) && identical(sample_col, "sample") && ("lmp_id" %in% names(long_df))) {
  message("Column 'sample' not found; using legacy alias 'lmp_id'.")
  sample_col <- "lmp_id"
}

# Extract metadata (unique sample-level records)
required_cols <- c(sample_col, args$`patient-col`)
group_cols_vec <- strsplit(args$`group-cols`, ",", fixed = TRUE)[[1]] %>% trimws()
status_site_col <- NULL
if ("status" %in% group_cols_vec) {
  non_status_cols <- setdiff(group_cols_vec, "status")
  if (length(non_status_cols) > 0) {
    status_site_col <- non_status_cols[[1]]
  } else if ("type_group" %in% names(long_df)) {
    status_site_col <- "type_group"
  } else if ("Type_Group" %in% names(long_df)) {
    status_site_col <- "Type_Group"
  }

  if (!is.null(status_site_col) && !(status_site_col %in% group_cols_vec)) {
    # Needed for status stratification by sample type.
    group_cols_vec <- c(group_cols_vec, status_site_col)
  }
}
optional_cols <- c()
derive_contralateral_from_sides <- FALSE
if (isTRUE(args$`status-extra-no-contralateral`) && ("status" %in% group_cols_vec)) {
  if (args$`contralateral-col` %in% names(long_df)) {
    optional_cols <- c(optional_cols, args$`contralateral-col`)
  } else if (all(c(args$`cancer-site-col`, args$`lung-side-col`) %in% names(long_df))) {
    optional_cols <- c(optional_cols, args$`cancer-site-col`, args$`lung-side-col`)
    derive_contralateral_from_sides <- TRUE
    message(
      "Column '", args$`contralateral-col`, "' not found; deriving contralateral status from ",
      args$`cancer-site-col`, " and ", args$`lung-side-col`, "."
    )
  } else {
    warning(
      "Optional column '", args$`contralateral-col`, "' not found and cannot derive from ",
      args$`cancer-site-col`, "/", args$`lung-side-col`,
      "; Bronchial Brush no-contralateral power filter will be skipped."
    )
  }
}
all_cols <- unique(c(required_cols, group_cols_vec, optional_cols))

# Check all required columns exist
missing_cols <- setdiff(all_cols, names(long_df))
if (length(missing_cols) > 0) {
  stop("Missing columns in long data: ", paste(missing_cols, collapse = ", "))
}

meta <- long_df %>%
  select(all_of(all_cols)) %>%
  distinct() %>%
  tibble::column_to_rownames(sample_col)

effective_contralateral_col <- args$`contralateral-col`
if (derive_contralateral_from_sides) {
  # Derive lung status for filtering contralateral samples in cancer patients.
  meta <- meta %>%
    mutate(
      .derived_lung_status = case_when(
        as.character(status) == as.character(args$`cancer-label`) &
          !is.na(.data[[args$`cancer-site-col`]]) &
          !is.na(.data[[args$`lung-side-col`]]) &
          toupper(substr(as.character(.data[[args$`cancer-site-col`]]), 1, 1)) ==
            toupper(substr(as.character(.data[[args$`lung-side-col`]]), 1, 1)) ~ "TumorSide",
        as.character(status) == as.character(args$`cancer-label`) &
          !is.na(.data[[args$`cancer-site-col`]]) &
          !is.na(.data[[args$`lung-side-col`]]) ~ "Contralateral",
        TRUE ~ "Healthy"
      )
    )
  effective_contralateral_col <- ".derived_lung_status"
}

# Read wide format ASV table
message("Reading wide format ASV table: ", args$`data-wide`)
asv <- read_tsv(args$`data-wide`, show_col_types = FALSE)
stopifnot(ncol(asv) >= 2)
# Expect first column to be ASV IDs
asv <- asv %>% rename(ASV = 1)
asv_mat <- asv %>% column_to_rownames("ASV") %>% as.matrix()
mode(asv_mat) <- "numeric"

# Align samples
common <- intersect(colnames(asv_mat), rownames(meta))
if (length(common) == 0) {
  stop("No overlapping samples between ASV table and metadata.")
}

asv_mat <- asv_mat[, common, drop = FALSE]
meta <- meta[common, , drop = FALSE]

message("ASV table dimensions: ", paste(dim(asv_mat), collapse = " x "))
message("Metadata dimensions: ", paste(dim(meta), collapse = " x "))

cat("Data loaded:\n")
cat("  Samples:", ncol(asv_mat), "\n")
cat("  ASVs:", nrow(asv_mat), "\n")
cat("  Patients:", length(unique(meta[[args$`patient-col`]])), "\n\n")

# grouping columns already parsed above when extracting metadata
group_cols <- group_cols_vec
sample_sizes_default <- as.integer(strsplit(args$`sample-sizes`, ",")[[1]])
sample_sizes_default <- sample_sizes_default[!is.na(sample_sizes_default)]
if (length(sample_sizes_default) == 0) {
  stop("No valid values parsed from --sample-sizes")
}

sample_sizes_cancer <- sample_sizes_default
if (nzchar(args$`sample-sizes-cancer`)) {
  sample_sizes_cancer <- as.integer(strsplit(args$`sample-sizes-cancer`, ",")[[1]])
  sample_sizes_cancer <- sample_sizes_cancer[!is.na(sample_sizes_cancer)]
  if (length(sample_sizes_cancer) == 0) {
    stop("No valid values parsed from --sample-sizes-cancer")
  }
}

sample_sizes_stype <- sample_sizes_default
if (nzchar(args$`sample-sizes-stype`)) {
  sample_sizes_stype <- as.integer(strsplit(args$`sample-sizes-stype`, ",")[[1]])
  sample_sizes_stype <- sample_sizes_stype[!is.na(sample_sizes_stype)]
  if (length(sample_sizes_stype) == 0) {
    stop("No valid values parsed from --sample-sizes-stype")
  }
}

status_sites_requested <- strsplit(args$`status-sites`, ",", fixed = TRUE)[[1]] %>% trimws()
status_sites_requested <- status_sites_requested[status_sites_requested != ""]
status_contralateral_sites <- strsplit(args$`status-contralateral-sites`, ",", fixed = TRUE)[[1]] %>% trimws()
status_contralateral_sites <- status_contralateral_sites[status_contralateral_sites != ""]

# Parse requested scenarios
requested_scenarios <- tolower(trimws(strsplit(args$scenarios, ",")[[1]]))
cat("Requested scenarios:", paste(requested_scenarios, collapse=", "), "\n")

# Determine actual number of control patients from the data
if ("status" %in% colnames(meta)) {
  control_patients <- meta %>%
    filter(status %in% c("Control", "Non-Cancer")) %>%
    pull(!!sym(args$`patient-col`)) %>%
    unique()
  actual_n_control <- length(control_patients)
  cat("Detected", actual_n_control, "control patients in data\n")
} else {
  actual_n_control <- NULL
  cat("No 'status' column found; cannot auto-detect control count\n")
}

cat("Grouping columns:", paste(group_cols, collapse=", "), "\n")
if (length(status_sites_requested) > 0) {
  cat("Status sites filter:", paste(status_sites_requested, collapse=", "), "\n")
} else {
  cat("Status sites filter: all\n")
}
cat("Sample sizes (status/cancer):", paste(sample_sizes_cancer, collapse=", "), "\n")
cat("Sample sizes (type_group):", paste(sample_sizes_stype, collapse=", "), "\n")
cat("Fallback sample sizes:", paste(sample_sizes_default, collapse=", "), "\n")
cat("Simulations:", args$`n-simulations`, "\n")
cat("Permutations per test:", args$perms, "\n")
cat("Test duleg (combos):", args$`test-duleg`, "\n\n")

# ============================================================================
# Helper functions
# ============================================================================

#' Build scenario list based on requested scenarios
#'
#' @param requested Vector of requested scenario names (lowercase)
#' @param top_asv_idx Top ASV indices for spike-ins (can be NULL if no spikes requested)
#' @return List of scenario configurations
build_scenario_list <- function(requested, top_asv_idx = NULL) {
  scenarios <- list()

  if ("null" %in% requested) {
    scenarios <- c(scenarios, list(
      list(name = "Null", asv_indices = NULL, fold_change = 1.0, use_true_null = TRUE)
    ))
  }

  if ("observed" %in% requested) {
    scenarios <- c(scenarios, list(
      list(name = "Observed", asv_indices = NULL, fold_change = 1.0, use_true_null = FALSE)
    ))
  }

  if ("weak" %in% requested) {
    if (is.null(top_asv_idx) || length(top_asv_idx) == 0) {
      warning("'weak' scenario requested but no spike ASVs available; skipping")
    } else {
      scenarios <- c(scenarios, list(
        list(name = "Weak", asv_indices = top_asv_idx[1:min(3, length(top_asv_idx))],
             fold_change = 1.5, use_true_null = FALSE)
      ))
    }
  }

  if ("moderate" %in% requested) {
    if (is.null(top_asv_idx) || length(top_asv_idx) == 0) {
      warning("'moderate' scenario requested but no spike ASVs available; skipping")
    } else {
      scenarios <- c(scenarios, list(
        list(name = "Moderate", asv_indices = top_asv_idx[1:min(5, length(top_asv_idx))],
             fold_change = 2.0, use_true_null = FALSE)
      ))
    }
  }

  if ("strong" %in% requested) {
    if (is.null(top_asv_idx) || length(top_asv_idx) == 0) {
      warning("'strong' scenario requested but no spike ASVs available; skipping")
    } else {
      scenarios <- c(scenarios, list(
        list(name = "Strong", asv_indices = top_asv_idx,
             fold_change = 2.5, use_true_null = FALSE)
      ))
    }
  }

  if (length(scenarios) == 0) {
    stop("No valid scenarios were built from requested: ", paste(requested, collapse=", "))
  }

  return(scenarios)
}

#' Compute hybrid sample sizes for cancer vs control
#'
#' For n_cancer <= actual_n_control: use actual_n_control
#' For n_cancer > actual_n_control: use balanced (n_cancer)
#'
#' @param n_cancer Number of cancer patients
#' @param actual_n_control Actual number of control patients in data
#' @param group_levels Character vector of group level names (e.g., c("Cancer", "Control"))
#' @return Named vector with sample sizes per group
compute_hybrid_n_per_group <- function(n_cancer, actual_n_control, group_levels) {
  if (is.null(actual_n_control)) {
    # Fallback: balanced sampling
    n_control <- n_cancer
  } else if (n_cancer <= actual_n_control) {
    n_control <- actual_n_control
  } else {
    n_control <- n_cancer
  }

  # Return named vector
  # Assumes first group is "Cancer" or treatment group
  setNames(c(n_cancer, n_control), group_levels)
}

#' Bootstrap patients with replacement (preserves hierarchical structure)
#'
#' Generic function that works with any grouping variable.
#' For each resampled patient, include ALL their samples.
#' Assigns unique bootstrap IDs to handle duplicate patients in permutations.
#'
#' @param counts Sample × ASV count matrix
#' @param patient_ids Patient ID for each sample
#' @param grouping Grouping factor for each sample (e.g., status, type_group)
#' @param n_per_group Named vector of sample sizes per group, or single integer for balanced
#' @param seed Random seed
#' @return List with boot_counts, boot_patients, boot_grouping
bootstrap_patients_generic <- function(counts, patient_ids, grouping,
                                       n_per_group, seed = 42) {
  set.seed(seed)

  # Defensive: ensure patient_ids is character for type-safe comparisons
  patient_ids <- as.character(patient_ids)

  # Get unique groups
  unique_groups <- levels(grouping)

  # Handle n_per_group as single integer (balanced) or named vector
  if (length(n_per_group) == 1) {
    n_per_group <- setNames(rep(n_per_group, length(unique_groups)), unique_groups)
  }

  # Track draw counts for unique IDs (critical for permutation correctness)
  patient_draw_count <- list()

  boot_indices <- c()
  boot_patient_list <- c()
  boot_grouping_list <- c()

  # Process each group
  for (grp in unique_groups) {
    grp_mask <- grouping == grp
    grp_patients <- unique(patient_ids[grp_mask])

    # Sample n patients for this group
    n_sample <- n_per_group[[as.character(grp)]]

    # Skip empty groups or zero sample size
    if (length(grp_patients) == 0 || n_sample == 0) {
      next
    }

    boot_patients_grp <- sample(grp_patients, n_sample, replace = TRUE)

    for (patient in boot_patients_grp) {
      patient_key <- as.character(patient)
      if (is.null(patient_draw_count[[patient_key]])) {
        draw_idx <- 0
      } else {
        draw_idx <- patient_draw_count[[patient_key]]
      }
      patient_draw_count[[patient_key]] <- draw_idx + 1

      # CRITICAL: unique bootstrap ID prevents permutation collapse
      unique_boot_id <- paste0(patient_key, "__b", draw_idx)

      # Get all samples from this patient (CRITICAL: convert both to character for matching)
      patient_samples <- which(as.character(patient_ids) == patient_key)
      boot_indices <- c(boot_indices, patient_samples)
      boot_patient_list <- c(boot_patient_list, rep(unique_boot_id, length(patient_samples)))
      boot_grouping_list <- c(boot_grouping_list, rep(as.character(grp), length(patient_samples)))
    }
  }

  boot_counts <- counts[boot_indices, , drop = FALSE]
  boot_patients <- boot_patient_list
  boot_grouping <- factor(boot_grouping_list, levels = unique_groups)

  return(list(
    counts = boot_counts,
    patients = boot_patients,
    grouping = boot_grouping
  ))
}

#' Bootstrap patients with random label assignment (TRUE NULL)
#'
#' CRITICAL for Type I error calibration.
#' Pools ALL patients (ignoring original groups) and randomly assigns labels.
#' This breaks the link between labels and composition.
#'
#' @param counts Sample × ASV count matrix
#' @param patient_ids Patient ID for each sample
#' @param grouping Grouping factor (used only to get group levels)
#' @param n_per_group Named vector of sample sizes per group, or single integer for balanced
#' @param seed Random seed
#' @return List with boot_counts, boot_patients, boot_grouping
bootstrap_patients_true_null <- function(counts, patient_ids, grouping,
                                         n_per_group, seed = 42) {
  set.seed(seed)

  # Defensive: ensure patient_ids is character for type-safe comparisons
  patient_ids <- as.character(patient_ids)

  # Get unique groups
  unique_groups <- levels(grouping)

  # Handle n_per_group as single integer (balanced) or named vector
  if (length(n_per_group) == 1) {
    n_per_group <- setNames(rep(n_per_group, length(unique_groups)), unique_groups)
  }

  # Pool ALL patients (ignore original labels)
  all_patients <- unique(patient_ids)

  # Validate we have patients to sample
  if (length(all_patients) == 0) {
    stop("No patients available for bootstrap")
  }

  # Calculate total sample size
  n_total <- sum(n_per_group)

  # Sample n_total patients with replacement
  boot_patients_selected <- sample(all_patients, n_total, replace = TRUE)

  # Create random label assignment
  random_labels <- character(0)
  for (grp in unique_groups) {
    random_labels <- c(random_labels, rep(as.character(grp), n_per_group[[as.character(grp)]]))
  }
  random_labels <- sample(random_labels)  # Shuffle labels

  # Track draw counts for unique IDs
  patient_draw_count <- list()

  boot_indices <- c()
  boot_patient_list <- c()
  boot_grouping_list <- c()

  # Build bootstrap dataset with random labels
  for (i in seq_along(boot_patients_selected)) {
    patient <- boot_patients_selected[i]
    patient_key <- as.character(patient)

    if (is.null(patient_draw_count[[patient_key]])) {
      draw_idx <- 0
    } else {
      draw_idx <- patient_draw_count[[patient_key]]
    }
    patient_draw_count[[patient_key]] <- draw_idx + 1

    # CRITICAL: unique bootstrap ID prevents permutation collapse
    unique_boot_id <- paste0(patient_key, "__b", draw_idx)

    # Get all samples from this patient (CRITICAL: convert both to character for matching)
    patient_samples <- which(as.character(patient_ids) == patient_key)
    boot_indices <- c(boot_indices, patient_samples)
    boot_patient_list <- c(boot_patient_list, rep(unique_boot_id, length(patient_samples)))
    boot_grouping_list <- c(boot_grouping_list, rep(random_labels[i], length(patient_samples)))
  }

  boot_counts <- counts[boot_indices, , drop = FALSE]
  boot_patients <- boot_patient_list
  boot_grouping <- factor(boot_grouping_list, levels = unique_groups)

  return(list(
    counts = boot_counts,
    patients = boot_patients,
    grouping = boot_grouping
  ))
}

#' Aggregate to mean relative abundance by group
#'
#' @param counts Sample × ASV count matrix
#' @param group_ids Group ID for each sample
#' @return Group × ASV matrix
aggregate_mean_relative <- function(counts, group_ids) {
  counts <- as.matrix(counts)
  rs <- rowSums(counts, na.rm = TRUE)
  rs[rs == 0] <- 1
  rel <- counts / rs

  unique_groups <- unique(group_ids)
  group_matrix <- matrix(0, nrow = length(unique_groups), ncol = ncol(counts))
  rownames(group_matrix) <- unique_groups
  colnames(group_matrix) <- colnames(counts)

  for (i in seq_along(unique_groups)) {
    group <- unique_groups[i]
    group_mask <- as.character(group_ids) == as.character(group)
    group_matrix[i, ] <- colMeans(rel[group_mask, , drop = FALSE], na.rm = TRUE)
  }

  group_matrix
}

aggregate_to_patient_level <- function(counts, patient_ids) {
  aggregate_mean_relative(counts, patient_ids)
}

aggregate_to_patient_group_level <- function(counts, patient_ids, grouping) {
  keys <- paste(as.character(patient_ids), as.character(grouping), sep = "|||")
  group_matrix <- aggregate_mean_relative(counts, keys)
  key_df <- data.frame(key = rownames(group_matrix), stringsAsFactors = FALSE)
  parts <- tidyr::separate(key_df, key, into = c("patient", "group"), sep = "\\|\\|\\|")
  list(
    counts = group_matrix,
    patients = parts$patient,
    grouping = factor(parts$group, levels = levels(grouping))
  )
}

#' Bootstrap patients for within-patient designs (preserves sample-level labels)
#'
#' Samples patients with replacement and keeps their original per-sample grouping
#' labels (e.g., BAL/Bronchial Brush/Oral Rinse) intact.
#' Optionally generates a true null by shuffling labels within each bootstrap patient.
#'
#' @param counts Sample × ASV count matrix
#' @param patient_ids Patient ID for each sample
#' @param grouping Grouping factor for each sample
#' @param n_patients Number of patients to sample
#' @param true_null If TRUE, shuffle grouping labels within each bootstrap patient
#' @param seed Random seed
#' @return List with boot_counts, boot_patients, boot_grouping
bootstrap_patients_within <- function(counts, patient_ids, grouping,
                                      n_patients, true_null = FALSE, seed = 42) {
  set.seed(seed)

  patient_ids <- as.character(patient_ids)
  all_patients <- unique(patient_ids)
  if (length(all_patients) == 0) {
    stop("No patients available for bootstrap")
  }

  boot_patients_selected <- sample(all_patients, n_patients, replace = TRUE)
  patient_draw_count <- list()

  boot_indices <- c()
  boot_patient_list <- c()

  for (patient in boot_patients_selected) {
    patient_key <- as.character(patient)
    if (is.null(patient_draw_count[[patient_key]])) {
      draw_idx <- 0
    } else {
      draw_idx <- patient_draw_count[[patient_key]]
    }
    patient_draw_count[[patient_key]] <- draw_idx + 1

    unique_boot_id <- paste0(patient_key, "__b", draw_idx)
    patient_samples <- which(as.character(patient_ids) == patient_key)

    boot_indices <- c(boot_indices, patient_samples)
    boot_patient_list <- c(boot_patient_list, rep(unique_boot_id, length(patient_samples)))
  }

  boot_counts <- counts[boot_indices, , drop = FALSE]
  boot_patients <- boot_patient_list
  boot_grouping <- factor(as.character(grouping[boot_indices]), levels = levels(grouping))

  if (true_null) {
    for (p in unique(boot_patients)) {
      idx <- which(boot_patients == p)
      if (length(idx) > 1) {
        boot_grouping[idx] <- sample(boot_grouping[idx], length(idx), replace = FALSE)
      }
    }
    boot_grouping <- factor(as.character(boot_grouping), levels = levels(grouping))
  }

  return(list(
    counts = boot_counts,
    patients = boot_patients,
    grouping = boot_grouping
  ))
}

#' Apply deterministic spike-in to first group in factor
#'
#' Increases spiked ASVs by fold-change using integer reallocation.
#' Maintains library sizes and preserves zeros.
#' Spikes the first level of the grouping factor.
#'
#' @param counts Sample × ASV count matrix
#' @param grouping Grouping factor
#' @param spike_asv_idx Column indices of ASVs to spike
#' @param fold_change Multiplicative fold-change (e.g., 2.0)
#' @return Spiked count matrix
spike_in_first_group <- function(counts, grouping, spike_asv_idx, fold_change) {
  spiked_counts <- counts
  target_group <- levels(grouping)[1]
  target_mask <- grouping == target_group

  for (i in which(target_mask)) {
    count_vec <- spiked_counts[i, ]
    library_size <- sum(count_vec)

    if (library_size == 0) next

    # Convert to relative abundance
    rel_abund <- count_vec / library_size

    # Spike selected ASVs
    rel_abund[spike_asv_idx] <- rel_abund[spike_asv_idx] * fold_change

    # Renormalize
    rel_abund <- rel_abund / sum(rel_abund)

    # Resample counts (deterministic integer reconciliation)
    new_counts <- rel_abund * library_size
    new_counts_int <- floor(new_counts)
    remainders <- new_counts - new_counts_int
    deficit <- library_size - sum(new_counts_int)

    if (deficit > 0) {
      sorted_idx <- order(remainders, decreasing = TRUE)
      add_idx <- sorted_idx[1:min(deficit, length(sorted_idx))]
      new_counts_int[add_idx] <- new_counts_int[add_idx] + 1
    }

    spiked_counts[i, ] <- new_counts_int
  }

  return(spiked_counts)
}

apply_matrix_transform <- function(mat, method = "none") {
  method <- tolower(method)
  if (method == "none") {
    # Match run_indicspecies.R default behavior: use raw counts.
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

#' Run ISA power simulation (generic for any grouping)
#'
#' @param counts Sample × ASV count matrix
#' @param patient_ids Patient ID for each sample
#' @param grouping Grouping factor
#' @param spike_asv_idx ASV indices to spike (NULL for null scenario)
#' @param spike_fc Fold-change for spike-in
#' @param n_size Sample size (balanced across groups)
#' @param duleg Whether to use duleg=TRUE (combinations allowed)
#' @param use_true_null If TRUE, uses pool-then-label null (for Type I error calibration)
#' @param n_simulations Number of bootstrap simulations
#' @param n_perm Permutations for multipatt
#' @param alpha Significance threshold
#' @param seed Random seed
#' @return List with power, sensitivity, fdr
run_isa_power_generic <- function(counts, patient_ids, grouping,
                                   spike_asv_idx, spike_fc, n_size,
                                   duleg = FALSE,
                                   use_true_null = FALSE,
                                   use_blocking = FALSE,
                                   n_simulations = 1000, n_perm = 199,
                                   alpha = 0.05, seed = 42,
                                   transform = "none") {

  n_total_asvs <- ncol(counts)
  n_spiked <- ifelse(is.null(spike_asv_idx), 0, length(spike_asv_idx))

  power_any <- 0
  sensitivity_sum <- 0
  fdr_sum <- 0
  fdr_conditional_sum <- 0
  n_with_discoveries <- 0

  for (i in 1:n_simulations) {
    # Bootstrap patients
    if (use_blocking) {
      # Within-patient design: sample patients and preserve sample-level labels
      boot_data <- bootstrap_patients_within(
        counts, patient_ids, grouping,
        n_patients = n_size,
        true_null = use_true_null,
        seed = seed + i
      )
    } else {
      if (use_true_null) {
        # Between-patient true null: pool patients and randomly assign labels
        boot_data <- bootstrap_patients_true_null(
          counts, patient_ids, grouping,
          n_per_group = n_size,
          seed = seed + i
        )
      } else {
        # Between-patient observed effect: resample within groups
        boot_data <- bootstrap_patients_generic(
          counts, patient_ids, grouping,
          n_per_group = n_size,
          seed = seed + i
        )
      }
    }

    # Apply spike-in if specified
    if (!is.null(spike_asv_idx) && spike_fc > 1.0) {
      boot_data$counts <- spike_in_first_group(
        boot_data$counts,
        boot_data$grouping,
        spike_asv_idx,
        spike_fc
      )
    }

    if (use_blocking) {
      patient_group_data <- aggregate_to_patient_group_level(
        boot_data$counts,
        boot_data$patients,
        boot_data$grouping
      )
      sample_rel <- apply_matrix_transform(patient_group_data$counts, transform)
      sample_grouping <- droplevels(patient_group_data$grouping)
      patient_blocks <- droplevels(factor(patient_group_data$patients))

      # Run multipatt with blocked permutations
      tryCatch({
        isa_result <- suppressWarnings({
          multipatt(
            x = sample_rel,
            cluster = sample_grouping,
            duleg = duleg,
            control = how(nperm = n_perm, blocks = patient_blocks)
          )
        })
      }, error = function(e) {
        # Skip if multipatt fails (rare)
        isa_result <<- NULL
      })
    } else {
      # Patient-level analysis with standard permutations
      patient_counts <- aggregate_to_patient_level(
        boot_data$counts,
        boot_data$patients
      )

      # Map patients to grouping
      unique_patients <- unique(boot_data$patients)
      patient_grouping <- sapply(unique_patients, function(p) {
        as.character(boot_data$grouping[which(boot_data$patients == p)[1]])
      })
      patient_grouping <- factor(patient_grouping, levels = levels(grouping))

      patient_rel <- apply_matrix_transform(patient_counts, transform)

      # Run multipatt
      tryCatch({
        isa_result <- suppressWarnings({
          multipatt(
            x = patient_rel,
            cluster = patient_grouping,
            duleg = duleg,
            control = how(nperm = n_perm)
          )
        })
      }, error = function(e) {
        # Skip if multipatt fails (rare)
        isa_result <<- NULL
      })
    }

    # Process results if multipatt succeeded
    if (!is.null(isa_result)) {
      tryCatch({

      # Get significant ASVs (FDR-corrected, matching run_indicspecies.R)
      # CRITICAL: Must use FDR correction to match production analysis
      p_values <- isa_result$sign$p.value
      q_values <- p.adjust(p_values, method = "fdr")
      sig_asvs <- which(q_values < alpha)
      n_total_detected <- length(sig_asvs)

      if (n_total_detected > 0) {
        power_any <- power_any + 1
        n_with_discoveries <- n_with_discoveries + 1
      }

        if (n_spiked > 0) {
          # Sensitivity: proportion of spiked ASVs detected
          n_spiked_detected <- sum(spike_asv_idx %in% sig_asvs)
          sensitivity_sum <- sensitivity_sum + (n_spiked_detected / n_spiked)

          # FDR metrics: proportion of null ASVs falsely detected
          null_idx <- setdiff(1:n_total_asvs, spike_asv_idx)
          n_null_detected <- sum(null_idx %in% sig_asvs)

          if (n_total_detected > 0) {
            # Current FDR (diluted across all simulations)
            fdr_sum <- fdr_sum + (n_null_detected / n_total_detected)

            # Conditional FDR (only among simulations with discoveries)
            fdr_conditional_sum <- fdr_conditional_sum + (n_null_detected / n_total_detected)
          }
        }

      }, error = function(e) {
        # Skip if processing fails
      })
    }

    if (i %% 50 == 0) {
      cat(sprintf("\r      %d/%d...", i, n_simulations))
      flush.console()
    }
  }

  cat("\n")

  power <- power_any / n_simulations
  sensitivity <- ifelse(n_spiked > 0, sensitivity_sum / n_simulations, 0)
  fdr <- ifelse(n_spiked > 0, fdr_sum / n_simulations, 0)
  fdr_conditional <- ifelse(n_with_discoveries > 0, fdr_conditional_sum / n_with_discoveries, 0)

  return(list(
    power = power,
    sensitivity = sensitivity,
    fdr = fdr,
    fdr_conditional = fdr_conditional
  ))
}

# ============================================================================
# Main analysis: Loop over grouping columns (matching run_indicspecies.R)
# ============================================================================

# Transpose ASV matrix to samples × ASVs (for multipatt)
count_matrix <- t(asv_mat)

# Get patient IDs for bootstrap
patient_ids_vec <- meta[[args$`patient-col`]]

# Initialize results
all_results <- data.frame()

cat("================================================================\n")
cat("Running power analysis over grouping columns\n")
cat("================================================================\n\n")

# Loop over grouping columns (matching run_indicspecies.R approach)
for (gcol in group_cols) {
  if (!(gcol %in% colnames(meta))) {
    warning("Skipping grouping column '", gcol, "' (not found in metadata).")
    next
  }

  cat("\n--- Grouping Column:", gcol, "---\n")
  sample_sizes_current <- if (gcol == "status") sample_sizes_cancer else sample_sizes_stype

  grouping_vec <- meta[[gcol]]

  # Drop NAs
  keep_idx <- !is.na(grouping_vec)
  grouping_clean <- as.factor(grouping_vec[keep_idx])
  grouping_clean <- droplevels(grouping_clean)

  # Get clean vectors
  counts_clean <- count_matrix[keep_idx, , drop = FALSE]
  patients_clean <- patient_ids_vec[keep_idx]

  if (length(unique(grouping_clean)) < 2) {
    warning("Grouping column '", gcol, "' has <2 groups after filtering; skipping.")
    next
  }

  # Status is analyzed within each sample type (between-patient per site).
  if (gcol == "status") {
    if (is.null(status_site_col) || !(status_site_col %in% colnames(meta))) {
      warning("Sample-type column for status power not found; skipping status power analysis.")
      next
    }
    type_group_vec <- meta[[status_site_col]][keep_idx]
    sites <- unique(as.character(type_group_vec))
    sites <- sites[!is.na(sites)]
    if (length(status_sites_requested) > 0) {
      sites <- sites[sites %in% status_sites_requested]
      if (length(sites) == 0) {
        warning("No matching status sites found after applying --status-sites filter; skipping status power analysis.")
        next
      }
    }

    cat("  Design: BETWEEN-patient stratified by ", status_site_col, "\n", sep = "")
    cat("  Sites:", paste(sites, collapse=", "), "\n\n")

    for (site in sites) {
      site_mask <- as.character(type_group_vec) == site
      counts_site <- counts_clean[site_mask, , drop = FALSE]
      patients_site <- patients_clean[site_mask]
      meta_site <- meta[keep_idx, , drop = FALSE][site_mask, , drop = FALSE]
      grouping_site <- droplevels(as.factor(grouping_clean[site_mask]))

      if (isTRUE(args$`status-exclude-contralateral`) && (as.character(site) %in% status_contralateral_sites)) {
        if (!(effective_contralateral_col %in% colnames(meta_site))) {
          warning("Cannot exclude contralateral samples for status power: column \"", effective_contralateral_col, "\" not found.")
        } else {
          is_cancer_site <- as.character(meta_site[[gcol]]) == as.character(args$`cancer-label`)
          is_contralateral_site <- as.character(meta_site[[effective_contralateral_col]]) == as.character(args$`contralateral-value`)
          keep_site_global <- !(is_cancer_site & is_contralateral_site)
          counts_site <- counts_site[keep_site_global, , drop = FALSE]
          patients_site <- patients_site[keep_site_global]
          grouping_site <- droplevels(grouping_site[keep_site_global])
          meta_site <- meta_site[keep_site_global, , drop = FALSE]
        }
      }

      # Optional: exclude contralateral samples from cancer patients in Bronchial Brush.
      site_label_for_output <- site
      if (isTRUE(args$`status-extra-no-contralateral`) &&
          identical(as.character(site), as.character(args$`lung-brush-label`))) {
        if (!(effective_contralateral_col %in% colnames(meta_site))) {
          warning("Skipping no-contralateral filter for Bronchial Brush status power: column '",
                  effective_contralateral_col, "' not found.")
        } else {
          is_cancer <- as.character(meta_site[[gcol]]) == as.character(args$`cancer-label`)
          is_contralateral <- as.character(meta_site[[effective_contralateral_col]]) ==
            as.character(args$`contralateral-value`)
          keep_no_contra <- !(is_cancer & is_contralateral)
          n_removed <- sum(!keep_no_contra, na.rm = TRUE)
          counts_site <- counts_site[keep_no_contra, , drop = FALSE]
          patients_site <- patients_site[keep_no_contra]
          grouping_site <- droplevels(grouping_site[keep_no_contra])
          meta_site <- meta_site[keep_no_contra, , drop = FALSE]
          site_label_for_output <- paste0(site, " (no contralateral)")
          cat("    Applied no-contralateral filter:", n_removed, "sample(s) removed\n")
        }
      }

      if (length(unique(grouping_site)) < 2) {
        warning("Skipping status power for ", status_site_col, "='", site, "' (<2 status groups).", sep = "")
        next
      }

      cat("  Site:", site, "\n")
      cat("    Status groups:", paste(levels(grouping_site), collapse=", "), "\n")
      cat("    Group sizes:", paste(table(grouping_site), collapse=", "), "\n")
      cat("    Unique patients:", length(unique(patients_site)), "\n")
      cat("    Design: BETWEEN-patient (standard permutations)\n")

      mean_abund <- colMeans(counts_site)
      top_asv_idx <- order(mean_abund, decreasing = TRUE)[1:min(10, ncol(counts_site))]

      # Build scenarios based on user request
      scenarios <- build_scenario_list(requested_scenarios, top_asv_idx)

      for (scenario in scenarios) {
        cat("\n    Scenario:", scenario$name, "(duleg=FALSE)\n")
        if (!is.null(scenario$asv_indices)) {
          cat("      Spiking", length(scenario$asv_indices), "ASVs @", scenario$fold_change, "×\n")
        }

        for (n_cancer in sample_sizes_current) {
          # Hybrid logic: compute n_per_group for cancer vs control
          group_levels <- levels(grouping_site)
          n_per_group <- compute_hybrid_n_per_group(n_cancer, actual_n_control, group_levels)

          cat(sprintf("      n_cancer=%d, n_control=%d", n_per_group[1], n_per_group[2]), "")

          result <- run_isa_power_generic(
            counts_site, patients_site, grouping_site,
            scenario$asv_indices, scenario$fold_change,
            n_per_group,
            duleg = FALSE,
            use_true_null = scenario$use_true_null,
            use_blocking = FALSE,
            n_simulations = args$`n-simulations`,
            n_perm = args$perms,
            alpha = args$alpha,
            seed = args$seed
          )
          cat(sprintf(" → Power=%.3f, Sens=%.3f, FDR=%.3f, FDRc=%.3f\n",
                      result$power, result$sensitivity, result$fdr, result$fdr_conditional))

          all_results <- rbind(all_results, data.frame(
            grouping_col = gcol,
            subgroup_col = "type_group",
            subgroup_level = site_label_for_output,
            duleg = FALSE,
            scenario = scenario$name,
            n_cancer = n_per_group[1],
            n_control = n_per_group[2],
            power = result$power,
            sensitivity = result$sensitivity,
            fdr = result$fdr,
            fdr_conditional = result$fdr_conditional,
            n_simulations = args$`n-simulations`,
            n_perm = args$perms,
            stringsAsFactors = FALSE
          ))

          # Early stopping: if power >= 0.995, skip remaining sample sizes
          if (result$power >= 0.995) {
            cat("      → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.\n")
            break
          }
        }
      }

      if (args$`test-duleg`) {
        for (scenario in scenarios) {
          cat("\n    Scenario:", scenario$name, "(duleg=TRUE)\n")
          if (!is.null(scenario$asv_indices)) {
            cat("      Spiking", length(scenario$asv_indices), "ASVs @", scenario$fold_change, "×\n")
          }

          for (n_cancer in sample_sizes_current) {
            # Hybrid logic: compute n_per_group for cancer vs control
            group_levels <- levels(grouping_site)
            n_per_group <- compute_hybrid_n_per_group(n_cancer, actual_n_control, group_levels)

            cat(sprintf("      n_cancer=%d, n_control=%d", n_per_group[1], n_per_group[2]), "")

            result <- run_isa_power_generic(
              counts_site, patients_site, grouping_site,
              scenario$asv_indices, scenario$fold_change,
              n_per_group,
              duleg = TRUE,
              use_true_null = scenario$use_true_null,
              use_blocking = FALSE,
              n_simulations = args$`n-simulations`,
              n_perm = args$perms,
              alpha = args$alpha,
              seed = args$seed
            )
            cat(sprintf(" → Power=%.3f, Sens=%.3f, FDR=%.3f, FDRc=%.3f\n",
                        result$power, result$sensitivity, result$fdr, result$fdr_conditional))

            all_results <- rbind(all_results, data.frame(
              grouping_col = gcol,
              subgroup_col = "type_group",
              subgroup_level = site_label_for_output,
              duleg = TRUE,
              scenario = scenario$name,
              n_cancer = n_per_group[1],
              n_control = n_per_group[2],
              power = result$power,
              sensitivity = result$sensitivity,
              fdr = result$fdr,
              fdr_conditional = result$fdr_conditional,
              n_simulations = args$`n-simulations`,
              n_perm = args$perms,
              stringsAsFactors = FALSE
            ))

            # Early stopping: if power >= 0.995, skip remaining sample sizes
            if (result$power >= 0.995) {
              cat("      → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.\n")
              break
            }
          }
        }
      }
      cat("\n")
    }
    next
  }

  # Check if this is a within-patient comparison
  patient_group_combos <- meta[keep_idx, ] %>%
    select(all_of(c(args$`patient-col`, gcol))) %>%
    distinct() %>%
    group_by(.data[[args$`patient-col`]]) %>%
    summarise(n_groups = n_distinct(.data[[gcol]]), .groups = 'drop')

  use_blocking <- any(patient_group_combos$n_groups > 1)

  cat("  Groups:", paste(levels(grouping_clean), collapse=", "), "\n")
  cat("  Group sizes:", paste(table(grouping_clean), collapse=", "), "\n")
  cat("  Unique patients:", length(unique(patients_clean)), "\n")

  if (use_blocking) {
    cat("  Design: WITHIN-patient (using blocked permutations)\n\n")
  } else {
    cat("  Design: BETWEEN-patient (standard permutations)\n\n")
  }

  # Select ASVs for spiking (top 10 most abundant overall)
  mean_abund <- colMeans(counts_clean)
  top_asv_idx <- order(mean_abund, decreasing = TRUE)[1:min(10, ncol(counts_clean))]

  # Build scenarios based on user request
  scenarios <- build_scenario_list(requested_scenarios, top_asv_idx)

  # Test duleg=FALSE (single groups)
  for (scenario in scenarios) {
    cat("\n  Scenario:", scenario$name, "(duleg=FALSE)\n")
    if (!is.null(scenario$asv_indices)) {
      cat("    Spiking", length(scenario$asv_indices), "ASVs @", scenario$fold_change, "×\n")
    }

    for (n_size in sample_sizes_current) {
      if (use_blocking) {
        cat("    n_patients =", n_size, "")
      } else {
        cat("    n =", n_size, "")
      }

      result <- run_isa_power_generic(
        counts_clean, patients_clean, grouping_clean,
        scenario$asv_indices, scenario$fold_change,
        n_size,
        duleg = FALSE,
        use_true_null = scenario$use_true_null,
        use_blocking = use_blocking,
        n_simulations = args$`n-simulations`,
        n_perm = args$perms,
        alpha = args$alpha,
        seed = args$seed
      )

      cat(sprintf("→ Power=%.3f, Sens=%.3f, FDR=%.3f, FDRc=%.3f\n",
                  result$power, result$sensitivity, result$fdr, result$fdr_conditional))

      all_results <- rbind(all_results, data.frame(
        grouping_col = gcol,
        subgroup_col = NA_character_,
        subgroup_level = NA_character_,
        duleg = FALSE,
        scenario = scenario$name,
        n_cancer = n_size,
        n_control = n_size,
        power = result$power,
        sensitivity = result$sensitivity,
        fdr = result$fdr,
        fdr_conditional = result$fdr_conditional,
        n_simulations = args$`n-simulations`,
        n_perm = args$perms,
        stringsAsFactors = FALSE
      ))

      # Early stopping: if power >= 0.995, skip remaining sample sizes
      if (result$power >= 0.995) {
        cat("    → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.\n")
        break
      }
    }
  }

  # Test duleg=TRUE (combinations allowed) if requested
  if (args$`test-duleg`) {
    for (scenario in scenarios) {
      cat("\n  Scenario:", scenario$name, "(duleg=TRUE)\n")
      if (!is.null(scenario$asv_indices)) {
        cat("    Spiking", length(scenario$asv_indices), "ASVs @", scenario$fold_change, "×\n")
      }

      for (n_size in sample_sizes_current) {
        if (use_blocking) {
          cat("    n_patients =", n_size, "")
        } else {
          cat("    n =", n_size, "")
        }

        result <- run_isa_power_generic(
          counts_clean, patients_clean, grouping_clean,
          scenario$asv_indices, scenario$fold_change,
          n_size,
          duleg = TRUE,
          use_true_null = scenario$use_true_null,
          use_blocking = use_blocking,
          n_simulations = args$`n-simulations`,
          n_perm = args$perms,
          alpha = args$alpha,
          seed = args$seed
        )

        cat(sprintf("→ Power=%.3f, Sens=%.3f, FDR=%.3f, FDRc=%.3f\n",
                    result$power, result$sensitivity, result$fdr, result$fdr_conditional))

        all_results <- rbind(all_results, data.frame(
          grouping_col = gcol,
          subgroup_col = NA_character_,
          subgroup_level = NA_character_,
          duleg = TRUE,
          scenario = scenario$name,
          n_cancer = n_size,
          n_control = n_size,
          power = result$power,
          sensitivity = result$sensitivity,
          fdr = result$fdr,
          fdr_conditional = result$fdr_conditional,
          n_simulations = args$`n-simulations`,
          n_perm = args$perms,
          stringsAsFactors = FALSE
        ))

        # Early stopping: if power >= 0.995, skip remaining sample sizes
        if (result$power >= 0.995) {
          cat("    → Power ≥ 0.995 reached. Skipping larger sample sizes for this scenario.\n")
          break
        }
      }
    }
  }
}

# ============================================================================
# Save results
# ============================================================================

dir.create(args$outdir, recursive = TRUE, showWarnings = FALSE)
outfile <- file.path(args$outdir, "indicspecies_power_results.tsv")
write.table(all_results, outfile, sep = "\t", row.names = FALSE, quote = FALSE)

cat("\n================================================================\n")
cat("Power analysis complete!\n")
cat("Results saved to:", outfile, "\n")
cat("================================================================\n")
