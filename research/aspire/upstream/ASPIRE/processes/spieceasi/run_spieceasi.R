#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(tibble)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(SpiecEasi)
  library(igraph)
})

# ----------------------------- CLI -------------------------------------------
opt_list <- list(
  make_option("--counts", type="character", default=NULL,
              help="Path to ASV table (TSV). First column = ASV IDs. Other columns = samples."),
  make_option("--transpose", type="logical", default=TRUE,
              help="Transpose so rows=samples, cols=ASVs [default %default]."),
  make_option("--strip-suffix-regex", type="character", default=NULL, dest="strip_suffix_regex",
              help="Regex to strip from sample column names before transpose (e.g. '_[^_]+$')."),
  make_option("--outdir", type="character", default=NULL,
              help="Output directory (required)."),
  make_option("--prefix", type="character", default="spieceasi",
              help="Filename prefix for outputs [default %default]."),

  # Filtering
  make_option("--min-rel-abund", type="double", default=0.0, dest="min_rel_abund",
              help="Keep ASVs reaching at least this relative abundance in >=1 sample. Accepts fraction (0-1) or percent (0-100) [default %default]."),
  make_option("--min-prevalence", type="double", default=0.25, dest="min_prevalence",
              help="Keep ASVs present in at least this prevalence threshold. Accepts fraction (0-1) or percent (0-100) [default %default]."),
  make_option("--force-keep-asvs", type="character", default=NULL, dest="force_keep_asvs",
              help="Optional TSV/list of ASV IDs to retain regardless of min abundance/prevalence filters. ISA summary TSVs are supported; zero-variance filtering still applies."),
  make_option("--remove-zero-var", type="logical", default=TRUE, dest="remove_zero_var",
              help="Drop ASVs with zero variance after filtering [default %default]."),

  # SpiecEasi params
  make_option("--method", type="character", default="glasso",
              help="SpiecEasi method: glasso | mb [default %default]."),
  make_option("--lambda-min-ratio", type="double", default=0.1, dest="lambda_min_ratio",
              help="lambda.min.ratio [default %default]."),
  make_option("--nlambda", type="integer", default=20,
              help="Number of lambda values [default %default]."),
  make_option("--rep-num", type="integer", default=50, dest="rep_num",
              help="pulsar rep.num [default %default]."),
  make_option("--thresh", type="double", default=0.1,
              help="pulsar selection threshold [default %default]."),
  make_option("--pulsar-criterion", type="character", default="bstars", dest="pulsar_criterion",
              help="pulsar model selection criterion: stars | bstars [default %default]."),
  make_option("--ncores", type="integer", default=4,
              help="Number of cores for pulsar [default %default]."),
  make_option("--seed", type="integer", default=10010,
              help="Random seed [default %default]."),

  # Graph construction
  make_option("--edge-threshold", type="double", default=0.1, dest="edge_threshold",
              help="Absolute partial correlation cutoff for thresholded graphs [default %default]."),
  make_option("--keep-negative", type="logical", default=TRUE, dest="keep_negative",
              help="Also write a signed (pos/neg) thresholded network [default %default]."),

  # Layout + viz sizes
  make_option("--layout-iters", type="integer", default=1000, dest="layout_iters",
              help="Fruchterman–Reingold iterations [default %default]."),
  make_option("--pdf-width", type="double", default=10, dest="pdf_width",
              help="PDF width (inches) [default %default]."),
  make_option("--pdf-height", type="double", default=10, dest="pdf_height",
              help="PDF height (inches) [default %default]."),
  make_option("--vsize-offset", type="double", default=6, dest="vsize_offset",
              help="Vertex size offset added to CLR means [default %default]."),
  make_option("--vsize-scale", type="double", default=0.5, dest="vsize_scale",
              help="Vertex size scaling factor [default %default]."),

  # Caching / recompute
  make_option("--force-filter", type="logical", default=FALSE, dest="force_filter",
              help="Recompute filtered counts even if cache exists [default %default]."),
  make_option("--force-spieceasi", type="logical", default=FALSE, dest="force_spieceasi",
              help="Recompute SpiecEasi even if cache exists [default %default]."),
  make_option("--force-graphs", type="logical", default=FALSE, dest="force_graphs",
              help="Recompute igraph + layouts even if cache exists [default %default].")
)

usage <- "%prog --counts ASV_final.micro.tsv --outdir out_dir [--transpose TRUE] [--min-rel-abund 0.0005] [--min-prevalence 0.01]"
opt <- parse_args(OptionParser(option_list = opt_list, usage = usage))

if (is.null(opt$counts) || is.null(opt$outdir)) {
  stop("Please provide --counts and --outdir", call. = FALSE)
}
opt$method <- tolower(opt$method)
if (!(opt$method %in% c("glasso", "mb"))) {
  stop(sprintf("Unsupported --method '%s'. Use one of: glasso, mb", opt$method), call. = FALSE)
}
if (!is.finite(opt$rep_num) || opt$rep_num < 2) {
  stop("--rep-num must be >= 2", call. = FALSE)
}
if (!is.finite(opt$nlambda) || opt$nlambda < 2) {
  stop("--nlambda must be >= 2", call. = FALSE)
}
opt$pulsar_criterion <- tolower(opt$pulsar_criterion)
if (!(opt$pulsar_criterion %in% c("stars", "bstars"))) {
  stop("--pulsar-criterion must be one of: stars, bstars", call. = FALSE)
}
if (!is.finite(opt$ncores) || opt$ncores < 1) {
  stop("--ncores must be >= 1", call. = FALSE)
}
dir.create(opt$outdir, showWarnings = FALSE, recursive = TRUE)

# ----------------------------- Helpers ---------------------------------------
msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), sprintf(...)))
load_if_exists <- function(path) if (file.exists(path)) readRDS(path) else FALSE
save_csv <- function(x, path) { write.csv(x, path, row.names = FALSE); msg("Wrote %s", path) }
normalize_asv_ids <- function(x) {
  x <- trimws(as.character(x))
  x <- x[!is.na(x) & nzchar(x)]
  unique(sub(";size=[0-9]+.*$", "", x, perl = TRUE))
}
read_force_keep_asvs <- function(path) {
  if (is.null(path) || !nzchar(path)) {
    return(character(0))
  }
  if (!file.exists(path)) {
    stop(sprintf("--force-keep-asvs file does not exist: %s", path), call. = FALSE)
  }
  if (file.info(path)$size == 0) {
    return(character(0))
  }

  keep_tbl <- suppressMessages(readr::read_tsv(path, col_types = readr::cols(.default = "c"), progress = FALSE))
  if (nrow(keep_tbl) == 0 || ncol(keep_tbl) == 0) {
    return(character(0))
  }

  if ("ASV" %in% names(keep_tbl)) {
    keep_rows <- rep(TRUE, nrow(keep_tbl))
    if ("significant" %in% names(keep_tbl)) {
      keep_rows <- keep_rows & tolower(keep_tbl$significant) %in% c("true", "t", "1", "yes")
    }
    if ("q.value" %in% names(keep_tbl)) {
      q_values <- suppressWarnings(as.numeric(keep_tbl$q.value))
      keep_rows <- keep_rows & !is.na(q_values) & q_values < 0.05
    } else if ("q_value" %in% names(keep_tbl)) {
      q_values <- suppressWarnings(as.numeric(keep_tbl$q_value))
      keep_rows <- keep_rows & !is.na(q_values) & q_values < 0.05
    }
    return(normalize_asv_ids(keep_tbl$ASV[keep_rows]))
  }

  normalize_asv_ids(keep_tbl[[1]])
}
safe_write_graph <- function(graph, path, format, required = TRUE) {
  tryCatch(
    {
      write_graph(graph, file = path, format = format)
      msg("Wrote %s", path)
      TRUE
    },
    error = function(err) {
      if (file.exists(path)) {
        unlink(path)
      }
      if (required) {
        stop(err)
      }
      msg("WARNING: Failed to write optional %s graph (%s): %s", toupper(format), basename(path), conditionMessage(err))
      FALSE
    }
  )
}

normalize_fraction_threshold <- function(x, label) {
  if (!is.finite(x)) {
    stop(sprintf("%s must be a finite number.", label), call. = FALSE)
  }
  if (x < 0) {
    stop(sprintf("%s must be >= 0.", label), call. = FALSE)
  }
  if (x > 1 && x <= 100) {
    x_old <- x
    x <- x / 100
    msg("%s interpreted as percent: %.6g -> %.6g", label, x_old, x)
    return(x)
  }
  if (x > 100) {
    stop(sprintf("%s=%.6g is out of range. Use fraction (0-1) or percent (0-100).", label, x), call. = FALSE)
  }
  x
}

opt$min_rel_abund <- normalize_fraction_threshold(opt$min_rel_abund, "--min-rel-abund")
opt$min_prevalence <- normalize_fraction_threshold(opt$min_prevalence, "--min-prevalence")

safe_clr <- function(x, margin = 1) {
  x <- as.matrix(x)
  if (any(x == 0, na.rm = TRUE)) x <- x + 1
  SpiecEasi::clr(x, margin)
}

get_precision_at_opt <- function(se) {
  if (!is.null(se$est$icov)) {
    idx <- tryCatch(se$select$stars$opt.index, error = function(e) NA_integer_)
    if (is.na(idx) || idx < 1 || idx > length(se$est$icov)) idx <- length(se$est$icov)
    return(as.matrix(se$est$icov[[idx]]))
  }
  stop("Could not find precision (icov) in SpiecEasi object.")
}

precision_to_partial_cor <- function(Theta) {
  d <- diag(Theta)
  d[d == 0 | is.na(d) | is.infinite(d)] <- 1e-8
  D <- diag(1 / sqrt(d))
  P <- - D %*% Theta %*% D
  diag(P) <- 0
  P
}

adj_from_partial <- function(P, threshold, positive_only = TRUE) {
  A <- P
  if (positive_only) {
    A[A < 0] <- 0
    A[abs(A) < threshold] <- 0
  } else {
    A[abs(A) < threshold] <- 0
  }
  A <- (A + t(A)) / 2
  diag(A) <- 0
  A
}

run_spieceasi_with_function_object <- function(data, opt) {
  ns <- asNamespace("SpiecEasi")
  estimator_fun <- get("sparseiCov", envir = ns)
  norm_fun <- get(".spiec.easi.norm", envir = ns)
  get_max_cov <- get("getMaxCov", envir = ns)
  get_lam_path <- get("getLamPath", envir = ns)

  msg("Applying data transformations...")
  X <- norm_fun(data)

  fargs <- list(method = opt$method)
  lambda_max <- get_max_cov(cor(X))
  fargs$lambda <- get_lam_path(
    lambda_max,
    lambda_max * opt$lambda_min_ratio,
    opt$nlambda,
    log = TRUE
  )

  requested_criterion <- opt$pulsar_criterion
  actual_criterion <- if (requested_criterion == "bstars") "stars" else requested_criterion
  bounded_stars <- requested_criterion == "bstars"
  if (bounded_stars) {
    msg("Selecting model with pulsar using bounded StARS (criterion=stars, lb.stars=TRUE, ub.stars=TRUE)...")
  } else {
    msg("Selecting model with pulsar using %s...", actual_criterion)
  }
  est <- pulsar::pulsar(
    data = X,
    fun = estimator_fun,
    fargs = fargs,
    rep.num = opt$rep_num,
    seed = opt$seed,
    ncores = opt$ncores,
    thresh = opt$thresh,
    criterion = actual_criterion,
    lb.stars = bounded_stars,
    ub.stars = bounded_stars
  )
  est[[actual_criterion]]$opt.index <- pulsar::opt.index(est, actual_criterion)
  if (is.null(est$stars)) {
    est$stars <- est[[actual_criterion]]
  }
  est$aspire_requested_criterion <- requested_criterion
  est$aspire_actual_criterion <- actual_criterion
  est$aspire_bounded_stars <- bounded_stars

  msg("Fitting final estimate with %s...", opt$method)
  suppressWarnings(fit <- pulsar::refit(est))
  fit$select <- est
  fit$lambda <- fargs$lambda
  fit$fun <- estimator_fun
  msg("done")
  fit
}

# ----------------------------- I/O & filtering -------------------------------
counts_path <- normalizePath(opt$counts)
prefix <- file.path(normalizePath(opt$outdir), opt$prefix)

cache_counts <- paste0(prefix, "_count_data_filtered.rds")
cache_spiece <- paste0(prefix, "_spiec_easi.rds")
cache_graph  <- paste0(prefix, "_igraph_main.rds")
cache_layout <- paste0(prefix, "_layout_nicely.rds")

msg("Reading counts from: %s", counts_path)
raw <- suppressMessages(
  readr::read_tsv(counts_path, show_col_types = FALSE)
)
raw <- tibble::as_tibble(raw, .name_repair = "minimal")

# First column = ASV IDs, rest must be numeric
id_col <- names(raw)[1]
asv_ids <- raw[[id_col]]
mat_df  <- dplyr::select(raw, -all_of(id_col))

# Force numeric (coerce if needed)
mat <- do.call(cbind, lapply(mat_df, function(x) suppressWarnings(as.numeric(as.character(x)))))
if (is.null(dim(mat))) mat <- matrix(mat, ncol = ncol(mat_df))
colnames(mat) <- colnames(mat_df)
rownames(mat) <- asv_ids
storage.mode(mat) <- "double"
if (anyDuplicated(rownames(mat))) {
  dup_n <- sum(duplicated(rownames(mat)))
  msg("Found %d duplicated ASV IDs; applying make.unique() to row names.", dup_n)
  rownames(mat) <- make.unique(rownames(mat))
}

bad_vals <- sum(!is.finite(mat))
if (bad_vals > 0) {
  msg("Found %d non-finite values in count matrix; replacing with 0.", bad_vals)
  mat[!is.finite(mat)] <- 0
}
neg_vals <- sum(mat < 0, na.rm = TRUE)
if (neg_vals > 0) {
  msg("Found %d negative count values; clipping to 0.", neg_vals)
  mat[mat < 0] <- 0
}

# Optional sample name cleanup
if (!is.null(opt$strip_suffix_regex)) {
  colnames(mat) <- sub(opt$strip_suffix_regex, "", colnames(mat), perl = TRUE)
}

# SpiecEasi expects rows = samples, cols = features (ASVs)
if (isTRUE(opt$transpose)) {
  mat <- t(mat)
}
if (nrow(mat) < 2 || ncol(mat) < 2) {
  stop(sprintf("Input matrix is too small after loading/transposition: samples=%d, ASVs=%d", nrow(mat), ncol(mat)), call. = FALSE)
}

force_keep_asvs <- read_force_keep_asvs(opt$force_keep_asvs)
force_keep_present <- intersect(force_keep_asvs, colnames(mat))
if (length(force_keep_asvs) > 0) {
  msg(
    "Loaded %d force-keep ASVs from %s; %d are present in the count table.",
    length(force_keep_asvs),
    opt$force_keep_asvs,
    length(force_keep_present)
  )
  missing_force_keep <- setdiff(force_keep_asvs, colnames(mat))
  if (length(missing_force_keep) > 0) {
    msg("WARNING: %d force-keep ASVs are absent from the count table and cannot be retained.", length(missing_force_keep))
  }
}

force_filter_effective <- isTRUE(opt$force_filter) || length(force_keep_present) > 0
force_spieceasi_effective <- isTRUE(opt$force_spieceasi) || length(force_keep_present) > 0
force_graphs_effective <- isTRUE(opt$force_graphs) || length(force_keep_present) > 0

# Filter (cached)
count_data_filtered <- if (isFALSE(force_filter_effective)) load_if_exists(cache_counts) else FALSE

if (identical(count_data_filtered, FALSE)) {
  msg("Filtering ASVs ...")
  keep <- rep(TRUE, ncol(mat))

  if (opt$min_rel_abund > 0) {
    rs <- rowSums(mat, na.rm = TRUE)
    rs[rs == 0] <- 1
    rel <- mat / rs
    max_ra <- apply(rel, 2, max, na.rm = TRUE)
    keep <- keep & (max_ra >= opt$min_rel_abund)
  }

  if (opt$min_prevalence > 0) {
    prev <- colSums(mat > 0, na.rm = TRUE) / nrow(mat)
    keep <- keep & (prev >= opt$min_prevalence)
  }

  if (length(force_keep_present) > 0) {
    keep <- keep | (colnames(mat) %in% force_keep_present)
    msg("Force-retaining %d ASVs through abundance/prevalence filtering.", length(force_keep_present))
  }

  mat_f <- mat[, keep, drop = FALSE]

  if (isTRUE(opt$remove_zero_var) && ncol(mat_f) > 0) {
    v <- apply(mat_f, 2, var, na.rm = TRUE)
    dropped_force_keep <- intersect(force_keep_present, colnames(mat_f)[v <= 0])
    if (length(dropped_force_keep) > 0) {
      msg("WARNING: Dropping %d force-kept ASVs with zero variance after filtering.", length(dropped_force_keep))
    }
    mat_f <- mat_f[, v > 0, drop = FALSE]
  }
  if (nrow(mat_f) < 2 || ncol(mat_f) < 2) {
    stop(
      sprintf(
        "Filtering removed too much data (samples=%d, ASVs=%d). Consider lowering --min-rel-abund/--min-prevalence or disabling --remove-zero-var.",
        nrow(mat_f), ncol(mat_f)
      ),
      call. = FALSE
    )
  }

  count_data_filtered <- mat_f
  saveRDS(count_data_filtered, cache_counts)
  msg("Saved filtered counts: %s  (samples=%d, ASVs=%d)", cache_counts, nrow(count_data_filtered), ncol(count_data_filtered))
} else {
  msg("Loaded filtered counts: %s  (samples=%d, ASVs=%d)", cache_counts, nrow(count_data_filtered), ncol(count_data_filtered))
}

stopifnot(nrow(count_data_filtered) > 1, ncol(count_data_filtered) > 1)

# ----------------------------- SpiecEasi -------------------------------------
se_obj <- if (isFALSE(force_spieceasi_effective)) load_if_exists(cache_spiece) else FALSE

if (identical(se_obj, FALSE)) {
  set.seed(opt$seed)
  msg("Running SpiecEasi (%s) ...", opt$method)
  if (opt$method %in% c("glasso", "mb")) {
    se_obj <- run_spieceasi_with_function_object(count_data_filtered, opt)
  } else {
    pargs <- list(rep.num = opt$rep_num, seed = opt$seed, ncores = opt$ncores, thresh = opt$thresh)
    se_obj <- SpiecEasi::spiec.easi(
      count_data_filtered,
      method = opt$method,
      lambda.min.ratio = opt$lambda_min_ratio,
      nlambda = opt$nlambda,
      pulsar.params = pargs
    )
  }
  saveRDS(se_obj, cache_spiece)
  msg("Saved SpiecEasi object: %s", cache_spiece)
} else {
  msg("Loaded SpiecEasi object: %s", cache_spiece)
}

msg("Samples: %d, ASVs: %d", nrow(count_data_filtered), ncol(count_data_filtered))

# -------- Precision / Partial Cor / STARS (export CSVs for inspection) -------
msg("Extracting precision and partial correlations ...")
Theta <- get_precision_at_opt(se_obj)
stopifnot(nrow(Theta) == ncol(count_data_filtered), ncol(Theta) == ncol(count_data_filtered))

# Ensure dimnames exist on Theta (SpiecEasi sometimes omits them)
asv_names <- colnames(count_data_filtered)
if (is.null(rownames(Theta)) || is.null(colnames(Theta))) {
  rownames(Theta) <- asv_names
  colnames(Theta) <- asv_names
}

pcor <- precision_to_partial_cor(Theta)
# Set names on pcor too (safe if already set)
rownames(pcor) <- asv_names
colnames(pcor) <- asv_names

# Export matrices
save_csv(tibble(ASV_ID = rownames(Theta)) %>% bind_cols(as.data.frame(Theta)),
         paste0(prefix, "_precision_matrix.csv"))
save_csv(tibble(ASV_ID = rownames(pcor)) %>% bind_cols(as.data.frame(pcor)),
         paste0(prefix, "_partial_correlation_matrix.csv"))

if (!is.null(se_obj$refit$stars)) {
  adj_bin <- as.matrix(se_obj$refit$stars)
  colnames(adj_bin) <- colnames(count_data_filtered)
  rownames(adj_bin) <- colnames(count_data_filtered)
  save_csv(tibble(ASV_ID = rownames(adj_bin)) %>% bind_cols(as.data.frame(adj_bin)),
           paste0(prefix, "_adj_STARS_matrix.csv"))
}

# ----------------------------- Graphs + Layouts ------------------------------
ig_main  <- if (isFALSE(force_graphs_effective)) load_if_exists(cache_graph)  else FALSE
am_coord <- if (isFALSE(force_graphs_effective)) load_if_exists(cache_layout) else FALSE

if (identical(ig_main, FALSE) || identical(am_coord, FALSE)) {
  msg("Building graphs ...")

  # Canonical positive graph: always respect edge_threshold.
  # With edge_threshold=0, this is equivalent to the old all-positive graph.
  A_pos_thr <- adj_from_partial(pcor, threshold = opt$edge_threshold, positive_only = TRUE)
  ig_main   <- graph_from_adjacency_matrix(A_pos_thr, mode = "undirected", weighted = TRUE, diag = FALSE)

  # Keep the historical POS_ALL filenames, but make them reflect the same
  # thresholded positive edge set so every downstream table/plot stays aligned.
  A_pos_all <- A_pos_thr
  ig_pos_all <- graph_from_adjacency_matrix(A_pos_all, mode = "undirected", weighted = TRUE, diag = FALSE)

  # Signed thresholded (optional)
  ig_signed <- NULL
  if (isTRUE(opt$keep_negative)) {
    A_signed_thr <- adj_from_partial(pcor, threshold = opt$edge_threshold, positive_only = FALSE)
    ig_signed <- graph_from_adjacency_matrix(A_signed_thr, mode = "undirected", weighted = TRUE, diag = FALSE)
    E(ig_signed)$color <- ifelse(E(ig_signed)$weight > 0, "red", "blue")
  }

  # --- Vertex sizes from CLR means (always per-ASV), robust to orientation ---
  # Expected: count_data_filtered has rows=samples, cols=ASVs. But we guard anyway.
  asv_names <- colnames(count_data_filtered)
  stopifnot(!is.null(asv_names))

  clr_mat <- safe_clr(count_data_filtered, margin = 1)
  msg("CLR matrix dims: %d x %d (rows x cols)", nrow(clr_mat), ncol(clr_mat))
  msg("ASV (column) count: %d", length(asv_names))

  # Compute means along the dimension that matches ASVs
  if (ncol(clr_mat) == length(asv_names)) {
    vsize_vec <- colMeans(clr_mat, na.rm = TRUE)
  } else if (nrow(clr_mat) == length(asv_names)) {
    # Orientation is flipped; take row means instead
    vsize_vec <- rowMeans(clr_mat, na.rm = TRUE)
  } else {
    stop(sprintf("Cannot align CLR matrix (%dx%d) to %d ASVs.",
                nrow(clr_mat), ncol(clr_mat), length(asv_names)))
  }

  # Scale/offset and align by vertex name
  vsize_vec <- (as.numeric(vsize_vec) + opt$vsize_offset) * opt$vsize_scale

  # Ensure graph vertices are named with ASV IDs
  if (is.null(V(ig_main)$name)) V(ig_main)$name <- asv_names

  # Name the size vector with the ASV IDs it represents
  names(vsize_vec) <- asv_names

  # Pull sizes in graph vertex order
  vsize <- vsize_vec[V(ig_main)$name]

  # Final sanity & flooring
  vsize[!is.finite(vsize)] <- 1
  vsize <- pmax(vsize, 1)

  msg("Sizes aligned: length(vsize)=%d, vcount=%d, ASVs=%d",
      length(vsize), vcount(ig_main), length(asv_names))
  stopifnot(length(vsize) == vcount(ig_main))

  # Layouts
  set.seed(opt$seed)
  layout_nicely_coords <- layout_nicely(ig_main); if (!is.matrix(layout_nicely_coords)) layout_nicely_coords <- matrix(layout_nicely_coords, ncol=2)
  layout_fr_coords     <- layout_with_fr(ig_main, niter = opt$layout_iters); if (!is.matrix(layout_fr_coords)) layout_fr_coords <- matrix(layout_fr_coords, ncol=2)
  layout_kk_coords     <- layout_with_kk(ig_main); if (!is.matrix(layout_kk_coords)) layout_kk_coords <- matrix(layout_kk_coords, ncol=2)
  layout_drl_coords    <- layout_with_drl(ig_main); if (!is.matrix(layout_drl_coords)) layout_drl_coords <- matrix(layout_drl_coords, ncol=2)

  # Cache main objects
  saveRDS(ig_main,  cache_graph);  msg("Saved igraph:  %s", cache_graph)
  saveRDS(layout_nicely_coords, cache_layout); msg("Saved layout: %s", cache_layout)

  # Multipage PDF of layouts
  pdf(file = paste0(prefix, "_multipage_layouts.pdf"), width = opt$pdf_width, height = opt$pdf_height)
  plot(ig_main, layout = layout_nicely_coords, vertex.size = vsize, vertex.label = NA, main = "layout_nicely")
  plot(ig_main, layout = layout_fr_coords,     vertex.size = vsize, vertex.label = NA, main = sprintf("Fruchterman-Reingold (niter=%d)", opt$layout_iters))
  plot(ig_main, layout = layout_kk_coords,     vertex.size = vsize, vertex.label = NA, main = "Kamada-Kawai")
  plot(ig_main, layout = layout_drl_coords,    vertex.size = vsize, vertex.label = NA, main = "DRL")
  dev.off()
  msg("Wrote %s_multipage_layouts.pdf", prefix)

  # GraphML is required downstream. GML is best-effort only because some
  # igraph builds fail globally on GML export, even for trivial graphs.
  safe_write_graph(ig_main,    paste0(prefix, "_network_pos_thr.graphml"),  format = "graphml", required = TRUE)
  safe_write_graph(ig_main,    paste0(prefix, "_network_pos_thr.gml"),      format = "gml",     required = FALSE)
  safe_write_graph(ig_pos_all, paste0(prefix, "_network_pos_all.graphml"),  format = "graphml", required = TRUE)
  safe_write_graph(ig_pos_all, paste0(prefix, "_network_pos_all.gml"),      format = "gml",     required = FALSE)
  if (!is.null(ig_signed)) {
    safe_write_graph(ig_signed,  paste0(prefix, "_network_signed_thr.graphml"), format = "graphml", required = TRUE)
    safe_write_graph(ig_signed,  paste0(prefix, "_network_signed_thr.gml"),     format = "gml",     required = FALSE)
  }

  # Export weighted adjacency matrices
  save_csv(tibble(ASV_ID = colnames(A_pos_thr)) %>% bind_cols(as.data.frame(A_pos_thr)),
           paste0(prefix, "_adj_weighted_pos_thr.csv"))
  save_csv(tibble(ASV_ID = colnames(A_pos_all)) %>% bind_cols(as.data.frame(A_pos_all)),
           paste0(prefix, "_adj_weighted_pos_all.csv"))
  if (!is.null(ig_signed)) {
    save_csv(tibble(ASV_ID = colnames(A_signed_thr)) %>% bind_cols(as.data.frame(A_signed_thr)),
             paste0(prefix, "_adj_weighted_signed_thr.csv"))
  }
} else {
  msg("Loaded igraph + layout from cache.")
  # The cached graph is the canonical positive graph used for downstream
  # node summaries, so mirror it here when graph reconstruction is skipped.
  ig_pos_all <- ig_main
}

# ----------------------------- Edge list -------------------------------------
if (is.null(V(ig_main)$name)) V(ig_main)$name <- colnames(count_data_filtered)

edges_df <- as_edgelist(ig_main) %>% as.data.frame()
colnames(edges_df) <- c("Taxon1", "Taxon2")
edges_df$Weight <- E(ig_main)$weight
save_csv(edges_df, paste0(prefix, "_edge_list.csv"))

# -------------------------- Node centralities --------------------------------
# Use the all-positive graph as the canonical node summary source so Degree
# matches every downstream POS_ALL plot/table. Keep the thresholded degree as
# an explicit audit column.
node_degree            <- igraph::degree(ig_pos_all)
node_degree_threshold  <- igraph::degree(ig_main)
node_betweenness_raw   <- igraph::betweenness(ig_pos_all, normalized = FALSE)
node_betweenness_norm  <- igraph::betweenness(ig_pos_all, normalized = TRUE)
node_closeness         <- igraph::closeness(ig_pos_all)
node_eigen             <- igraph::eigen_centrality(ig_pos_all)$vector
node_ids               <- paste0("n", seq_along(V(ig_pos_all)) - 1)

node_features <- data.frame(
  GraphML_ID   = node_ids,
  Taxon        = V(ig_pos_all)$name,
  Degree       = node_degree,
  Degree_thresholded = node_degree_threshold,
  Betweenness  = node_betweenness_norm,
  Betweenness_raw = node_betweenness_raw,
  Betweenness_norm = node_betweenness_norm,
  Closeness    = node_closeness,
  EigenCentral = node_eigen
)
save_csv(node_features, paste0(prefix, "_node_features.csv"))

msg("Done. Outputs under: %s", normalizePath(opt$outdir))
