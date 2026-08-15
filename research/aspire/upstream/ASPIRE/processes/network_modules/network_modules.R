#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(optparse)
  library(igraph)
  library(readr)
  library(dplyr)
})

opt_list <- list(
  make_option("--graph-sub", type = "character", default = NULL, dest = "graph_sub",
              help = "Thresholded positive GraphML (required)."),
  make_option("--graph-all", type = "character", default = NULL, dest = "graph_all",
              help = "All-positive GraphML (optional)."),
  make_option("--outdir", type = "character", default = NULL, dest = "outdir",
              help = "Output directory (required)."),
  make_option("--prefix", type = "character", default = "spieceasi", help = "Output prefix [default %default]."),
  make_option("--methods", type = "character", default = "leiden,louvain", help = "Comma-separated methods [default %default]."),
  make_option("--primary-method", type = "character", default = "leiden", dest = "primary_method",
              help = "Primary method for consensus partition [default %default]."),
  make_option("--reps", type = "integer", default = 25, help = "Repetitions per method/resolution [default %default]."),
  make_option("--resolutions", type = "character", default = "0.5,1.0,1.5",
              help = "Comma-separated resolution values [default %default]."),
  make_option("--consensus-threshold", type = "double", default = 0.8, dest = "consensus_threshold",
              help = "Co-assignment threshold for consensus graph [default %default]."),
  make_option("--seed", type = "integer", default = 42, help = "Base random seed [default %default]."),
  make_option("--max-dense-nodes", type = "integer", default = 5000, dest = "max_dense_nodes",
              help = "Maximum node count for dense co-assignment matrix [default %default].")
)

usage <- "%prog --graph-sub sub.graphml --outdir outdir [--graph-all all.graphml]"
opt <- parse_args(OptionParser(option_list = opt_list, usage = usage))

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), sprintf(...)))
`%||%` <- function(a, b) if (is.null(a) || (is.character(a) && !nzchar(a))) b else a

# Robust key access across optparse variants that normalize dashes differently.
graph_sub <- opt$graph_sub %||% opt$graph.sub %||% opt[["graph-sub"]]
graph_all <- opt$graph_all %||% opt$graph.all %||% opt[["graph-all"]]
outdir <- opt$outdir

if (is.null(graph_sub) || is.null(outdir)) {
  stop("Please provide --graph-sub and --outdir", call. = FALSE)
}

dir.create(outdir, showWarnings = FALSE, recursive = TRUE)

parse_csv_char <- function(x) {
  vals <- unlist(strsplit(as.character(x), ",", fixed = TRUE))
  vals <- trimws(vals)
  vals[vals != ""]
}

parse_csv_num <- function(x) {
  vals <- suppressWarnings(as.numeric(parse_csv_char(x)))
  vals[is.finite(vals)]
}

sanitize_graph <- function(g) {
  if (is.directed(g)) {
    g <- as.undirected(g, mode = "collapse", edge.attr.comb = list(weight = "mean", "ignore"))
  }
  g <- simplify(g, remove.multiple = TRUE, remove.loops = TRUE, edge.attr.comb = list(weight = "mean", "ignore"))
  if (is.null(V(g)$name)) {
    V(g)$name <- as.character(seq_len(vcount(g)))
  } else {
    V(g)$name <- as.character(V(g)$name)
  }
  if (ecount(g) > 0) {
    w <- E(g)$weight
    if (is.null(w)) w <- rep(1, ecount(g))
    w <- suppressWarnings(as.numeric(w))
    w[!is.finite(w)] <- 1
    w[w < 0] <- abs(w[w < 0])
    w[w == 0] <- 1e-6
    E(g)$weight <- w
  }
  g
}

load_graph_safe <- function(path) {
  if (is.null(path) || !nzchar(path) || !file.exists(path)) return(NULL)
  msg("Reading graph: %s", path)
  g <- read_graph(path, format = "graphml")
  sanitize_graph(g)
}

run_partition <- function(g, method, resolution, seed) {
  set.seed(seed)
  weights <- if (ecount(g) > 0) E(g)$weight else NULL
  method <- tolower(method)
  comm <- NULL

  if (method == "leiden") {
    if ("cluster_leiden" %in% getNamespaceExports("igraph")) {
      f <- get("cluster_leiden", envir = asNamespace("igraph"))
      args <- list(graph = g, objective_function = "modularity", weights = weights)
      if ("resolution_parameter" %in% names(formals(f))) args$resolution_parameter <- resolution
      if ("n_iterations" %in% names(formals(f))) args$n_iterations <- -1
      comm <- tryCatch(do.call(f, args), error = function(e) NULL)
    }
    if (is.null(comm)) {
      method <- "louvain"
    }
  }

  if (is.null(comm) && method == "louvain") {
    f <- get("cluster_louvain", envir = asNamespace("igraph"))
    args <- list(graph = g, weights = weights)
    if ("resolution" %in% names(formals(f))) args$resolution <- resolution
    comm <- do.call(f, args)
  }

  membership_vec <- as.integer(membership(comm))
  names(membership_vec) <- V(g)$name
  modularity_val <- tryCatch(modularity(g, membership_vec, weights = weights), error = function(e) NA_real_)
  list(
    membership = membership_vec,
    modularity = modularity_val,
    n_modules = length(unique(membership_vec)),
    method_used = method
  )
}

mean_pairwise_nmi <- function(partitions) {
  k <- length(partitions)
  if (k < 2) return(NA_real_)
  vals <- c()
  for (i in seq_len(k - 1)) {
    for (j in (i + 1):k) {
      vals <- c(vals, compare(partitions[[i]], partitions[[j]], method = "nmi"))
    }
  }
  if (length(vals) == 0) return(NA_real_)
  mean(vals, na.rm = TRUE)
}

select_medoid_partition <- function(partitions) {
  k <- length(partitions)
  if (k == 1) return(partitions[[1]])
  sim <- matrix(0, nrow = k, ncol = k)
  for (i in seq_len(k)) sim[i, i] <- 1
  for (i in seq_len(k - 1)) {
    for (j in (i + 1):k) {
      s <- compare(partitions[[i]], partitions[[j]], method = "nmi")
      sim[i, j] <- s
      sim[j, i] <- s
    }
  }
  medoid <- which.max(rowMeans(sim))
  partitions[[medoid]]
}

consensus_partition <- function(g, partitions, primary_method, primary_resolution, consensus_threshold, seed, max_dense_nodes) {
  n <- vcount(g)
  node_names <- V(g)$name

  if (length(partitions) == 0) {
    return(list(
      membership = rep(seq_len(n), length.out = n),
      node_stability = rep(NA_real_, n),
      mean_nmi = NA_real_,
      used_dense = FALSE
    ))
  }

  if (n > max_dense_nodes) {
    msg("Node count (%d) exceeds max_dense_nodes (%d); using medoid partition fallback.", n, max_dense_nodes)
    med <- select_medoid_partition(partitions)
    med <- med[node_names]
    return(list(
      membership = as.integer(med),
      node_stability = rep(NA_real_, n),
      mean_nmi = mean_pairwise_nmi(partitions),
      used_dense = FALSE
    ))
  }

  coassoc <- matrix(0, nrow = n, ncol = n)
  for (mem in partitions) {
    mem <- mem[node_names]
    groups <- split(seq_len(n), mem)
    for (idx in groups) {
      coassoc[idx, idx] <- coassoc[idx, idx] + 1
    }
  }
  coassoc <- coassoc / length(partitions)
  diag(coassoc) <- 0

  adj <- coassoc
  adj[adj < consensus_threshold] <- 0
  g_cons <- graph_from_adjacency_matrix(adj, mode = "undirected", weighted = TRUE, diag = FALSE)
  V(g_cons)$name <- node_names
  g_cons <- sanitize_graph(g_cons)

  if (ecount(g_cons) == 0) {
    msg("Consensus graph has no edges at threshold %.3f; using medoid partition fallback.", consensus_threshold)
    mem_final <- select_medoid_partition(partitions)[node_names]
  } else {
    cp <- run_partition(g_cons, primary_method, primary_resolution, seed)
    mem_final <- cp$membership[node_names]
  }

  mem_final <- setNames(as.integer(mem_final), node_names)
  stab <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    same <- which(mem_final == mem_final[i] & seq_len(n) != i)
    if (length(same) > 0) {
      stab[i] <- mean(coassoc[i, same], na.rm = TRUE)
    } else {
      stab[i] <- 1.0
    }
  }

  list(
    membership = mem_final,
    node_stability = stab,
    mean_nmi = mean_pairwise_nmi(partitions),
    used_dense = TRUE
  )
}

analyze_variant <- function(g, variant, methods, primary_method, reps, resolutions, consensus_threshold, seed, max_dense_nodes) {
  node_names <- V(g)$name
  weights <- if (ecount(g) > 0) E(g)$weight else NULL

  if (vcount(g) < 2 || ecount(g) == 0) {
    msg("Variant '%s' has <2 nodes or no edges; assigning singleton modules.", variant)
    mem <- seq_len(vcount(g))
    assign_df <- tibble(
      Taxon = node_names,
      module_id = as.integer(mem),
      module_label = paste0("M", as.integer(mem)),
      node_stability = NA_real_,
      graph_variant = variant,
      method = "singleton"
    )
    summary_df <- tibble(
      graph_variant = variant,
      method = "singleton",
      n_nodes = vcount(g),
      n_edges = ecount(g),
      n_modules = length(unique(mem)),
      modularity_consensus = NA_real_,
      modularity_mean_runs = NA_real_,
      modularity_sd_runs = NA_real_,
      mean_pairwise_nmi = NA_real_,
      consensus_threshold = consensus_threshold
    )
    runs_df <- tibble(
      graph_variant = variant,
      run_id = integer(),
      method = character(),
      method_used = character(),
      resolution = double(),
      rep = integer(),
      seed = integer(),
      modularity = double(),
      n_modules = integer()
    )
    return(list(assignments = assign_df, summary = summary_df, runs = runs_df))
  }

  run_rows <- list()
  partitions <- list()
  run_id <- 0
  for (m in methods) {
    for (res in resolutions) {
      for (r in seq_len(reps)) {
        run_id <- run_id + 1
        seed_i <- seed + run_id
        part <- run_partition(g, m, res, seed_i)
        partitions[[run_id]] <- part$membership[node_names]
        run_rows[[run_id]] <- tibble(
          graph_variant = variant,
          run_id = run_id,
          method = m,
          method_used = part$method_used,
          resolution = res,
          rep = r,
          seed = seed_i,
          modularity = part$modularity,
          n_modules = part$n_modules
        )
      }
    }
  }
  runs_df <- bind_rows(run_rows)

  primary_resolution <- resolutions[ceiling(length(resolutions) / 2)]
  cons <- consensus_partition(
    g = g,
    partitions = partitions,
    primary_method = primary_method,
    primary_resolution = primary_resolution,
    consensus_threshold = consensus_threshold,
    seed = seed + 9999,
    max_dense_nodes = max_dense_nodes
  )

  mem <- cons$membership[node_names]
  level_vals <- sort(unique(mem))
  module_id <- match(mem, level_vals)
  mod_cons <- tryCatch(modularity(g, module_id, weights = weights), error = function(e) NA_real_)

  assign_df <- tibble(
    Taxon = node_names,
    module_id = as.integer(module_id),
    module_label = ifelse(is.na(module_id), "Unassigned", paste0("M", as.integer(module_id))),
    node_stability = as.numeric(cons$node_stability),
    graph_variant = variant,
    method = primary_method
  )

  summary_df <- tibble(
    graph_variant = variant,
    method = primary_method,
    n_nodes = vcount(g),
    n_edges = ecount(g),
    n_modules = length(unique(module_id[!is.na(module_id)])),
    modularity_consensus = mod_cons,
    modularity_mean_runs = mean(runs_df$modularity, na.rm = TRUE),
    modularity_sd_runs = sd(runs_df$modularity, na.rm = TRUE),
    mean_pairwise_nmi = cons$mean_nmi,
    consensus_threshold = consensus_threshold
  )

  list(assignments = assign_df, summary = summary_df, runs = runs_df)
}

methods <- tolower(parse_csv_char(opt$methods))
if (length(methods) == 0) methods <- c("leiden", "louvain")
methods <- methods[methods %in% c("leiden", "louvain")]
if (length(methods) == 0) methods <- c("louvain")

resolutions <- parse_csv_num(opt$resolutions)
if (length(resolutions) == 0) resolutions <- c(1.0)

primary_method <- tolower(opt$primary_method)
if (!(primary_method %in% methods)) {
  msg("primary-method '%s' not in methods list; using '%s'", primary_method, methods[1])
  primary_method <- methods[1]
}

msg("Methods: %s", paste(methods, collapse = ", "))
msg("Resolutions: %s", paste(resolutions, collapse = ", "))
msg("Reps per method/resolution: %d", opt$reps)
msg("Consensus threshold: %.3f", opt$consensus_threshold)

g_sub <- load_graph_safe(graph_sub)
if (is.null(g_sub)) stop("Could not load --graph-sub", call. = FALSE)
g_all <- load_graph_safe(graph_all)

sub_res <- analyze_variant(
  g = g_sub,
  variant = "sub",
  methods = methods,
  primary_method = primary_method,
  reps = opt$reps,
  resolutions = resolutions,
  consensus_threshold = opt$consensus_threshold,
  seed = opt$seed,
  max_dense_nodes = opt$max_dense_nodes
)

all_res <- NULL
if (!is.null(g_all)) {
  all_res <- analyze_variant(
    g = g_all,
    variant = "all",
    methods = methods,
    primary_method = primary_method,
    reps = opt$reps,
    resolutions = resolutions,
    consensus_threshold = opt$consensus_threshold,
    seed = opt$seed + 500000,
    max_dense_nodes = opt$max_dense_nodes
  )
}

modules_sub_path <- file.path(outdir, sprintf("%s_modules_sub.tsv", opt$prefix))
modules_all_path <- file.path(outdir, sprintf("%s_modules_all.tsv", opt$prefix))
summary_path <- file.path(outdir, sprintf("%s_module_summary.tsv", opt$prefix))
runs_path <- file.path(outdir, sprintf("%s_module_runs.tsv", opt$prefix))

write_tsv(sub_res$assignments, modules_sub_path)
msg("Wrote %s", modules_sub_path)

if (!is.null(all_res)) {
  write_tsv(all_res$assignments, modules_all_path)
  msg("Wrote %s", modules_all_path)
} else {
  write_tsv(tibble(
    Taxon = character(),
    module_id = integer(),
    module_label = character(),
    node_stability = double(),
    graph_variant = character(),
    method = character()
  ), modules_all_path)
  msg("Wrote empty all-graph modules table: %s", modules_all_path)
}

summary_df <- if (is.null(all_res)) {
  sub_res$summary
} else {
  bind_rows(sub_res$summary, all_res$summary)
}
write_tsv(summary_df, summary_path)
msg("Wrote %s", summary_path)

runs_df <- if (is.null(all_res)) {
  sub_res$runs
} else {
  bind_rows(sub_res$runs, all_res$runs)
}
write_tsv(runs_df, runs_path)
msg("Wrote %s", runs_path)

msg("Done.")
