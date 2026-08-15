#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(optparse)
  library(vegan)
})

option_list <- list(
  make_option("--asv-matrix", type = "character"),
  make_option("--measurement-matrix", type = "character"),
  make_option("--metadata", type = "character"),
  make_option("--outdir", type = "character"),
  make_option("--methods", type = "character", default = "cca,rda,dbrda"),
  make_option("--permutations", type = "integer", default = 999),
  make_option("--bray-transform", type = "character", default = "hellinger")
)
opt <- parse_args(OptionParser(option_list = option_list))

dir.create(opt$outdir, recursive = TRUE, showWarnings = FALSE)
tables_dir <- file.path(opt$outdir, "tables")
dir.create(tables_dir, recursive = TRUE, showWarnings = FALSE)

asv <- read.delim(opt$`asv-matrix`, row.names = 1, check.names = FALSE)
env <- read.delim(opt$`measurement-matrix`, row.names = 1, check.names = FALSE)
meta <- read.delim(opt$metadata, check.names = FALSE)

common <- intersect(rownames(asv), rownames(env))
if (length(common) < 3) {
  stop(sprintf("Need at least 3 overlapping samples for constrained ordination; found %d", length(common)))
}

asv <- asv[common, , drop = FALSE]
env <- env[common, , drop = FALSE]

asv[] <- lapply(asv, function(x) as.numeric(as.character(x)))
asv[is.na(asv)] <- 0
env[] <- lapply(env, function(x) as.numeric(as.character(x)))

env <- env[, colSums(is.na(env)) < nrow(env), drop = FALSE]
if (ncol(env) < 1) {
  stop("No usable numeric measurement columns after NA filtering")
}
for (col in colnames(env)) {
  vals <- env[[col]]
  if (anyNA(vals)) {
    vals[is.na(vals)] <- median(vals, na.rm = TRUE)
    env[[col]] <- vals
  }
}
env <- env[, vapply(env, function(x) sd(x, na.rm = TRUE) > 0, logical(1)), drop = FALSE]
if (ncol(env) < 1) {
  stop("No usable numeric measurement columns after variance filtering")
}

if (ncol(env) > 1) {
  cm <- suppressWarnings(cor(env, use = "pairwise.complete.obs"))
  drop <- c()
  for (i in seq_len(ncol(cm) - 1)) {
    if (colnames(cm)[i] %in% drop) next
    hits <- which(abs(cm[i, (i + 1):ncol(cm)]) >= 0.95)
    if (length(hits)) {
      drop <- c(drop, colnames(cm)[i + hits])
    }
  }
  drop <- unique(drop)
  if (length(drop)) {
    write.table(
      data.frame(removed = drop, note = "absolute correlation >= 0.95"),
      file = file.path(tables_dir, "ordination_collinearity_removed.tsv"),
      sep = "\t", quote = FALSE, row.names = FALSE
    )
    env <- env[, setdiff(colnames(env), drop), drop = FALSE]
  }
}

if (ncol(env) < 1) {
  stop("No measurement columns remain after collinearity filtering")
}

env_scaled <- as.data.frame(scale(env))
asv_hel <- decostand(asv, method = "hellinger")
methods <- trimws(strsplit(opt$methods, ",")[[1]])
methods <- methods[methods != ""]

save_scores <- function(model, method_name) {
  site <- as.data.frame(scores(model, display = "sites", choices = 1:2))
  site$sample_id <- rownames(site)
  species <- as.data.frame(scores(model, display = "species", choices = 1:2))
  species$ASV_ID <- rownames(species)
  bp <- as.data.frame(scores(model, display = "bp", choices = 1:2))
  bp$measurement <- rownames(bp)
  write.table(site, file.path(tables_dir, paste0(method_name, "_site_scores.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)
  write.table(species, file.path(tables_dir, paste0(method_name, "_asv_scores.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)
  write.table(bp, file.path(tables_dir, paste0(method_name, "_measurement_scores.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)

  total <- tryCatch(sum(model$CCA$eig, model$CA$eig), error = function(e) NA_real_)
  constrained <- tryCatch(sum(model$CCA$eig), error = function(e) NA_real_)
  summary_row <- data.frame(
    method = method_name,
    n_samples = nrow(asv),
    n_asvs = ncol(asv),
    n_measurements = ncol(env_scaled),
    constrained_inertia = constrained,
    total_inertia = total,
    constrained_fraction = ifelse(is.na(total) || total == 0, NA_real_, constrained / total)
  )
  write.table(summary_row, file.path(tables_dir, paste0(method_name, "_summary.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)

  full_test <- tryCatch(anova.cca(model, permutations = opt$permutations), error = function(e) NULL)
  term_test <- tryCatch(anova.cca(model, by = "term", permutations = opt$permutations), error = function(e) NULL)
  axis_test <- tryCatch(anova.cca(model, by = "axis", permutations = opt$permutations), error = function(e) NULL)
  if (!is.null(full_test)) {
    ft <- as.data.frame(full_test)
    ft$term <- rownames(ft)
    write.table(ft, file.path(tables_dir, paste0(method_name, "_anova_full.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)
  }
  if (!is.null(term_test)) {
    tt <- as.data.frame(term_test)
    tt$term <- rownames(tt)
    write.table(tt, file.path(tables_dir, paste0(method_name, "_anova_terms.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)
  }
  if (!is.null(axis_test)) {
    at <- as.data.frame(axis_test)
    at$axis <- rownames(at)
    write.table(at, file.path(tables_dir, paste0(method_name, "_anova_axes.tsv")), sep = "\t", quote = FALSE, row.names = FALSE)
  }
}

if ("cca" %in% methods) {
  model <- cca(asv_hel ~ ., data = env_scaled)
  save_scores(model, "cca")
}
if ("rda" %in% methods) {
  model <- rda(asv_hel ~ ., data = env_scaled)
  save_scores(model, "rda")
}
if ("dbrda" %in% methods) {
  dist_obj <- vegdist(asv_hel, method = "bray")
  model <- capscale(dist_obj ~ ., data = env_scaled)
  save_scores(model, "dbrda")
}
