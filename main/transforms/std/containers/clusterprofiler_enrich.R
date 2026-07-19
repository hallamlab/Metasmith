#!/usr/bin/env Rscript
# Functional-enrichment (KEGG/GO over-representation) from an eggNOG-mapper
# annotations table, using clusterProfiler.
#
# Usage:
#   clusterprofiler_enrich.R <eggnog.emapper.annotations> <out_table.tsv> <out_plot.png>
#
# The eggNOG annotations file is a TSV with comment lines beginning with '#'.
# The header row starts with '#query' and includes (among others) the columns
# 'GOs' and 'KEGG_ko'. This script builds a gene -> term (GO) map directly from
# the annotations (fully offline, no KEGG/GO web lookups) and runs a generic
# over-representation test with clusterProfiler::enricher, then writes the
# result table and a dotplot.

suppressPackageStartupMessages({
    library(clusterProfiler)
})

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
    stop("Usage: clusterprofiler_enrich.R <annotations.tsv> <out_table.tsv> <out_plot.png>")
}
annotations_file <- args[[1]]
out_table <- args[[2]]
out_plot  <- args[[3]]

# --- locate the header line (starts with '#query') and read the table ---
raw <- readLines(annotations_file)
header_idx <- grep("^#query", raw)
if (length(header_idx) == 0) {
    header_idx <- grep("^#", raw)
    header_idx <- if (length(header_idx)) max(header_idx) else 0
}
skip_n <- if (length(header_idx)) header_idx[[1]] - 1 else 0

df <- read.delim(
    annotations_file,
    header = TRUE,
    sep = "\t",
    quote = "",
    comment.char = "",
    skip = skip_n,
    check.names = FALSE,
    stringsAsFactors = FALSE
)
colnames(df) <- sub("^#", "", colnames(df))

# drop trailing eggNOG summary comment rows
if ("query" %in% colnames(df)) {
    df <- df[!startsWith(as.character(df$query), "#"), , drop = FALSE]
}

gene_col <- if ("query" %in% colnames(df)) "query" else colnames(df)[[1]]
term_col <- if ("GOs" %in% colnames(df)) "GOs" else if ("KEGG_ko" %in% colnames(df)) "KEGG_ko" else NA

# --- build TERM2GENE from the (possibly comma-separated) term column ---
term2gene <- data.frame(term = character(0), gene = character(0), stringsAsFactors = FALSE)
if (!is.na(term_col)) {
    # Vectorized TERM2GENE construction. The naive per-row rbind is O(n^2) and
    # takes ~8.5 min on a few thousand annotated genes; this builds the same
    # (term, gene) pairs in one pass (~7 s, output-identical to enricher).
    genes_vec <- as.character(df[[gene_col]])
    terms_vec <- as.character(df[[term_col]])
    lst <- strsplit(terms_vec, ",", fixed = TRUE)
    gene_rep <- rep(genes_vec, lengths(lst))
    term_all <- trimws(unlist(lst, use.names = FALSE))
    keep <- !is.na(term_all) & term_all != "" & term_all != "-" &
            !is.na(gene_rep)
    if (any(keep)) {
        term2gene <- data.frame(term = term_all[keep], gene = gene_rep[keep],
                                stringsAsFactors = FALSE)
    }
}

genes <- unique(term2gene$gene)

result_df <- NULL
enrich <- NULL
if (nrow(term2gene) > 0 && length(genes) > 0) {
    enrich <- tryCatch(
        enricher(
            gene = genes,
            TERM2GENE = term2gene,
            pvalueCutoff = 1,
            qvalueCutoff = 1,
            minGSSize = 1,
            maxGSSize = 100000
        ),
        error = function(e) NULL
    )
    if (!is.null(enrich)) {
        result_df <- as.data.frame(enrich)
    }
}

if (is.null(result_df)) {
    result_df <- data.frame(
        ID = character(0), Description = character(0),
        GeneRatio = character(0), BgRatio = character(0),
        pvalue = numeric(0), p.adjust = numeric(0), qvalue = numeric(0),
        geneID = character(0), Count = integer(0),
        stringsAsFactors = FALSE
    )
}

write.table(result_df, file = out_table, sep = "\t", quote = FALSE, row.names = FALSE)

# --- dotplot (fall back to an empty labelled PNG if there is nothing to draw) ---
png(out_plot, width = 1600, height = 1200, res = 150)
plotted <- FALSE
if (!is.null(enrich) && nrow(result_df) > 0) {
    p <- tryCatch(dotplot(enrich, showCategory = 20), error = function(e) NULL)
    if (!is.null(p)) {
        print(p)
        plotted <- TRUE
    }
}
if (!plotted) {
    plot.new()
    text(0.5, 0.5, "No enriched terms", cex = 1.5)
}
invisible(dev.off())
