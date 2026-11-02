# Zero out jgi_summarize_bam_contig_depths garbage rows.
# It writes uninitialised memory for a handful of contigs: depth ~ -1.2e9 with
# variance ~1e18. On S1 that was 4 rows out of 4888. Subsampling the BAM 50x did
# not change the magnitude, which is what ruled out an arithmetic overflow.
# metabat2 refuses to run at all when it sees such a row, so zero the bad ones
# and keep every good one.
BEGIN { OFS = "\t" }
NR == 1 { print; next }
{
    bad = 0
    for (i = 3; i <= NF; i++) {
        if ($i + 0 < 0 || $i + 0 > 1e7) { $i = 0; bad = 1 }
    }
    if (bad) n++
    print
}
END { printf("sanitised %d rows\n", n + 0) > "/dev/stderr" }
