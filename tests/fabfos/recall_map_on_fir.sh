#!/usr/bin/env bash
# Per-read-pair HIT SETS against the day-6 piece set, one SLURM job per barcode.
#
# WHY NOT `assembly_stats`: that lane runs minimap2 without secondaries and reports
# summary statistics, not which contig a read hit. What the recall sweep needs is,
# per read PAIR, the SET of pieces it aligns to -- so that when a piece is merged
# away, the pair is still credited to whatever else it matches. That makes one
# mapping pass serve every threshold on the curve instead of one pass per candidate
# cut.
#
# WHY A PLAIN SBATCH: no transform in the library produces a secondary-retaining PAF,
# and authoring one for a measurement that executes three times would cost more than
# it returns. Nothing here stages the metasmith engine, so the dev-overlay hazard
# does not apply. minimap2 comes from the CVMFS module rather than a container --
# fir has minimap2/2.30 on `module spider` and the agent home carries no minimap2
# image, so the module is both simpler and the only thing actually present.
#
# The PAF is never written to disk: minimap2 pipes straight into a gawk reducer that
# groups CONSECUTIVE records by query name (minimap2 preserves input order, and
# interleaved mates are adjacent and share a name once the /1 /2 suffix is stripped)
# and counts each distinct hit-set once per pair. Output is a few thousand rows.
#
# USAGE   ./recall_map_on_fir.sh submit|status|fetch
set -euo pipefail

HOST=${FIR_HOST:-fir}
ACCOUNT=${SLURM_ACCOUNT:-rrg-shallam-ab}
READS=/scratch/phyberos/metasmith_fabfos/host_filtered_reads_pairaware_20260724
REMOTE=/scratch/phyberos/metasmith_fabfos/recall_sweep_day6
POOLS=(pool01_ATTGAGCC pool01_GAACGCTT pool01_GTAACGAC)

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CACHE=$HERE/../main/clustering_sweep/cache
NCAP=50          # -N: secondary alignments retained per read. A clone appears in at
                 # most 3 barcodes x 2 assemblers, so 50 is far above the real ceiling;
                 # the reducer reports the largest hit-set it saw so the cap is checked
                 # rather than assumed.

case "${1:-}" in
submit)
    test -s "$CACHE/day6_pieces.fna" || {
        echo "no $CACHE/day6_pieces.fna -- run gen_recall_sweep.py first" >&2; exit 1; }
    ssh "$HOST" "mkdir -p $REMOTE/logs"
    rsync -av "$CACHE/day6_pieces.fna" "$HOST:$REMOTE/"

    # The reducer. Held in a heredoc rather than a second file so the job script and
    # the thing it runs cannot drift apart.
    cat > /tmp/recall_reduce.awk <<'AWK'
BEGIN { FS = "\t"; OFS = "\t"; nh = 0; cur = "" }
function emit(   i, j, t, sig) {
    for (i = 2; i <= nh; i++) { t = n[i]; for (j = i-1; j >= 1 && n[j] > t; j--) n[j+1] = n[j]; n[j+1] = t }
    sig = n[1]; for (i = 2; i <= nh; i++) sig = sig "," n[i]
    cnt[sig]++
    if (nh > maxset) maxset = nh
    if (nh >= NCAP) capped++
    pairs++
}
{
    # The suffix is DOUBLED: these FASTQ headers already end in /1 or /2, and
    # minimap2 -x sr recognises the file as interleaved and appends its own mate
    # number on top -- so the query name arrives as `...:34726/1/1`. Stripping one
    # suffix leaves the mates distinct and silently turns this into a per-READ
    # count; the tell is that the total then matches the read count exactly.
    q = $1; sub(/(\/[12])+$/, "", q)
    if (q != cur) { if (nh > 0) emit(); cur = q; nh = 0; delete seen }
    if (!($6 in seen)) { seen[$6] = 1; n[++nh] = $6 }
}
END {
    if (nh > 0) emit()
    ndist = 0
    for (s in cnt) { print s, cnt[s] > SIGOUT; ndist++ }
    print "pairs_with_a_hit", pairs+0 > STATOUT
    print "distinct_hitsets", ndist+0   > STATOUT
    print "max_hitset_size",  maxset+0  > STATOUT
    print "pairs_at_N_cap",   capped+0  > STATOUT
}
AWK
    rsync -av /tmp/recall_reduce.awk "$HOST:$REMOTE/"

    cat > /tmp/recall_map.sbatch <<EOF
#!/usr/bin/env bash
#SBATCH --account=$ACCOUNT
#SBATCH --cpus-per-task=16
#SBATCH --mem=32G
#SBATCH --time=3:00:00
set -euo pipefail
module load minimap2/2.30
POOL=\$1
cd $REMOTE
# -x sr short-read preset; secondaries kept because near-identical pieces from the
# three barcodes are exactly what a pair must be allowed to hit at once. -p 0.5 is
# deliberately loose: co-clustered pieces score within a whisker of each other and
# the default 0.8 ratio would silently drop the ties this measurement is about.
minimap2 -t 16 -x sr --secondary=yes -N $NCAP -p 0.5 \\
    day6_pieces.fna "$READS/\$POOL.host_filtered.fq.gz" 2> logs/\$POOL.minimap2.err \\
  | gawk -v NCAP=$NCAP -v SIGOUT=\$POOL.hitsets.tsv -v STATOUT=\$POOL.stats.tsv \\
        -f recall_reduce.awk
echo "done \$POOL"
EOF
    rsync -av /tmp/recall_map.sbatch "$HOST:$REMOTE/"
    for p in "${POOLS[@]}"; do
        ssh "$HOST" "cd $REMOTE && sbatch --job-name=recall_$p \
            --output=logs/$p.slurm.out --error=logs/$p.slurm.err \
            recall_map.sbatch $p"
    done
    ;;
status)
    ssh "$HOST" "squeue -u \$USER -o '%.10i %.20j %.8T %.10M %R'; echo '--- products ---'; ls -la $REMOTE/*.tsv 2>/dev/null; echo '--- stats ---'; tail -n +1 $REMOTE/*.stats.tsv 2>/dev/null"
    ;;
fetch)
    mkdir -p "$CACHE/fir_hitsets"
    rsync -av "$HOST:$REMOTE/*.hitsets.tsv" "$HOST:$REMOTE/*.stats.tsv" "$CACHE/fir_hitsets/"
    ls -la "$CACHE/fir_hitsets/"
    ;;
*)
    sed -n '1,30p' "$0"; exit 1 ;;
esac
