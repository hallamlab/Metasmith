#!/bin/bash
# Resume a COMEBin run at the clustering step, from a preserved representation.
#
#   usage: submit_comebin_cluster.sh <sample> [cpus] [mem] [time] [threads]
#
# Why this exists
# ---------------
# S13/S9/S25 each trained for 14-25 h and then deadlocked in the Leiden
# parameter sweep: parent plus all 64 pool workers parked in futex_do_wait, no
# file written for 7-12 h, 11 s of CPU across a 5 h window. The training was the
# expensive part and it had already succeeded, so the answer is to resume rather
# than re-run -- `salvage/<S>_comebin_out.tar` holds the trained embeddings.
#
# Three existence checks make the resume work, and all three are COMEBin's own:
#   run_comebin.sh  skips augmentation when data_augmentation/ has n_views
#                   *_datacoverage_mean.tsv files, and skips training when
#                   comebin_res/ has both embeddings.tsv and covembeddings.tsv.
#   cluster.py      skips any Leiden combination whose output .tsv exists, so the
#                   120-point sweep resumes where it stalled (95/64/48 done).
#   seed_kmeans_full  skips when its weight_seed_kmeans_k_<n>_result.tsv exists.
#
# That last one is the likely fix, not a convenience. seed_kmeans_full builds
# `KMeans(n_jobs=-1)`, whose joblib/loky backend leaves a live worker pool and
# its threads in the parent -- and the stalled parent still had exactly such a
# loky process sitting in pipe_read. multiprocessing.Pool then forks 64 children
# from that threaded parent, and a fork inherits locked mutexes but not the
# threads that would release them: textbook fork-after-threads deadlock, which
# is why every worker was in futex with nothing to show for it. On a resume the
# k-means result already exists, so no loky pool is ever created and the fork is
# clean. OMP_NUM_THREADS=1 below removes the other thread pool for the same
# reason; the pool, not the parent, is where the parallelism belongs.
#
# The sweep is checkpointed, so progress survives even a failed attempt: the trap
# writes comebin_res back to salvage/<S>_progress.tar on every exit path,
# including SIGTERM at the wall clock (--signal=B:TERM@600 buys 10 minutes).
# Re-running this script simply continues.
set -eu
G=/scratch/phyberos/gmcf3495/gapfill
. $G/env.sh
SAMPLE=$1
CPUS=${2:-16}; MEM=${3:-128G}; TIME=${4:-24:00:00}; THREADS=${5:-8}
SALV=$G/salvage
MAP=/scratch/phyberos/gmcf3495/inv/sample_asm_bam.tsv

row=$(awk -F'\t' -v s="$SAMPLE" '$1==s' "$MAP")
[ -z "$row" ] && { echo "no map row for $SAMPLE"; exit 1; }
a=$(echo "$row" | cut -f2)
case $a in /*) ASM=$a ;; *) ASM=$RES/sequences-megahit_assembly/$a ;; esac
[ -f "$ASM" ] || { echo "missing asm $ASM"; exit 1; }
[ -f "$SALV/${SAMPLE}_comebin_out.tar" ] || { echo "no salvage tar for $SAMPLE"; exit 1; }

# Not the same work dir as submit_bin.sh's, which opens with `rm -rf`. Bins are
# collected from out/<sample>/comebin, so stage there but build in $SLURM_TMPDIR.
WD=$G/out/$SAMPLE/comebin
mkdir -p "$WD" $G/logs

cat > $G/out/$SAMPLE/cluster_job.sh <<EOF
#!/bin/bash
#SBATCH --account=def-shallam_cpu
#SBATCH --job-name=gf_comebin_${SAMPLE}
#SBATCH --nodes=1 --ntasks=1
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --time=$TIME
#SBATCH --signal=B:TERM@600
#SBATCH --output=$G/logs/comebin_${SAMPLE}.%j.out
#SBATCH --error=$G/logs/comebin_${SAMPLE}.%j.err
set -eu
cd \$SLURM_TMPDIR

save_progress() {
  rc=\$?
  [ -n "\${SAVED:-}" ] && exit \$rc
  SAVED=1
  echo "=== saving progress (rc=\$rc) ==="
  if [ -d \$SLURM_TMPDIR/comebin_out/comebin_res ]; then
    ( cd \$SLURM_TMPDIR/comebin_out && tar -cf $SALV/${SAMPLE}_progress.tar.tmp comebin_res ) \
      && mv $SALV/${SAMPLE}_progress.tar.tmp $SALV/${SAMPLE}_progress.tar
    echo "leiden tsv now: \$(ls \$SLURM_TMPDIR/comebin_out/comebin_res/cluster_res/*.tsv 2>/dev/null | wc -l) / 120"
    mkdir -p $WD
    cp -r \$SLURM_TMPDIR/comebin_out/comebin_res $WD/ 2>/dev/null || true
  fi
  # A deadlock leaves no log line to read a cause from, so name it here rather
  # than letting binner_status.py infer TIMEOUT from the sacct state. The census
  # requires a cause it can attribute, not a silent zero.
  nb=\$(ls \$SLURM_TMPDIR/comebin_out/comebin_res/comebin_res_bins/*.fa 2>/dev/null | wc -l)
  nt=\$(ls \$SLURM_TMPDIR/comebin_out/comebin_res/cluster_res/*.tsv 2>/dev/null | wc -l)
  if [ "\$nb" -eq 0 ] && [ "\$rc" -ne 0 ] && [ "\$nt" -lt 120 ]; then
    mkdir -p $WD
    printf 'comebin\t%s\tCOMEBIN_LEIDEN_POOL_DEADLOCK\t%s of 120 leiden results\n' \
      "$SAMPLE" "\$nt" > $WD/skipped.txt
  fi
  exit \$rc
}
# TERM/INT must kill the foreground child first: bash defers a trap until the
# current foreground command returns, and a deadlocked apptainer never does --
# which is why the run below is backgrounded and waited on rather than run
# directly. Without that, --signal=B:TERM@600 would buy nothing.
trap 'kill \$APID 2>/dev/null || true; save_progress' TERM INT
trap save_progress EXIT

echo "=== restoring representation ==="
tar -xf $SALV/${SAMPLE}_comebin_out.tar
# Written by a previous attempt of this same script; holds strictly more Leiden
# results than the base tar, so it must be unpacked second. Written as an "if",
# not "[ -f ] && tar": that form is the last command in the list, so on the first
# attempt -- when no progress tar exists yet -- it returns 1 and "set -e" kills
# the job before COMEBin is ever reached.
# (No backticks anywhere below: this heredoc is unquoted, so a backtick runs as
# command substitution and silently deletes itself from the emitted script.)
if [ -f $SALV/${SAMPLE}_progress.tar ]; then
  tar -xf $SALV/${SAMPLE}_progress.tar -C comebin_out
fi
cp -L '$ASM' asm.fna
# The sidecars are FragGeneScan + hmmsearch output and the marker seed list, all
# of which live next to asm.fna rather than inside comebin_out -- easy to miss
# when preserving, and ~20 min to regenerate.
cp -p $SALV/$SAMPLE/* . 2>/dev/null || true
mkdir -p bam_input
echo "leiden tsv restored: \$(ls comebin_out/comebin_res/cluster_res/*.tsv 2>/dev/null | wc -l) / 120"

# Single-threaded on purpose: every thread pool alive in the parent at fork time
# is a mutex the 8 pool children can inherit locked. Parallelism comes from -t.
export OMP_NUM_THREADS=1
apptainer exec $BIND --env OMP_NUM_THREADS=1 --bind "\$SLURM_TMPDIR" --pwd "\$SLURM_TMPDIR" \
  '$SIF_COMEBIN' bash -c "set -eux
  run_comebin.sh -a \\\$PWD/asm.fna -o \\\$PWD/comebin_out -p \\\$PWD/bam_input -t $THREADS -b 1024" &
APID=\$!

# The deadlock is silent -- no log line, no exit, just 64 futexes -- so the only
# way to tell it apart from slow progress is that the Leiden sweep stops emitting
# files. Bounded to the sweep itself (< 120 results): once the sweep completes,
# get_result runs UniteM and CheckM for hours without touching cluster_res, and
# killing that would be killing real work. 60 min of no new file is generous --
# the whole 95-file sweep on S13 took 9 minutes.
(
  stall=0; last=-1
  while kill -0 \$APID 2>/dev/null; do
    sleep 300
    n=\$(ls \$SLURM_TMPDIR/comebin_out/comebin_res/cluster_res/*.tsv 2>/dev/null | wc -l)
    [ "\$n" -ge 120 ] && { stall=0; last=\$n; continue; }
    if [ "\$n" -eq "\$last" ]; then stall=\$((stall+1)); else stall=0; last=\$n; fi
    if [ "\$stall" -ge 12 ]; then
      echo "WATCHDOG: leiden sweep stalled at \$n/120 for 60 min -- killing"
      kill \$APID 2>/dev/null || true
      break
    fi
  done
) &
WPID=\$!

wait \$APID
kill \$WPID 2>/dev/null || true
echo DONE
EOF
jid=$(sbatch --parsable $G/out/$SAMPLE/cluster_job.sh)
echo -e "comebin_cluster\t${SAMPLE}\t${jid}\t${WD}"
