#!/bin/bash
# research/cami/nfcore/run_test.sh -- confirm fir.config against nf-core/mag's own bundled
# test data, on Slurm, before trusting it with a real CAMI sample. Mirrors the campaign's
# proven recipe (module load for Java/env, private nextflow26 binary, NXF_HOME pinned away
# from the 25.x plugin state) plus two fixes this run found the hard way:
#
#   -XX:ActiveProcessorCount=4  -- without it the driver JVM loses a threads-at-boot race
#                                  against the login node's shared per-user cgroup and dies
#                                  in seconds, before Slurm ever sees a submission. See
#                                  fir.config's header comment for the evidence.
#   -profile test,fir            -- `fir` (not a bare `-c fir.config`) so it composes with
#                                  other profiles the same way; harmless either way here.
#
# `-c control.config` is what actually applies the pinned three-binner/refinement/CheckM2
# choice -- it was missing before this repair round, so every run through this script had
# silently validated only the executor profile against `-profile test`'s OWN tool choices
# (which invert all three: no COMEBin, no CheckM2, refinement off). See control.config's
# header for why the merge order on the command line never mattered.
#
# Every container this needs must already be cached (see prepull.sh) -- a compute node has
# no network, so a cache miss here hangs a task instead of failing it.
set -uo pipefail
WORKDIR="${1:-/scratch/phyberos/wave2_b3_nfcore}"
CONFIG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

module load nextflow/25.04.6 apptainer/1.3.5
export NXF_HOME="$HOME/.nextflow26"
export APPTAINER_CACHEDIR=/scratch/phyberos/apptainer_cache
export APPTAINER_TMPDIR="$WORKDIR/apptainer_tmp/run_test"
export SINGULARITY_TMPDIR="$APPTAINER_TMPDIR"
export NXF_APPTAINER_CACHEDIR="$WORKDIR/apptainer_cache/nxf"
export NXF_SINGULARITY_CACHEDIR="$NXF_APPTAINER_CACHEDIR"
export NXF_OPTS="-Xmx6g -XX:ActiveProcessorCount=4"
mkdir -p "$APPTAINER_TMPDIR"

cd "$WORKDIR"
"$HOME/bin/nextflow26" run nf-core/mag -r 5.5.0 \
  -profile test,fir -c "$CONFIG_DIR/fir.config" -c "$CONFIG_DIR/control.config" \
  --outdir "$WORKDIR/out_test" \
  -work-dir "$WORKDIR/work_test" \
  -ansi-log false
rc=$?
echo "NXF_EXIT=$rc"
exit "$rc"
