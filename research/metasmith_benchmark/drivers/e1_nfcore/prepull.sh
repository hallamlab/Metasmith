#!/bin/bash
# research/metasmith_benchmark/drivers/e1_nfcore/prepull.sh -- convert every nf-core/mag 5.5.0 container to a cached
# .img BEFORE a single Slurm task submits. fir's compute nodes move under 1 MB/s to any
# external host, so a task that has to pull hangs rather than fails -- this must run on a
# login node (the only place on fir with real network) and must finish before run_test.sh
# or a real launch, not concurrently with one. Run only ONE prepull tree at a time: two
# concurrent apptainer pull/build trees against the same cache is what corrupted a prior
# round's images (each pull already gets its own APPTAINER_TMPDIR below -- that guards
# against collisions WITHIN one tree, not against a second tree started separately).
#
# The image list (IMAGES_FILE, one `nextflow inspect -profile test,apptainer` container ref
# per line) is independent of which skip_* flags are set -- `nextflow inspect` walks the
# whole pipeline DAG, so it already includes comebin/das_tool/checkm2/semibin even though
# the pipeline's OWN -profile test skips comebin by default. Regenerate it with:
#   nextflow inspect nf-core/mag -r 5.5.0 -profile test,apptainer -concretize false > inspect.json
#   <parse .processes[].container into one-per-line, de-duplicated>
#
# Target filenames follow Nextflow's own apptainer.cacheDir naming: strip the scheme, then
# replace every `/` and `:` with `-`, append `.img`. A bare `org/name:tag` with no dot in its
# first path segment (nf-core's convention for images with no explicit registry, e.g.
# `biocontainers/comebin:...`) resolves to `quay.io/org/name:tag` for BOTH the pull source and
# the cache filename -- confirmed against images the campaign's own probe had already pulled
# (e.g. `biocontainers/bioawk:...` cached as `quay.io-biocontainers-bioawk-....img`). Getting
# this wrong doesn't error -- it just means Nextflow's own cache lookup misses at run time and
# tries to pull again, and now that pull is running unattended on whatever node did the miss.
#
# A cached file existing and non-empty is NOT proof it is usable -- a truncated pull from an
# interrupted or colliding run leaves exactly that shape. Every SKIP and every fresh pull is
# verified with `apptainer sif list`, which reads the SIF header/descriptor table and fails
# fast on a truncated or corrupt image without re-downloading anything. The script exits
# non-zero (not 0) if any image is missing, fails to pull, or fails verification, and prints
# a manifest-count line so a partial cache cannot be mistaken for a complete one by inspection.
#
# PARALLELISM default dropped from 6 to 2, and mksquashfs is capped to 2 threads via a
# per-run apptainer.conf (below), after a real run of this script at PARALLELISM=6 produced
# 17 of 19 failures reading `FATAL ERROR: Failed to create thread` out of `mksquashfs`. That
# is not a registry or network failure: apptainer's default is `mksquashfs procs = 0` (use
# nproc, 64 on this login node), so six concurrent pulls each spawn a ~10-thread compressor
# and collectively starve the login node's shared PER-USER cgroup pid cap (the same 512-pid
# ceiling documented elsewhere in this campaign) alongside everyone else's session on the
# node. `apptainer --config <ours> pull` with `mksquashfs procs = 2` (verified: the resulting
# mksquashfs process actually launches with `-processors 2`, confirmed against the vendor
# apptainer.conf's own commented default) plus PARALLELISM=2 keeps the worst-case thread
# count an order of magnitude lower. Override either via PREPULL_PARALLELISM /
# PREPULL_MKSQUASHFS_PROCS if the shared node is quieter than it was here.
set -uo pipefail
IMAGES_FILE="${1:-$(dirname "${BASH_SOURCE[0]}")/images.txt}"
CACHE="${NXF_APPTAINER_CACHEDIR:-/scratch/phyberos/wave2_b3_nfcore/apptainer_cache/nxf}"
BASE_TMP="${APPTAINER_TMPDIR_ROOT:-/scratch/phyberos/wave2_b3_nfcore/apptainer_tmp}"
PARALLELISM="${PREPULL_PARALLELISM:-2}"
MKSQUASHFS_PROCS="${PREPULL_MKSQUASHFS_PROCS:-2}"
FAIL_LOG="$(mktemp)"

module load apptainer/1.3.5 2>/dev/null
export APPTAINER_CACHEDIR="${APPTAINER_CACHEDIR:-/scratch/phyberos/apptainer_cache}"
mkdir -p "$CACHE" "$BASE_TMP"

# `-c`/`--config` is documented as available "for root or unprivileged installation only" --
# this module's install is unprivileged, verified by using it successfully below. Derive our
# copy from apptainer's own vendored default so every other setting stays exactly what the
# module ships, and only mksquashfs's thread count changes.
VENDOR_CONF="$(apptainer config global 2>/dev/null | grep -oE '/\S+/apptainer\.conf' | head -1)"
if [ -z "$VENDOR_CONF" ]; then
  # apptainer --help prints the resolved default path even when `config global` does not.
  VENDOR_CONF="$(apptainer --help 2>&1 | grep -oE '/\S+/apptainer\.conf' | head -1)"
fi
OUR_CONF="$BASE_TMP/apptainer.conf"
if [ -n "$VENDOR_CONF" ] && [ -r "$VENDOR_CONF" ]; then
  sed "s/^mksquashfs procs = .*/mksquashfs procs = ${MKSQUASHFS_PROCS}/" "$VENDOR_CONF" > "$OUR_CONF"
  export APPTAINER_CONF_FLAG="--config $OUR_CONF"  # path has no spaces; safe to word-split below
else
  echo "WARNING: could not locate the vendor apptainer.conf to derive a thread-capped copy;" \
       "proceeding with apptainer's own default (mksquashfs procs = 0, i.e. nproc) -- if" \
       "pulls start failing with 'Failed to create thread', that is why." >&2
  export APPTAINER_CONF_FLAG=""
fi

verify_one() {
  # Exits 0 iff $1 is a readable, structurally intact SIF image.
  apptainer sif list "$1" >/dev/null 2>&1
}
export -f verify_one

pull_one() {
  idx="$1"; line="$2"
  wd="$BASE_TMP/w$idx"; mkdir -p "$wd"
  export APPTAINER_TMPDIR="$wd" SINGULARITY_TMPDIR="$wd"  # concurrent pulls need separate tmpdirs -- Blocker 2
  if [[ "$line" == http* ]]; then
    src="$line"; name="${line#https://}"
  elif [[ "$line" == *.*/* ]]; then
    src="docker://$line"; name="$line"
  else
    src="docker://quay.io/$line"; name="quay.io/$line"
  fi
  name="${name//\//-}"; name="${name//:/-}"
  target="$CACHE/${name}.img"
  if [ -s "$target" ] && verify_one "$target"; then
    echo "SKIP $line"
    return 0
  fi
  echo "PULL-START $line"
  if apptainer $APPTAINER_CONF_FLAG pull -F "$target" "$src" > "$BASE_TMP/prepull_${idx}.log" 2>&1 && verify_one "$target"; then
    echo "PULL-OK $line"
  else
    echo "PULL-FAIL $line ($BASE_TMP/prepull_${idx}.log)"
    echo "$line" >> "$FAIL_LOG"
  fi
}
export -f pull_one
export CACHE BASE_TMP FAIL_LOG

nl -ba -w1 -s' ' "$IMAGES_FILE" | xargs -P "$PARALLELISM" -L1 bash -c 'pull_one "$1" "$2"' _

n_wanted=$(grep -c . "$IMAGES_FILE")
# Not `grep -c . "$FAIL_LOG" || echo 0`: grep -c on a file with no matching lines both prints
# "0" AND exits 1, so the `||` fires anyway and appends a second "0" -- n_failed ends up as the
# two-line string "0\n0", and the -gt below dies with "integer expression expected" (observed
# live on the run that produced this comment). wc -l has no such "0 but also failed" split.
n_failed=0
[ -s "$FAIL_LOG" ] && n_failed=$(wc -l < "$FAIL_LOG")
n_cached=$(find "$CACHE" -maxdepth 1 -name '*.img' | wc -l)
echo "MANIFEST=$n_wanted CACHED_FILES=$n_cached FAILED=$n_failed"

if [ "$n_failed" -gt 0 ]; then
  echo "PREPULL-FAILED: $n_failed of $n_wanted image(s) missing or failed verification:"
  cat "$FAIL_LOG"
  rm -f "$FAIL_LOG"
  exit 1
fi
if [ "$n_cached" -lt "$n_wanted" ]; then
  echo "PREPULL-FAILED: cache holds $n_cached files but manifest names $n_wanted -- naming mismatch or stray file, investigate before trusting this cache."
  rm -f "$FAIL_LOG"
  exit 1
fi
rm -f "$FAIL_LOG"
echo "ALL-DONE"
