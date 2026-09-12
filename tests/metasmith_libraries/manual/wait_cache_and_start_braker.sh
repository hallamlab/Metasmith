#!/usr/bin/env bash
set -euo pipefail

log() { printf '[%s] %s\n' "$(date)" "$*"; }

log "Waiting for merged BAM from msrLr7rq"
while true; do
  BAM=$(ssh sockeye "find /scratch/st-shallam-1/pwy_group/metasmith/runs/msrLr7rq/results -type f -name \"*-iNlpm1XR.bam\" | head -n1")
  if [[ -n "${BAM}" ]]; then
    log "Found merged BAM: ${BAM}"
    break
  fi
  sleep 30
done

CACHE_DIR="/scratch/st-shallam-1/pwy_group/metasmith/cache/merged_bams"
CACHE_BAM="${CACHE_DIR}/porphyridium_all9_iNlpm1XR.bam"
ssh sockeye "mkdir -p ${CACHE_DIR} && cp -f ${BAM} ${CACHE_BAM} && ls -lh ${CACHE_BAM}"
log "Cached merged BAM at ${CACHE_BAM}"

log "Starting BRaKER pipeline using cached BAM"
conda run -n msm_env python tests/manual/run_euk_braker3_pipeline_sockeye.py
