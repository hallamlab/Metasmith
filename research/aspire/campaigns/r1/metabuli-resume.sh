#!/usr/bin/env bash
#
# Resume the metabuli reference build from the download onward.
#
# The original build-metabuli-ref.sh run was killed at 95% by
# unattended-upgrades restarting its transient systemd unit. The script is
# linear and starts with mkfs, so it cannot be re-run: a second invocation
# tries to format the already-mounted volume and dies. This picks up from the
# point the original reached, doing everything after "STAGE download" exactly
# as the original does, so the resulting volume is indistinguishable from one
# built in a single pass.
#
# Deliberately NOT a systemd unit. The original's failure mode was a unit
# restart; a detached process has no unit to restart.

set -euo pipefail

STATE=/var/lib/metabuli-build.state
RELEASE=r232
MIN_METABULI=1.2.0--pl5321h0bb26bb_0
DATA_URL=https://steineggerlab.s3.amazonaws.com/metabuli/gtdb232.tar.gz
TARBALL=/mnt/metabuli-dl/metabuli_db.tar.gz
EXPECT_BYTES=633960254218        # S3 Content-Length, measured

say() { echo "$(date -Is) $*" | tee -a "${STATE}"; }

say "RESUME picking up after download-kill by unattended-upgrades"

# --- wait for the download ------------------------------------------------
# The size check is the whole safety story here. Extracting a truncated
# tarball would produce a database that looks plausible and is silently
# incomplete -- the same class of failure as F1 on the read side, where an
# exit code said nothing about whether the data was whole.
while pgrep -x wget >/dev/null; do sleep 60; done

sz=$(stat -c %s "${TARBALL}")
if [ "${sz}" != "${EXPECT_BYTES}" ]; then
    say "FAIL tarball is ${sz} bytes, expected ${EXPECT_BYTES}; refusing to extract"
    exit 1
fi
say "STAGE download-complete ${sz} bytes"

# --- everything below mirrors build-metabuli-ref.sh -----------------------
say "STAGE extract"
mkdir -p /metabuli/unpack
tar -I pigz -xf "${TARBALL}" -C /metabuli/unpack
rm -f "${TARBALL}"

mkdir -p /metabuli/db
if [ -f /metabuli/unpack/db.parameters ]; then
  inner=/metabuli/unpack
else
  found="$(find /metabuli/unpack -maxdepth 3 -name db.parameters -print -quit)"
  test -n "${found}" || { say "FAIL no db.parameters in tarball"; exit 1; }
  inner="$(dirname "${found}")"
fi
for e in "${inner}"/*; do mv "$e" /metabuli/db/; done
rm -rf /metabuli/unpack

say "STAGE release-check"
params="$(cat /metabuli/db/db.parameters 2>/dev/null || true)"
echo "${params}" | tee -a "${STATE}"
short="${RELEASE#r}"
if ! echo "${params}" | grep -qiE "gtdb[^0-9]*${short}|${RELEASE}"; then
  say "FAIL db.parameters does not name ${RELEASE}; refusing to label this volume"
  exit 1
fi
say "  db.parameters names ${RELEASE}"

say "STAGE images"
mkdir -p /metabuli/sif /mnt/sif-cache
export SINGULARITY_CACHEDIR=/mnt/sif-cache
for v in ${BAKE_VERSIONS:-${MIN_METABULI}}; do
  say "  pulling metabuli ${v}"
  singularity pull --force "/metabuli/sif/metabuli-${v}.sif" \
    "docker://quay.io/biocontainers/metabuli:${v}"
done
chmod -R a+rX /metabuli

say "STAGE manifest"
cat > /metabuli/MANIFEST <<MANIFEST
RELEASE=${RELEASE}
MIN_METABULI=${MIN_METABULI}
BUILT=$(date -Is)
SOURCE=${DATA_URL}
MANIFEST
cat /metabuli/MANIFEST | tee -a "${STATE}"

say "STAGE verify"
du -sh /metabuli/db /metabuli/sif | tee -a "${STATE}"
singularity exec --containall --bind /metabuli:/refdata:ro \
  "/metabuli/sif/metabuli-${MIN_METABULI}.sif" \
  metabuli databases 2>&1 | tail -5 | tee -a "${STATE}"
test -f /metabuli/db/db.parameters

say "DONE"
