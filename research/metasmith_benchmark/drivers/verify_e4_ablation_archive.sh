#!/bin/bash
# Check a Globus copy of fir's e4_ablation against the inventory taken before it, and stamp the
# result on fir for release_e4_ablation_scratch.sbatch to read.
#
#   verify_e4_ablation_archive.sh <task id> <chinook dest dir> <fir inventory dir>
#
# Every manifest file must appear in the task's successful transfers at its own path and size.
# Globus checksummed each one (--verify-checksum), which is the integrity check; the size is
# the check that the record describes the file the inventory saw.
set -euo pipefail

GLOBUS="${GLOBUS_BIN:-/home/tony/lib/miniforge3/envs/globus/bin/globus}"
TASK="${1:?task id}"
DEST="${2:?chinook dest dir}"
INV_FIR="${3:?fir inventory dir}"
WORK="${WORK:-$HOME/scratch/e4_ablation_verify}"
mkdir -p "$WORK"

scp -q "fir:$INV_FIR/manifest.tsv" "fir:$INV_FIR/STARTED_AT" "$WORK/"
ssh fir "cd '$INV_FIR' && find . -maxdepth 1 -type f -printf '%P\t%s\n'" | grep -v '^VERIFIED' > "$WORK/inventory_files.tsv"

status=$("$GLOBUS" task show "$TASK" -F json | python3 -c 'import json,sys; t=json.load(sys.stdin); print(t["status"], t["faults"], t["files"], t["subtasks_failed"])')
echo "task: $status"
[[ "$status" == SUCCEEDED* ]] || { echo "task has not succeeded" >&2; exit 1; }

out="$WORK/successful_transfers.$TASK.json"
[ -s "$out" ] || { "$GLOBUS" task show --successful-transfers "$TASK" -F json > "$out.part" && mv "$out.part" "$out"; }

python3 - "$out" "$WORK/manifest.tsv" "$WORK/inventory_files.tsv" "${DEST%/}/" "$WORK/undelivered.txt" <<'PY'
import json, sys
data, manifest, inventory, prefix, report = sys.argv[1:]
got = {}
for e in json.load(open(data))["DATA"]:
    p = e["destination_path"]
    if p.startswith(prefix):
        got[p[len(prefix):]] = e["size"]
want = {}
for line in open(manifest):
    path, size = line.rstrip("\n").split("\t")[:2]
    want[path] = int(size)
for line in open(inventory):
    path, size = line.rstrip("\n").split("\t")
    want["inventory/" + path] = int(size)
want["README.md"] = want["inventory/README.md"]
bad = [f"{p}\t{s}\t{got.get(p)}" for p, s in want.items() if got.get(p) != s]
extra = sorted(set(got) - set(want))
with open(report, "w") as fh:
    fh.writelines(b + "\n" for b in bad)
print(f"expected {len(want)}, delivered {len(got)}, undelivered or resized {len(bad)}, unexpected {len(extra)}")
for x in extra[:10]:
    print("  unexpected:", x)
sys.exit(1 if bad else 0)
PY

{
    echo "RESULT=PASS"
    echo "TASK=$TASK"
    echo "DEST=$DEST"
    echo "FILES=$(wc -l < "$WORK/manifest.tsv")"
    echo "INVENTORY_STARTED_AT=$(cat "$WORK/STARTED_AT")"
    echo "VERIFIED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} > "$WORK/VERIFIED"
scp -q "$WORK/VERIFIED" "fir:$INV_FIR/VERIFIED"
cat "$WORK/VERIFIED"
