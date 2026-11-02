#!/bin/bash
# Local check of repromote_step.py on a synthetic run: one good task over a tombstoned shard,
# one with a truncated BAM, one excluded.
set -euo pipefail
W="${TMPDIR:?}/repromote_test"; rm -rf -- "$W"; mkdir -p "$W"
HERE="$(cd "$(dirname "$0")" && pwd)"
python3 - "$W" <<'EOF'
import json, sys
from pathlib import Path
w = Path(sys.argv[1]); root = w / "task_cache"
eof = bytes.fromhex("1f8b08040000000000ff0600424302001b0003000000000000000000")
meta = ("transform_key GT1rWBD2\nsignature 1:abc\nstep_name bowtie2_binning_bam\ncacheable true\nsession 2\n"
        'slot_files [{"dtype_key":"5VZBahJZ","slot_id":"s1","branch_idx":0}]\nslk {}\n')
def task(sub, key, body, rate):
    t = w / "run/nxf_work" / sub; t.mkdir(parents=True)
    name = f"3-1-1.{key[-16:]}-5VZBahJZ.bam"
    (t / name).write_bytes(body)
    (t / ".exitcode").write_text("0")
    (t / ".command.metadata").write_text(meta)
    (t / ".command.out").write_text("out")
    (t / "bowtie2.log").write_text(f"{rate}% overall alignment rate\n")
    rec = {"session": 2, "step": 5, "step_name": "bowtie2_binning_bam", "member": 0, "key": key,
           "consumes": {}, "lineage": {}, "status": "exists",
           "produces": [{"relpath": f"out/1-1-1.{key[-16:]}-5VZBahJZ.bam", "slot_id": "s1",
                         "dtype_key": "5VZBahJZ", "dtype_name": "e2::binning_bam", "branch_idx": 0,
                         "parents": [], "size": len(body)}]}
    (t / ".command.cache").write_text(json.dumps(rec) + "\n")
    s = root / key[:2] / key[2:]; (s / "out").mkdir(parents=True); (s / "tombstone").touch()
good = "1e20" + "aa" * 32; bad = "1e20" + "bb" * 32; exc = "1e20" + "cc" * 32
task("aa/111111aaaa", good, b"x" * 100 + eof, 93.3)
task("bb/222222bbbb", bad, b"x" * 100, 93.3)
task("cc/333333cccc", exc, b"x" * 100 + eof, 0.0)
(w / "exclude.txt").write_text("cc/333333\n")
EOF
cd "$HERE/../../.."
export PYTHONPATH="$PWD/src"
"${PY:-python3}" "$HERE/repromote_step.py" "$W/run" "$W/task_cache" bowtie2_binning_bam --exclude "$W/exclude.txt" --bam-min-align 50 --apply
s="$W/task_cache/1e/20$(printf 'aa%.0s' $(seq 32))"
test ! -e "$s/tombstone"
test "$(stat -c %h "$s/out/1-1-1.aaaaaaaaaaaaaaaa-5VZBahJZ.bam")" = 2
test -f "$s/manifest.cbor"
test -e "$W/task_cache/1e/20$(printf 'bb%.0s' $(seq 32))/tombstone"
echo "TEST PASSED"
