#!/usr/bin/env bash
# One prompt revision, end to end: the split, the control gate, two scoreboard rows.
#
# A held GPU allocation bills wall-clock whether or not the card is busy, so a revision
# is one command rather than four typed in sequence.
#
#     ./revise.sh aam_r1 "annotated structures, worked polymer cases"
#     LIMIT=20 ./revise.sh aam_r1 "probe"        # first 20 records only
#     SPLIT=heldout ./revise.sh aam_r3 "final"   # scored once, at the end
set -euo pipefail

REV="${1:?usage: revise.sh <revision> [note]}"
NOTE="${2:-}"
SPLIT="${SPLIT:-dev}"
LIMIT="${LIMIT:-0}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8080/v1}"
MODEL="${MODEL:-qwen3-32b}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
RUNS="$HERE/runs"; mkdir -p "$RUNS"

# The env interpreters are called directly rather than through `mamba run`, which
# buffers a long run's progress until it exits and whose --no-capture-output is broken
# in mamba 2.5.0.
ENVS=/home/tony/lib/miniforge3/envs
export PYTHONPATH="$ROOT/src"

run () { "$ENVS/ecspr/bin/python" -u "$HERE/run_panel.py" --limit "$LIMIT" \
             --base-url "$BASE_URL" --model "$MODEL" "$@"; }
score () { "$ENVS/rdkit-scratch/bin/python" -u "$HERE/arbiter.py" \
               --scoreboard "$HERE/scoreboard.tsv" "$@"; }

echo "=== $REV over $SPLIT ==="
run --prompt "$HERE/prompts/$REV.md" --split "$HERE/panel/$SPLIT.jsonl" \
    --out "$RUNS/$REV.$SPLIT.jsonl"
score --run "$RUNS/$REV.$SPLIT.jsonl" --panel "$HERE/panel/$SPLIT.jsonl" \
      --detail "$RUNS/$REV.$SPLIT.tsv" --note "$NOTE"

# Zero regressions is a gate, not a metric: a revision that buys coverage by rewriting
# reactions that already bank has not earned the coverage.
echo "=== $REV control gate ==="
run --prompt "$HERE/prompts/$REV.md" --split "$HERE/panel/controls.jsonl" \
    --out "$RUNS/$REV.controls.jsonl"
score --run "$RUNS/$REV.controls.jsonl" --panel "$HERE/panel/controls.jsonl" --controls \
      --detail "$RUNS/$REV.controls.tsv" --note "control gate: $NOTE"
