"""Check results/pinned_config_diff.tsv against the sources it cites.

Every e1_source / e2_source is `path:line`, relative to the nf-core/mag root when prefixed
`nf-core/mag@5.5.0:` and to the repository root otherwise. The cited line must exist and
contain the row's e1_evidence / e2_evidence, compared with whitespace runs collapsed. A row's
status must also agree with its two values.
"""
import argparse
import csv
import re
import sys
from pathlib import Path

NF_PREFIX = "nf-core/mag@5.5.0:"
ABSENT = "(absent)"
STATUSES = {"same", "differ", "e1-only", "e2-only", "threads-differ", "reported"}
COLUMNS = ["arm", "tool", "argument", "e1_value", "e2_value", "status",
           "e1_source", "e2_source", "note", "e1_evidence", "e2_evidence"]
REPO = Path(__file__).resolve().parents[3]


def collapse(text):
    return re.sub(r"\s+", " ", text).strip()


def check_citation(side, source, evidence, roots, cache):
    if not source and not evidence:
        return None
    if not source or not evidence:
        return f"{side}: source and evidence must both be set or both blank"
    root, rel = (roots["nf"], source[len(NF_PREFIX):]) if source.startswith(NF_PREFIX) else (roots["repo"], source)
    path_text, _, line_text = rel.rpartition(":")
    if not path_text or not line_text.isdigit():
        return f"{side}: malformed source {source!r}"
    path = root / path_text
    if path not in cache:
        if not path.is_file():
            return f"{side}: no such file {path}"
        cache[path] = path.read_text(errors="replace").splitlines()
    lines, n = cache[path], int(line_text)
    if not 1 <= n <= len(lines):
        return f"{side}: {source} is past the end of a {len(lines)}-line file"
    if collapse(evidence) not in collapse(lines[n - 1]):
        return f"{side}: {source} lacks {evidence!r}; the line reads {lines[n - 1].strip()!r}"
    return None


def check_status(row):
    status, v1, v2 = row["status"], row["e1_value"], row["e2_value"]
    if status not in STATUSES:
        return f"unknown status {status!r}"
    if status == "reported":
        return None if row["tool"] == "slurm" else "status 'reported' is only for tool 'slurm'"
    if status == "threads-differ" and row["argument"] != "threads":
        return "status 'threads-differ' is only for argument 'threads'"
    expected = {
        "same": v1 == v2,
        "differ": v1 != v2 and ABSENT not in (v1, v2),
        "threads-differ": v1 != v2,
        "e1-only": v2 == ABSENT and v1 != ABSENT and not row["e2_source"],
        "e2-only": v1 == ABSENT and v2 != ABSENT and not row["e1_source"],
    }[status]
    return None if expected else f"status {status!r} disagrees with values {v1!r} / {v2!r}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nfcore", type=Path, required=True, help="extracted nf-core/mag 5.5.0 source tree")
    parser.add_argument("--repo", type=Path, default=REPO, help=f"repository root (default {REPO})")
    parser.add_argument("--tsv", type=Path, help="default: <repo>/research/metasmith_benchmark/results/pinned_config_diff.tsv")
    args = parser.parse_args()
    tsv = args.tsv or args.repo / "research/metasmith_benchmark/results/pinned_config_diff.tsv"
    roots = {"nf": args.nfcore, "repo": args.repo}

    with open(tsv, newline="") as f:
        reader = csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        header = next(reader)
        if header != COLUMNS:
            print(f"header is {header}, expected {COLUMNS}")
            return 1
        rows = list(reader)

    cache, failures = {}, 0
    for i, cells in enumerate(rows, start=2):
        if len(cells) != len(COLUMNS):
            print(f"line {i}: {len(cells)} columns, expected {len(COLUMNS)}")
            failures += 1
            continue
        row = dict(zip(COLUMNS, cells))
        problems = [p for p in (
            check_status(row),
            check_citation("e1", row["e1_source"], row["e1_evidence"], roots, cache),
            check_citation("e2", row["e2_source"], row["e2_evidence"], roots, cache),
        ) if p]
        for p in problems:
            print(f"line {i} [{row['arm']} | {row['tool']} | {row['argument']}]: {p}")
        failures += bool(problems)

    if failures:
        print(f"{failures} of {len(rows)} rows failed")
        return 1
    print(f"{len(rows)} rows OK: every cited line exists and carries its evidence")
    return 0


if __name__ == "__main__":
    sys.exit(main())
