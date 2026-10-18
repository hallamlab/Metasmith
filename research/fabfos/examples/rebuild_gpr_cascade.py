#!/usr/bin/env python3
"""Publish this campaign's lanes and rebuild every de-novo GPR table that follows.

    python research/fabfos/examples/rebuild_gpr_cascade.py --dry-run
    python research/fabfos/examples/rebuild_gpr_cascade.py

Seven ORF sets were annotated twice, once per cluster, and whichever run finished first
holds the lanes. This finds that run's retrieved results, publishes its lane tables and
mapper output into the run tier, then walks the derivations in dependency order: the six
host proteomes and the clone cohort come from their own mapper output, AG1 and LW06 are
their parents minus a named edit list, and ASKA and SCALES read AG1 and BW25113 in turn.

Running a later step against a stale parent produces a table that is wrong rather than
missing, which is why the order here is fixed rather than a set of independent commands.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SCRATCH = REPO / "data" / "fabfos" / "scratch"
LOCAL_SCRATCH = REPO / "data" / "scratch"
BREF = REPO / "src" / "fabfos" / "build_references"
BENCH = REPO / "research" / "fabfos" / "benchmarks"
# dvc lives in its own env, not the project one -- `mamba run -n msm dvc` finds nothing.
DVC = "/home/tony/lib/miniforge3/envs/dvc/bin/dvc"

# run name -> the ORF-set stem its work directories are named after
SETS = {
    "e_coli_dh1": "NC_017638.1",
    "e_coli_k12": "NC_000913.3",
    "e_coli_dh10b": "NC_010473.1",
    "e_coli_epi300": "CP189566.1",
    "e_coli_bw25113": "CP193896.1",
    "e_coli_w3110": "CP165600.1",
    "eydallin_clones": "eydallin_clones",
}

HOSTS = ["e_coli_dh1", "e_coli_k12", "e_coli_dh10b", "e_coli_epi300",
         "e_coli_bw25113", "e_coli_w3110"]


def results_for(name: str) -> Path | None:
    """The finished run's retrieved results, whichever cluster got there first."""
    for work in (LOCAL_SCRATCH / f"clone_gpr_fir_{SETS[name]}",
                 LOCAL_SCRATCH / f"race_sockeye_{SETS[name]}"):
        r = work / "results"
        if (r / "annotation-gpr_table").is_dir() and any(
                (r / "annotation-gpr_table").glob("*.parquet")):
            return r
    return None


def run(cmd: list[str], *, dry: bool) -> int:
    print(f"\n$ {' '.join(str(c) for c in cmd)}", flush=True)
    if dry:
        return 0
    return subprocess.run([str(c) for c in cmd], cwd=REPO).returncode


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-publish", action="store_true",
                    help="the lanes are already in the run tier; rebuild only")
    a = ap.parse_args()
    py = [sys.executable]

    found = {name: results_for(name) for name in SETS}
    missing = [n for n, r in found.items() if r is None]
    for name, r in found.items():
        print(f"  {name:<20} {r.parent.name if r else 'NO FINISHED RUN'}")
    if missing:
        print(f"\n{len(missing)} set(s) have no finished run: {missing}", file=sys.stderr)
        return 1

    # DVC materialises a pinned out as a read-only hardlink into its cache, so every
    # publisher below would die on PermissionError against a tree that is up to date.
    # `unprotect` swaps the link for a writable copy and leaves the cache intact.
    tracked = [d for d in (REPO / "data" / "fabfos" / "runs").glob("*/gpr")
               if (d.parent / "gpr.dvc").exists()]
    if run([DVC, "unprotect", *tracked], dry=a.dry_run):
        return 2

    if not a.skip_publish:
        for name in SETS:
            if run(py + [REPO / "research/fabfos/examples/clone_gpr_on_hpc.py",
                         "--work", found[name].parent,
                         "--site", "fir" if "clone_gpr_fir" in found[name].parts[-2] else "sockeye",
                         "--into", REPO / "data" / "fabfos" / "runs" / name,
                         "--publish"], dry=a.dry_run):
                return 2

    for host in HOSTS:
        if run(py + [BREF / "host_denovo_from_mapper.py", "--host", host,
                     "--mapper", found[host], "--publish"], dry=a.dry_run):
            return 3

    steps = [
        # this one wants the parquet itself, where host_denovo_from_mapper takes the tree
        (BENCH / "eydallin" / "build_clone_gpr_denovo.py",
         ["--mapper", next((found["eydallin_clones"] / "annotation-gpr_table").glob("*.parquet")),
          "--publish"]),
        (BREF / "derive_ag1_denovo.py", ["--publish"]),
        (BREF / "derive_lw06_denovo.py", ["--publish"]),
        (BENCH / "eydallin" / "build_aska_gpr.py", ["--publish"]),
        (BENCH / "scales" / "build_scales_gpr.py", ["--publish"]),
    ]
    for script, args in steps:
        if run(py + [script, *args], dry=a.dry_run):
            return 4

    print("\ncascade rebuilt. Now: the two GPR test modules, then `dvc add` each "
          "runs/<name>/{annotations,gpr}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
