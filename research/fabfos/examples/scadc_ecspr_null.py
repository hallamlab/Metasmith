#!/usr/bin/env python3
"""Frozen ECSPr null for the SCADC measurement (plan T2): draws over the
metagenome ORF pool, size-matched to each observed unit's `n_orfs`, solved on
the SAME atom-resolved engine `scadc_ecspr.py` used for the observed run.

    python examples/scadc_ecspr_null.py --preflight
    python examples/scadc_ecspr_null.py --run --pilot        # ~20 draws/bucket/style
    python examples/scadc_ecspr_null.py --run                # ~1000 draws/bucket/style
    python examples/scadc_ecspr_null.py --retrieve --pilot
    python examples/scadc_ecspr_null.py --publish --pilot    # data/fabfos/runs/scadc_ecspr/null/

WHY THIS DOES NOT GO THROUGH THE METASMITH TransformInstance MACHINERY
------------------------------------------------------------------------
Neither does `scadc_ecspr.py`, which this reuses verbatim (same pairs/ratios
loaders, same `graph_from_pairs`/`solve`, same host-weights-plus-unit-weights
composition): `ecspr_measure.py`'s `protocol()` is still a stub, and there is
no `null_draw` transform to plan against. Building one for a computation this
repo has run exactly once (as `scadc_ecspr.py`) would be new framework work
this plan does not need -- the compute itself is plain numpy/pandas/scipy, so
it runs the same way `compile_metag_gpr.py` did for T1: staged at absolute
fir paths, executed inside the pre-cached `python_for_data_science` apptainer
image via a plain `sbatch` job, one `ssh_once` per step, never a poll loop
(see `examples/_driver.py::ssh_once`'s "NEVER call this in a loop" warning --
`--run` submits and returns; check progress with `--status`, retrieve with
`--retrieve` once SLURM shows the job finished).

THE NULL BASIS
---------------
`examples/scadc_ecspr_null_draw.py` (staged to fir, not run locally) draws
ORFs from the metag pool (`data/fabfos/runs/scadc_metagenome/`, published by T1) in two
styles -- A (uniform) and D (contiguous window on one contig) -- at N sizes
bucketed off the OBSERVED run's own `n_orfs` distribution (computed fresh
each `--run`, not copied from any other run's fixed buckets), resolves each
draw to `{mnxr: E}` via `ecspr.model.evidence.per_unit_weights(metag_gpr, "orf")`,
adds the same host GEM weights `scadc_ecspr.py` adds, and solves. See that
script's own docstring for the resumability/reproducibility contract.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ECSPR_PKG = ROOT / "src" / "ecspr"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import FIR_HOST, ssh_once  # noqa: E402

METAGENOME = ROOT / "data" / "fabfos" / "runs" / "scadc_metagenome"
ECSPR = ROOT / "data" / "fabfos" / "runs" / "scadc_ecspr"
REFS = ECSPR / "refs"
CONDITIONS = ECSPR / "conditions.parquet"
RESULTS = ECSPR / "results.parquet"
HOST_GEM = ROOT / "data" / "fabfos" / "runs" / "e_coli_epi300" / "gpr" / "gpr_gem.parquet"
NULL_OUT = ECSPR / "null"

REMOTE_WORK = "/scratch/phyberos/fabfos_metagenome"
REMOTE_METAG_GPR = f"{REMOTE_WORK}/results/metag_gpr_3lane.parquet"
REMOTE_METAG_ORFS = f"{REMOTE_WORK}/raw/metag.orfs.csv"
REMOTE_REFS = f"{REMOTE_WORK}/ecspr_refs"
REMOTE_LIB = f"{REMOTE_WORK}/lib"
SIF = ("/scratch/phyberos/cache/apptainer/"
      "docker..quay.io_hallamlab_python_for_data_science..1.2.5.sif")

LOCAL_SCRIPT = Path(__file__).resolve().parent / "scadc_ecspr_null_draw.py"
FULL_K = 1000
PILOT_K = 20


def remote_out(pilot: bool) -> str:
    return f"{REMOTE_WORK}/results/null_draws_{'pilot' if pilot else 'full'}.csv"


def sbatch_script(pilot: bool) -> str:
    k = PILOT_K if pilot else FULL_K
    out = remote_out(pilot)
    mem = "16G" if pilot else "32G"
    hours = "03:00:00" if pilot else "48:00:00"
    return f"""#!/bin/bash
#SBATCH --job-name=scadc_ecspr_null_{'pilot' if pilot else 'full'}
#SBATCH --account=rrg-shallam-ab
#SBATCH --cpus-per-task=4
#SBATCH --mem={mem}
#SBATCH --time={hours}
#SBATCH --output={REMOTE_WORK}/logs/null_{'pilot' if pilot else 'full'}_%j.log

set -euo pipefail
apptainer exec --bind /scratch/phyberos:/scratch/phyberos {SIF} \\
  python3 {REMOTE_WORK}/scadc_ecspr_null_draw.py \\
    --metag-gpr  {REMOTE_METAG_GPR} \\
    --metag-orfs {REMOTE_METAG_ORFS} \\
    --pairs      {REMOTE_REFS}/atom_pairs.parquet \\
    --ratios     {REMOTE_REFS}/direction_ratios.parquet \\
    --host-gem   {REMOTE_REFS}/gpr_gem.parquet \\
    --conditions {REMOTE_REFS}/conditions.parquet \\
    --observed-n {REMOTE_REFS}/observed_n_orfs.txt \\
    --lib-dir    {REMOTE_LIB}/ecspr \\
    --k {k} --seed 20260731 \\
    --out {out} \\
    --resume
echo "SBATCH_DONE rc=$?"
"""


def write_observed_n() -> Path:
    import struct
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise SystemExit(
            "pyarrow is not available locally to read results.parquet's n_orfs "
            "column. Run on a host with pandas/pyarrow, or hand-edit "
            "data/fabfos/runs/scadc_ecspr/refs/observed_n_orfs.txt directly.")
    t = pq.read_table(RESULTS, columns=["unit", "n_orfs"])
    d = t.to_pydict()
    ns = sorted({n for u, n in zip(d["unit"], d["n_orfs"]) if u != "epi300_host"})
    path = REFS / "observed_n_orfs.txt"
    path.write_text("\n".join(str(n) for n in ns) + "\n")
    print(f"  {path.relative_to(ROOT)}  ({len(ns)} distinct N)")
    return path


def stage(host: str) -> None:
    print(f"=== staging refs + the ecspr package + driver script on {host} ===")
    ssh_once(host, f"mkdir -p {REMOTE_REFS} {REMOTE_LIB} {REMOTE_WORK}/logs "
                   f"{REMOTE_WORK}/results")
    observed_n = write_observed_n()
    for src in (REFS / "atom_pairs.parquet", REFS / "direction_ratios.parquet",
                CONDITIONS, HOST_GEM, observed_n):
        subprocess.run(["scp", "-q", str(src), f"{host}:{REMOTE_REFS}/"], check=True)
    subprocess.run(["ssh", host, f"rm -rf {REMOTE_LIB}/ecspr"], check=True)
    subprocess.run(["scp", "-qr", str(ECSPR_PKG), f"{host}:{REMOTE_LIB}/"], check=True)
    subprocess.run(["scp", "-q", str(LOCAL_SCRIPT), f"{host}:{REMOTE_WORK}/"],
                   check=True)


def preflight(host: str) -> int:
    print(f"=== preflight: what {host} must hold ===")
    probe = "; ".join([
        f'[ -e "{REMOTE_METAG_GPR}" ] || echo "MISSING metag GPR table (run T1 first)"',
        f'[ -e "{REMOTE_METAG_ORFS}" ] || echo "MISSING metag.orfs.csv"',
        f'[ -e "{SIF}" ] || echo "MISSING python_for_data_science image"',
    ])
    out = ssh_once(host, probe).strip()
    if out:
        print(out, file=sys.stderr)
        return 1
    print("  metag GPR table, metag.orfs.csv, apptainer image: present")
    return 0


def run(host: str, pilot: bool) -> int:
    stage(host)
    if preflight(host):
        return 1
    remote_sbatch = f"{REMOTE_WORK}/scadc_ecspr_null_{'pilot' if pilot else 'full'}.sbatch"
    script = sbatch_script(pilot)
    submit = ssh_once(
        host,
        f"cat > {remote_sbatch} <<'EOF'\n{script}EOF\n"
        f"cd {REMOTE_WORK} && sbatch {remote_sbatch}")
    print(submit.strip())
    print("\nCheck progress by hand (do not poll in a loop):\n"
          f"  ssh {host} squeue -u phyberos\n"
          f"  ssh {host} tail -f {REMOTE_WORK}/logs/null_"
          f"{'pilot' if pilot else 'full'}_<jobid>.log")
    return 0


def retrieve(host: str, pilot: bool) -> int:
    NULL_OUT.mkdir(parents=True, exist_ok=True)
    local = NULL_OUT / f"draws_{'pilot' if pilot else 'full'}.csv"
    print(f"=== retrieving {remote_out(pilot)} -> {local} ===")
    subprocess.run(["scp", "-q", f"{host}:{remote_out(pilot)}", str(local)], check=True)
    n = sum(1 for _ in local.open()) - 1
    print(f"  {n:,} draw-rows")
    return 0


def publish(pilot: bool) -> int:
    import pyarrow.csv as pcsv
    import pyarrow.parquet as pq
    local = NULL_OUT / f"draws_{'pilot' if pilot else 'full'}.csv"
    if not local.exists():
        print(f"{local} not found -- run --retrieve first", file=sys.stderr)
        return 1
    table = pcsv.read_csv(local)
    out = NULL_OUT / "draws.parquet"
    pq.write_table(table, out)
    print(f"  {out.relative_to(ROOT)}  ({table.num_rows:,} rows)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--preflight", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--retrieve", action="store_true")
    ap.add_argument("--publish", action="store_true")
    ap.add_argument("--pilot", action="store_true",
                    help=f"{PILOT_K} draws/bucket/style instead of {FULL_K}")
    ap.add_argument("--host", default=FIR_HOST)
    a = ap.parse_args()

    if a.preflight:
        return preflight(a.host)
    if a.run:
        return run(a.host, a.pilot)
    if a.retrieve:
        return retrieve(a.host, a.pilot)
    if a.publish:
        return publish(a.pilot)
    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
