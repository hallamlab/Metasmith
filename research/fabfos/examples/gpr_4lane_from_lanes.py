#!/usr/bin/env python3
"""The mapper step alone, over a run whose lanes finished but whose mapper never ran.

    PATH="/home/tony/lib/miniforge3/envs/msm/bin:$PATH" \\
        python examples/gpr_4lane_from_lanes.py --run-key 8vBqTmYA --submit
    PATH="..." python examples/gpr_4lane_from_lanes.py --run-key 8vBqTmYA \\
        --into data/fabfos/runs/e_coli_k12 --fetch

WHY THIS EXISTS. `proteinbert` is the one step in the annotation graph with two
products, and its outputs reach the next step carrying only their own lineage key --
no ancestors. `gpr_4lane`'s `group(...)` join therefore classifies them
DESCENDANT_OF_BY, hits a null index, and yields an empty channel, so step 9 is never
submitted. The run has done all its expensive work and is missing only the cheap
step. This renders the SHIPPED `gpr_4lane` DRIVER against that run's published lane
outputs and runs it where they already are.

THE DRIVER IS NOT REIMPLEMENTED, it is substituted -- the same text the transform's
`protocol()` formats, with the same keys, read out of the transform at call time. A
second implementation of the mapper would be a second thing that can disagree with
the tables the graph writes, which is the one property this workaround must not cost.

Its output is `annotation::gpr_table` by construction and by content, so
`host_denovo_from_mapper.py` consumes it exactly as it consumes a graph-produced one.
Delete this script when the lineage bug is fixed.
"""
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
MLIB = REPO / "src" / "metasmith_libraries"
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _driver import SOCKEYE_ACCOUNT, SOCKEYE_AGENT_HOME, SOCKEYE_HOST  # noqa: E402

IMAGE_STORE = "/arc/project/st-shallam-1/metasmith/container_images"
PROCESSED = "/arc/project/st-shallam-1/fabfos_refs/processed"

WANT = {
    "7_annotation-kofamscan_results": "kofam",
    "2_annotation-clean_predictions": "clean",
    "6_annotation-diamond_uniref50_results": "uniref",
    "8_annotation-proteinbert_embeddings": "pbert_emb",
}


def ssh(cmd: str) -> str:
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", SOCKEYE_HOST, cmd],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(f"ssh failed: {r.stderr.strip()}")
    return r.stdout


def render(paths: dict, out: str, source: str, threads: int) -> str:
    src = (MLIB / "transforms" / "fabfos" / "gpr_4lane.py").read_text()
    marker = "DRIVER = r'''"
    start = src.index(marker) + len(marker)
    end = src.index("'''", start)
    ns: dict = {}
    exec(f"DRIVER = {src[start:end]!r}", ns)
    return ns["DRIVER"].format(
        ev_lib=f"{SOCKEYE_AGENT_HOME}/dev/fabfos_evidence.py",
        bridge=f"{PROCESSED}/mnxr_lookup/mnxr_lookup.parquet",
        pool=f"{PROCESSED}/label_transfer_landmarks/landmarks",
        out=out, lane_set="chosen_4", source=source, threads=threads, **paths)


def discover(run_key: str) -> tuple[dict, str, str]:
    results = f"{SOCKEYE_AGENT_HOME}/runs/{run_key}/results"
    listing = ssh(f"ls -d {results}/*/ 2>/dev/null; echo ---; "
                  f"find {SOCKEYE_AGENT_HOME}/runs/{run_key}/_metasmith/task/data "
                  f"-name '*.faa'")
    dirs, _, faas = listing.partition("---\n")
    faa = faas.strip().splitlines()
    if len(faa) != 1:
        raise SystemExit(f"expected one staged ORF FASTA for {run_key}, found {faa}")

    paths = {}
    for d in (x.strip().rstrip("/") for x in dirs.splitlines() if x.strip()):
        key = WANT.get(Path(d).name)
        if key is None:
            continue
        files = [f for f in ssh(f"ls {shlex.quote(d)}").split() if f]
        if len(files) != 1:
            raise SystemExit(f"expected one file in {d}, found {files}")
        paths[key] = f"{d}/{files[0]}"
    missing = sorted(set(WANT.values()) - set(paths))
    if missing:
        raise SystemExit(
            f"run {run_key} has no published output for {missing}. This script only "
            f"fills in the mapper; a missing LANE means the run failed earlier and "
            f"needs re-running, not patching.")
    paths["orfs"] = faa[0]
    return paths, faa[0], Path(faa[0]).stem


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-key", required=True)
    ap.add_argument("--into", type=Path,
                    help="local run dir; --fetch lands gpr/gpr_denovo_mapper.parquet here")
    ap.add_argument("--submit", action="store_true")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--threads", type=int, default=8)
    a = ap.parse_args()

    work = f"{SOCKEYE_AGENT_HOME}/runs/{a.run_key}/_gpr_fixup"
    out = f"{work}/gpr_denovo_mapper.parquet"

    if a.fetch:
        if not a.into:
            raise SystemExit("--fetch needs --into")
        dest = a.into / "gpr"
        dest.mkdir(parents=True, exist_ok=True)
        r = subprocess.run(["scp", f"{SOCKEYE_HOST}:{out}",
                            str(dest / "gpr_denovo_mapper.parquet")])
        if r.returncode != 0:
            raise SystemExit("the mapper table is not there yet -- check the job log")
        print(f"-> {dest}/gpr_denovo_mapper.parquet")
        return 0

    paths, faa, source = discover(a.run_key)
    print(f"run {a.run_key}: source={source}")
    for k in sorted(paths):
        print(f"  {k:12s} {paths[k]}")
    driver = render(paths, out, source, a.threads)

    ssh(f"mkdir -p {work} && cp {MLIB}/resources/lib/fabfos_evidence.py "
        f"{SOCKEYE_AGENT_HOME}/dev/ 2>/dev/null || true")
    subprocess.run(["scp", str(MLIB / "resources" / "lib" / "fabfos_evidence.py"),
                    f"{SOCKEYE_HOST}:{SOCKEYE_AGENT_HOME}/dev/"], check=True)

    local = Path(f"/tmp/_gpr_4lane.{a.run_key}.py")
    local.write_text(driver)
    subprocess.run(["scp", str(local), f"{SOCKEYE_HOST}:{work}/_gpr_4lane.py"], check=True)

    img = ssh(f"ls {IMAGE_STORE}/*python_for_data_science*.sif | head -1").strip()
    if not img:
        raise SystemExit(f"no python_for_data_science image under {IMAGE_STORE}")
    sbatch = f"""#!/bin/bash
#SBATCH --account={SOCKEYE_ACCOUNT}
#SBATCH --job-name=gpr4lane_{source}
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={a.threads}
#SBATCH --mem=32G
#SBATCH --time=2:00:00
#SBATCH --output={work}/gpr.%j.log
#SBATCH --error={work}/gpr.%j.log
set -euo pipefail
module load gcc/9.4.0 apptainer/1.3.1
export GPR_THREADS={a.threads}
cd {work}
apptainer exec \\
  --bind {SOCKEYE_AGENT_HOME}:{SOCKEYE_AGENT_HOME} \\
  --bind {PROCESSED}:{PROCESSED} \\
  {img} python3 {work}/_gpr_4lane.py
echo EXIT_OK
"""
    lsb = Path(f"/tmp/run_gpr.{a.run_key}.sh")
    lsb.write_text(sbatch)
    subprocess.run(["scp", str(lsb), f"{SOCKEYE_HOST}:{work}/run_gpr.sh"], check=True)
    if not a.submit:
        print(f"\nstaged only. Add --submit.\n  {work}/run_gpr.sh")
        return 0
    print(ssh(f"cd {work} && sbatch run_gpr.sh"))
    print(f"log: {work}/gpr.<jobid>.log\nthen: --into <dir> --fetch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
