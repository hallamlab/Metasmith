#!/usr/bin/env python3
"""The flux balance arms and their nulls, as one sbatch array per job on fir.

    research/fabfos/benchmarks/fba_on_fir/run.py --preflight   what fir already has
                                                 --stage       inputs + driver + PINS.txt
                                                 --submit      one array per job
                                                 --status      one batched connect
                                                 --retrieve    shards back, then assemble

WHY THIS IS AN ARRAY AND NOT A LOOP. `ecspr fba --part i --of n` splits over DIFFERENCES,
not over tests: every test of one perturbed condition shares one model, so a boundary
drawn through a condition would build that model twice. Each task writes per-test shards
into one shared directory and a final `--part`-less run assembles them, so a walltime kill
costs the tests that task had not reached and nothing else.

THE COHORT TABLES NAME THEIR GPR FILES BY BASENAME, which is why each job stages its own
`gpr/` rather than sharing one: `fuhrer` and `eydallin` both read a file called
`gpr_gem.parquet` and they are different files.

METANETX IS ALREADY ON FIR and is not copied -- but the SLICED cache is, because building
it re-reads an 809 MB `chem_prop.tsv`, and thirty-two array tasks doing that at once on a
Lustre filesystem is the whole job's runtime spent on one table.

**NEVER `ssh fir` FROM A LOOP.** The account locks after ten failed MFA attempts and it has
happened. Connect once through the ssh tool, which holds a ControlMaster this then
multiplexes over, and let a failure stop the run.
"""
from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
HERE = Path(__file__).resolve().parent

HOST = "fir"
ROOT = "/scratch/phyberos/fabfos_fba"
MNX = "/scratch/phyberos/fabfos_refs/originals/metanetx/4.5"
SIF_DIR = "/scratch/phyberos/cache/apptainer"
ACCOUNT = "rrg-shallam-ab_cpu"

FRAMES = {
    "iML1515": REPO / "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json",
    "iECDH10B_1368": REPO / "data/fabfos/originals/genomes/e_coli_dh10b/GEM/iECDH10B_1368.json",
}

#: The sliced MetaNetX parquets. `MetBridge` writes them beside the release the first time
#: it is asked; shipping them means no array task ever builds one.
MNX_CACHE = [REPO / "data/fabfos/originals/metanetx/4.5/chem_prop_slim.parquet",
             REPO / "data/fabfos/originals/metanetx/4.5/biggM_bridge.parquet"]

B = REPO / "research/fabfos/benchmarks"


def _job(name, *, cache, gpr, frame, mnx=False, tasks=8, hours=3, tables=None):
    return dict(name=name, gpr=[Path(p) for p in gpr], frame=frame, mnx=mnx,
                tasks=tasks, hours=hours,
                tables={k: Path(v) for k, v in (tables or {
                    "conditions.parquet": cache / "conditions.parquet",
                    "gem_tests.parquet": cache / "gem_tests.parquet",
                    "media.parquet": cache / "media.parquet"}).items()})


#: One entry per array. `tasks` is sized from the LOCAL rate measured in T8, not guessed:
#: a deletion difference goes through the bounds route at about 7 tests/s, an insertion
#: rebuilds and runs about 3.9 s per difference.
HOST_GPR = REPO / "data/fabfos/runs/e_coli_bw25113/gpr/gpr_gem.parquet"
SCADC_GPR = [B / "scadc/fba/cache/tables/epi300_gem.parquet",
             B / "scadc/fba/cache/tables/scadc_fosmids_4lane.parquet"]

JOBS = {
    j["name"]: j for j in [
        _job("eydallin_obs", cache=B / "eydallin/fba/cache", gpr=[HOST_GPR],
             frame="iML1515", tasks=1, hours=1),
        _job("eydallin_null", cache=B / "eydallin/fba/cache/null", gpr=[HOST_GPR],
             frame="iML1515", tasks=4, hours=2,
             tables={"conditions.parquet": B / "eydallin/fba/cache/null/conditions.parquet",
                     "gem_tests.parquet": B / "eydallin/fba/cache/null/gem_tests.parquet",
                     "media.parquet": B / "eydallin/fba/cache/media.parquet"}),
        _job("fuhrer_obs", cache=B / "fuhrer/fba/cache", gpr=[HOST_GPR],
             frame="iML1515", tasks=32, hours=3),
        _job("fuhrer_null", cache=B / "fuhrer/fba/cache/null", gpr=[HOST_GPR],
             frame="iML1515", tasks=16, hours=3,
             tables={"conditions.parquet": B / "fuhrer/fba/cache/null/conditions.parquet",
                     "gem_tests.parquet": B / "fuhrer/fba/cache/null/gem_tests.parquet",
                     "media.parquet": B / "fuhrer/fba/cache/media.parquet"}),
        _job("scadc_obs", cache=B / "scadc/fba/cache", gpr=SCADC_GPR,
             frame="iECDH10B_1368", mnx=True, tasks=4, hours=2),
        _job("scadc_null", cache=B / "scadc/fba/cache/null", gpr=SCADC_GPR,
             frame="iECDH10B_1368", mnx=True, tasks=32, hours=6,
             tables={"conditions.parquet": B / "scadc/fba/cache/null/conditions.parquet",
                     "gem_tests.parquet": B / "scadc/fba/cache/null/gem_tests.parquet",
                     "media.parquet": B / "scadc/fba/cache/media.parquet"}),
    ]
}


def ssh(command: str, *, check=True) -> str:
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", HOST, command],
                       capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(
            f"ssh to {HOST} failed ({r.returncode}):\n{r.stderr.strip()[-2000:]}\n"
            f"Connect once through the ssh tool and re-run. Do NOT retry in a loop -- "
            f"that is what locks the account.")
    return r.stdout


def rsync(src, dest: str) -> None:
    subprocess.run(["rsync", "-a", "--info=stats1", str(src), f"{HOST}:{dest}"],
                   check=True)


def sif_name(image: str) -> str:
    return "docker.." + image.replace(":", "..").replace("/", "_") + ".sif"


def _image() -> str:
    sys.path.insert(0, str(REPO / "src"))
    import ecspr
    return f"{ecspr.CONTAINER_IMAGE}:{ecspr.CONTAINER_TAG}"


# --------------------------------------------------------------------------- verbs

def preflight(_args) -> int:
    image = _image()
    checks = [
        f'echo "IMAGE  {image}"',
        f'sacctmgr -nP show assoc user=$USER format=account | grep -qx {ACCOUNT} '
        f'&& echo "OK   account {ACCOUNT}" || echo "MISSING account {ACCOUNT}"',
        f'[ -f {MNX}/reac_prop.tsv ] && echo "OK   metanetx 4.5" '
        f'|| echo "MISSING metanetx at {MNX}"',
        f'find {SIF_DIR} "${{APPTAINER_CACHEDIR:-$HOME/.apptainer}}" -maxdepth 2 '
        f'-name "{sif_name(image)}" -print -quit 2>/dev/null | grep -q . '
        f'&& echo "OK   sif {sif_name(image)}" '
        f'|| echo "MISSING sif {sif_name(image)} -- apptainer pull it on the LOGIN node"',
        f'[ -d {ROOT}/inputs ] && echo "OK   {ROOT}/inputs staged" '
        f'|| echo "NOTE {ROOT}/inputs absent -- run --stage"',
    ]
    out = ssh("; ".join(checks))
    print(out.rstrip())
    bad = [ln for ln in out.splitlines() if ln.startswith("MISSING")]
    if bad:
        print(f"\n{len(bad)} prerequisite(s) absent. Fix them on the login node; a "
              f"compute node cannot pull an image.", file=sys.stderr)
    return 1 if bad else 0


def pins() -> str:
    def rev(p):
        return subprocess.run(["git", "-C", str(p), "rev-parse", "HEAD"],
                              capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(REPO), "status", "--porcelain"],
                           capture_output=True, text=True).stdout
    lines = [f"driver repo   {rev(REPO)} "
             f"({subprocess.run(['git', '-C', str(REPO), 'rev-parse', '--abbrev-ref', 'HEAD'], capture_output=True, text=True).stdout.strip()})",
             f"image         {_image()}",
             f"metanetx      4.5 (pinned by the .md5 sidecars beside the release)",
             "",
             "THE FLUX BALANCE ARM READS NO BAKE PRODUCT. It reads a frame GEM, a GPR "
             "table and MetaNetX, none of which a promote touches, so these numbers "
             "carry no bake generation and must not be reported with one."]
    if dirty:
        lines += ["", "WORKING TREE NOT CLEAN at stage time:"]
        lines += ["    " + ln for ln in dirty.splitlines()[:20]]
    return "\n".join(lines) + "\n"


def stage(args) -> int:
    jobs = _selected(args)
    mk = [f"mkdir -p {ROOT}/logs {ROOT}/inputs/frames {ROOT}/inputs/mnx_cache"]
    for j in jobs:
        mk.append(f"mkdir -p {ROOT}/inputs/{j['name']}/gpr {ROOT}/runs/{j['name']}/shards")
    ssh("; ".join(mk))

    for name, p in FRAMES.items():
        if any(j["frame"] == name for j in jobs):
            rsync(p, f"{ROOT}/inputs/frames/")
    if any(j["mnx"] for j in jobs):
        for p in MNX_CACHE:
            if p.exists():
                rsync(p, f"{ROOT}/inputs/mnx_cache/")
            else:
                print(f"  NOTE {p.name} is not built locally; fir will slice it once",
                      file=sys.stderr)
    for j in jobs:
        for p in j["gpr"]:
            rsync(p, f"{ROOT}/inputs/{j['name']}/gpr/")
        for dest, p in j["tables"].items():
            rsync(p, f"{ROOT}/inputs/{j['name']}/{dest}")

    (HERE / "PINS.txt").write_text(pins())
    rsync(HERE / "PINS.txt", f"{ROOT}/PINS.txt")
    rsync(HERE / "fba_array.sbatch", f"{ROOT}/fba_array.sbatch")
    (HERE / "jobs.json").write_text(json.dumps(
        {j["name"]: dict(tasks=j["tasks"], hours=j["hours"], frame=j["frame"],
                         mnx=j["mnx"], gpr=[p.name for p in j["gpr"]]) for j in jobs},
        indent=2) + "\n")
    rsync(HERE / "jobs.json", f"{ROOT}/jobs.json")
    print(f"staged {len(jobs)} job(s) -> {HOST}:{ROOT}")
    return 0


def submit(args) -> int:
    image = _image()
    sif = f"{SIF_DIR}/{sif_name(image)}"
    out = []
    for j in _selected(args):
        env = " ".join(shlex.quote(f"{k}={v}") for k, v in dict(
            JOB=j["name"], SIF=sif, ROOT=ROOT, MNX=(MNX if j["mnx"] else ""),
            FRAME=j["frame"], GPR=" ".join(p.name for p in j["gpr"]),
            NTASKS=str(j["tasks"])).items())
        cmd = (f"sbatch --account={ACCOUNT} --job-name=fba_{j['name']} "
               f"--array=0-{j['tasks'] - 1} --time={j['hours']}:00:00 "
               f"--export=ALL,{env.replace(' ', ',')} {ROOT}/fba_array.sbatch")
        out.append(f"### {j['name']}\n" + ssh(cmd).strip())
    print("\n".join(out))
    print("\nCheck later with --status. Never poll in a loop.")
    return 0


def status(args) -> int:
    parts = ["squeue -u $USER -o '%.10i %.14j %.8T %.10M %.6D %R' | head -40",
             "echo '== shards =='"]
    for j in _selected(args):
        parts.append(f"printf '%-16s ' {j['name']}; "
                     f"ls {ROOT}/runs/{j['name']}/shards 2>/dev/null | wc -l")
    parts.append("echo '== last log lines =='")
    parts.append(f"tail -n 2 -q $(ls -t {ROOT}/logs/*.out 2>/dev/null | head -4) "
                 f"2>/dev/null || true")
    print(ssh("; ".join(parts)).rstrip())
    return 0


def retrieve(args) -> int:
    for j in _selected(args):
        dest = HERE / "retrieved" / j["name"]
        dest.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-a", "--info=stats1",
                        f"{HOST}:{ROOT}/runs/{j['name']}/shards/", str(dest) + "/"],
                       check=True)
        n = len(list(dest.glob("*.json")))
        print(f"{j['name']}: {n:,} shard(s) -> {dest}")
    print("\nAssemble with a --part-less `ecspr fba` run pointed at --shard-dir; it "
          "solves nothing that already has a shard.")
    return 0


def _selected(args) -> list:
    names = args.jobs or list(JOBS)
    bad = [n for n in names if n not in JOBS]
    if bad:
        raise SystemExit(f"unknown job(s) {bad}; have {sorted(JOBS)}")
    return [JOBS[n] for n in names]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("jobs", nargs="*", help=f"which of {sorted(JOBS)}; default all")
    for verb in ("preflight", "stage", "submit", "status", "retrieve"):
        ap.add_argument(f"--{verb}", action="store_const", const=verb, dest="verb")
    a = ap.parse_args()
    if not a.verb:
        ap.print_help()
        return 1
    return dict(preflight=preflight, stage=stage, submit=submit,
                status=status, retrieve=retrieve)[a.verb](a)


if __name__ == "__main__":
    raise SystemExit(main())
