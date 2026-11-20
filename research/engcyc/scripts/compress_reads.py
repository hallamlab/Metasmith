"""Compress uncompressed read files on Sockeye and put the .gz on chinook in place of the originals.

Reads plan files (see move_deliveries.py) and works in <work>/. Stages, each safe to rerun:
  plan     write units.tsv: fastq -> fastq.gz, sff -> sff.gz, a 454 fna+qual -> one fastq.gz
  stage    Globus the inputs of not-yet-staged units to Sockeye scratch, at most --max-bytes in flight
  run      submit a Slurm array of gz_worker.py over every staged unit without a result
  upload   Globus every ok output to its chinook path, checksum-verified
  finish   once an output is on chinook at its size: delete the original on chinook (only when --from-chinook)
           and the unit's scratch, and rewrite the plans so their rows name the .gz files
  status   count units per state
"""
import argparse
import csv
import json
import re
import subprocess
import sys
from collections import Counter, defaultdict
from pathlib import Path, PurePosixPath as P

from move_deliveries import CHINOOK, GLOBUS, Chinook, delivery, globus, read_plan

SOCKEYE = "64a5c402-05c4-4607-bbad-46a9c2aebd98"
SCRATCH = "/scratch/st-shallam-1/txyliu/engcyc_gz"
WORKER = Path(__file__).with_name("gz_worker.py")
READS = re.compile(r"\.(fastq|fq|fasta|fas|sff)$")
FNA = re.compile(r"\.fna(\.txt)?$")
QUAL = re.compile(r"\.qual(\.txt)?$")
UNIT_COLS = ["unit", "kind", "plan", "out_dest", "in_bytes", "inputs"]
TASK_COLS = ["stage", "task_id", "units"]


def tsv(path):
    if not path.exists():
        return []
    with open(path) as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def append(path, cols, row):
    new = not path.exists()
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, cols, delimiter="\t", lineterminator="\n")
        if new:
            w.writeheader()
        w.writerow(row)


def ssh(cmd, **kw):
    return subprocess.run(["ssh", "sockeye", cmd], capture_output=True, text=True, check=True, **kw).stdout


def plan_units(plans, from_chinook):
    units = []
    for plan in plans:
        ds = plan.parent.name
        rows = read_plan(plan)
        src = (lambda r: (CHINOOK, r["dest_path"])) if from_chinook else (lambda r: (r["source_endpoint"], r["source_path"]))
        quals = {QUAL.sub("", r["dest_path"]): r for r in rows if QUAL.search(r["dest_path"])}
        fnas = {FNA.sub("", r["dest_path"]) for r in rows if FNA.search(r["dest_path"])}
        for r in rows:
            dest = r["dest_path"]
            if READS.search(dest):
                kind, inputs, out = "gzip", [r], dest + ".gz"
            elif FNA.search(dest) and FNA.sub("", dest) in quals:
                kind, inputs, out = "fnaqual", [r, quals[FNA.sub("", dest)]], FNA.sub("", dest) + ".fastq.gz"
            elif FNA.search(dest) or (QUAL.search(dest) and QUAL.sub("", dest) not in fnas):
                kind, inputs, out = "gzip", [r], dest + ".gz"
            else:
                continue
            units.append({"unit": f"{ds}.{len(units):05d}", "kind": kind, "plan": str(plan.resolve()), "out_dest": out,
                          "in_bytes": sum(int(i["bytes"]) for i in inputs),
                          "inputs": json.dumps([[*src(i), i["dest_path"]] for i in inputs])})
    return units


def tasks_by_unit(work, stage):
    """Map unit -> Globus status of the latest task of this stage that carried it."""
    status, out = {}, {}
    for t in tsv(work / "tasks.tsv"):
        if t["stage"] != stage:
            continue
        if t["task_id"] not in status:
            status[t["task_id"]] = json.loads(globus("task", "show", t["task_id"], "--format", "json"))["status"]
        for u in t["units"].split():
            out[u] = status[t["task_id"]]
    return out


def results():
    text = ssh(f"cat {SCRATCH}/results/*.tsv 2>/dev/null || true")
    return {f[0]: dict(zip(["unit", "status", "bytes", "md5", "reads", "error"], f))
            for f in (l.split("\t") for l in text.splitlines() if l)}


def transfer(src, dst, pairs, label):
    batch = "".join(f'"{a}" "{b}"\n' for a, b in pairs)
    out = subprocess.run([GLOBUS, "transfer", src, dst, "--batch", "-", "--sync-level", "checksum", "--verify-checksum",
                          "--label", label, "--format", "json"], input=batch, capture_output=True, text=True, timeout=600)
    if out.returncode:
        raise RuntimeError(out.stderr.strip())
    return json.loads(out.stdout)["task_id"]


def finished(work):
    return {r["unit"] for r in tsv(work / "finished.tsv")}


def rewrite_plans(units, res):
    """Replace each finished unit's input rows with one row for its output, keeping the first input's source."""
    by_plan = defaultdict(list)
    for u in units:
        by_plan[u["plan"]].append(u)
    for plan, group in by_plan.items():
        rows = read_plan(plan)
        cols = list(rows[0])
        gone = {i[2] for u in group for i in json.loads(u["inputs"])}
        first = {json.loads(u["inputs"])[0][2]: u for u in group}
        new = []
        for r in rows:
            if r["dest_path"] in first:
                u = first[r["dest_path"]]
                new.append(r | {"dest_path": u["out_dest"], "bytes": res[u["unit"]]["bytes"], "action": "compressed"})
            elif r["dest_path"] not in gone:
                new.append(r)
        with open(plan, "w", newline="") as fh:
            w = csv.DictWriter(fh, cols, delimiter="\t", lineterminator="\n")
            w.writeheader()
            w.writerows(new)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=("plan", "stage", "run", "upload", "finish", "status"))
    ap.add_argument("plans", type=Path, nargs="*", help="<dataset_id>/plan_final.tsv, for plan")
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--from-chinook", action="store_true", help="inputs are the plan's dest_path files on chinook")
    ap.add_argument("--max-bytes", type=float, default=300e9, help="scratch bytes staged at once")
    ap.add_argument("--tasks", type=int, default=24, help="Slurm array size")
    args = ap.parse_args()
    args.work.mkdir(exist_ok=True)
    units_path = args.work / "units.tsv"

    if args.stage == "plan":
        units = plan_units(args.plans, args.from_chinook)
        with open(units_path, "w", newline="") as fh:
            w = csv.DictWriter(fh, UNIT_COLS, delimiter="\t", lineterminator="\n")
            w.writeheader()
            w.writerows(units)
        print(f"{len(units)} units, {sum(u['in_bytes'] for u in units) / 1e9:.1f} GB",
              dict(Counter(u["kind"] for u in units)))
        return

    units = tsv(units_path)
    done = finished(args.work)
    staged = tasks_by_unit(args.work, "stage")
    uploaded = tasks_by_unit(args.work, "upload")

    if args.stage == "status":
        res = results()
        state = Counter()
        for u in units:
            n = u["unit"]
            state["finished" if n in done else f"upload {uploaded[n]}" if n in uploaded
                  else f"result {res[n]['status']}" if n in res else f"stage {staged[n]}" if n in staged else "planned"] += 1
        for k, v in sorted(state.items()):
            print(f"{v:6d}  {k}")
        for r in res.values():
            if r["status"] != "ok":
                print(f"  {r['unit']}: {r['error']}", file=sys.stderr)
        return

    if args.stage == "stage":
        active = sum(int(u["in_bytes"]) for u in units if u["unit"] in staged and u["unit"] not in done)
        pick = []
        for u in units:
            if u["unit"] in staged and staged[u["unit"]] not in ("FAILED",):
                continue
            if active + int(u["in_bytes"]) > args.max_bytes and (pick or active):
                break
            pick.append(u)
            active += int(u["in_bytes"])
        by_source = defaultdict(list)
        for u in pick:
            for endpoint, path, _ in json.loads(u["inputs"]):
                by_source[endpoint].append((u["unit"], path))
        for endpoint, items in by_source.items():
            pairs = [(path, f"{SCRATCH}/in/{unit}/{P(path).name}") for unit, path in items]
            task = transfer(endpoint, SOCKEYE, pairs, "engcyc gz stage")
            append(args.work / "tasks.tsv", TASK_COLS, {"stage": "stage", "task_id": task,
                                                       "units": " ".join(sorted({u for u, _ in items}))})
            print(f"staged {len(pick)} units, {len(pairs)} files: {task}")
        return

    if args.stage == "run":
        res = results()
        ready = [u for u in units if staged.get(u["unit"]) == "SUCCEEDED" and u["unit"] not in done
                 and res.get(u["unit"], {}).get("status") != "ok"]
        if not ready:
            print("nothing ready")
            return
        name = f"units_{ready[0]['unit']}.tsv"
        lines = "".join("\t".join([u["unit"], u["kind"], P(u["out_dest"]).name,
                                   *(P(i[1]).name for i in json.loads(u["inputs"]))]) + "\n" for u in ready)
        ssh(f"mkdir -p {SCRATCH}/logs && cat > {SCRATCH}/{name}", input=lines)
        subprocess.run(["scp", "-q", str(WORKER), f"sockeye:{SCRATCH}/gz_worker.py"], check=True)
        n = min(args.tasks, len(ready))
        job = ssh(f"cd {SCRATCH} && sbatch --parsable --account=st-shallam-1 --partition=skylake --nodes=1 --ntasks=1 --cpus-per-task=8 "
                  f"--mem=8G --time=24:00:00 --array=0-{n - 1} --output=logs/%A_%a.out "
                  f"--wrap='python3 gz_worker.py {SCRATCH} {name} $SLURM_ARRAY_TASK_ID {n}'").strip()
        print(f"submitted job {job}: {len(ready)} units over {n} tasks")
        return

    if args.stage == "upload":
        res = results()
        pick = [u for u in units if res.get(u["unit"], {}).get("status") == "ok" and u["unit"] not in done
                and uploaded.get(u["unit"]) in (None, "FAILED")]
        if pick:
            pairs = [(f"{SCRATCH}/out/{u['unit']}/{P(u['out_dest']).name}", u["out_dest"]) for u in pick]
            task = transfer(SOCKEYE, CHINOOK, pairs, "engcyc gz upload")
            append(args.work / "tasks.tsv", TASK_COLS, {"stage": "upload", "task_id": task,
                                                       "units": " ".join(u["unit"] for u in pick)})
            print(f"uploading {len(pick)} outputs: {task}")
        return

    if args.stage == "finish":
        res = results()
        pick = [u for u in units if uploaded.get(u["unit"]) == "SUCCEEDED" and u["unit"] not in done]
        chinook = Chinook()
        for folder in sorted({delivery(u["out_dest"]) for u in pick}):
            chinook.list(folder)
        ok = [u for u in pick if chinook.sizes.get(u["out_dest"]) == int(res[u["unit"]]["bytes"])]
        if len(ok) < len(pick):
            print(f"{len(pick) - len(ok)} uploads not at their size on chinook", file=sys.stderr)
        if not ok:
            return
        if args.from_chinook:
            batch = "".join(f'"{i[1]}"\n' for u in ok for i in json.loads(u["inputs"]))
            out = subprocess.run([GLOBUS, "delete", CHINOOK, "--batch", "-", "--label", "engcyc gz originals",
                                  "--format", "json"], input=batch, capture_output=True, text=True, timeout=600)
            if out.returncode:
                raise RuntimeError(out.stderr.strip())
            task = json.loads(out.stdout)["task_id"]
            globus("task", "wait", task, "--timeout", "3600", timeout=3700)
        ssh("cd " + SCRATCH + " && rm -rf " + " ".join(f"in/{u['unit']} out/{u['unit']}" for u in ok))
        rewrite_plans(ok, res)
        for u in ok:
            r = res[u["unit"]]
            append(args.work / "finished.tsv", ["unit", "out_dest", "bytes", "md5", "reads"],
                   {"unit": u["unit"], "out_dest": u["out_dest"], "bytes": r["bytes"], "md5": r["md5"], "reads": r["reads"]})
        print(f"finished {len(ok)} units")


if __name__ == "__main__":
    main()
