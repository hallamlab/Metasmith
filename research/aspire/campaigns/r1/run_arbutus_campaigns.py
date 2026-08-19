import os
import re
import sys
import json
import shutil
import argparse
import subprocess
from pathlib import Path
from datetime import datetime, timezone

ROOT     = Path(__file__).resolve().parent
STAGING  = ROOT / ".campaigns"
ARBUTUS  = Path("/home/tony/agentic_workspace/projects/arbutus-infra/dev/scripts")

HPC_HOST     = os.environ.get("MSM_HPC_HOST", "fir")
HPC_MSM_HOME = Path(os.environ.get("MSM_AGENT_HOME", "/scratch/phyberos/gmcf3495/metasmith"))
HPC_SCRATCH  = os.environ.get("MSM_HPC_SCRATCH", "/scratch/phyberos/gmcf3495")

GTDB_RELEASE = "r232"

CAMPAIGNS = {
    "metabuli": dict(
        dtype="sequences::megahit_assembly",
        prefix_headers=True,
        batch_size=None,
        submit="metabuli-submit.sh",
        extension="fna",
        split_per_sample=True,
    ),
    "gtdbtk": dict(
        dtype="binning_local::quality_bin_fasta",
        prefix_headers=False,
        batch_size=150,
        submit="gtdbtk-submit.sh",
        extension="fna",
        attribute_binner=True,
    ),
}


BINNERS = ("metabat2", "semibin2", "comebin")


def binner_of(attrib_row):
    if not attrib_row:
        return None
    found = {b for b in BINNERS if b in attrib_row.get("upstream", ())}
    return found.pop() if len(found) == 1 else None


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run(cmd, **kw):
    try:
        return subprocess.run(cmd, text=True, capture_output=True, **kw)
    except FileNotFoundError as e:
        return subprocess.CompletedProcess(cmd, 127, "", str(e))


OS_ENV = os.environ.get("MSM_OPENSTACK_ENV", "arbutus")


def _openstack(*args, timeout=300):
    direct = _run(["openstack", *args], timeout=timeout)
    if direct.returncode != 127:
        return direct
    return _run(["conda", "run", "-n", OS_ENV, "openstack", *args], timeout=timeout)


def ssh_out(cmd, timeout=600):
    r = _run(["ssh", "-o", "BatchMode=yes", HPC_HOST, cmd], timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"ssh failed: {cmd}\n{r.stderr}")
    return r.stdout


def find_run_dir(task_key=None):
    if task_key is None:
        keys_file = ROOT / ".cache" / "task_keys.json"
        if not keys_file.exists():
            raise SystemExit("no .cache/task_keys.json — has the DAG been submitted?")
        keys = json.loads(keys_file.read_text())
        task_key = [v for k, v in keys.items() if "r1_metag" in k or "setup" not in k][-1]
    return f"{HPC_MSM_HOME}/runs/{task_key}", task_key


ATTRIB = Path(__file__).resolve().parent / "nxf_attribution.py"


def attribution(run_dir, refresh=True):
    key = Path(run_dir).name
    cache = STAGING / "_mirror" / key / "attribution.tsv"
    if cache.exists() and not refresh:
        text = cache.read_text()
    else:
        r = _run(["scp", "-q", str(ATTRIB), f"{HPC_HOST}:{HPC_SCRATCH}/pw/"], timeout=120)
        if r.returncode != 0:
            raise SystemExit(f"could not stage nxf_attribution.py on {HPC_HOST}:\n{r.stderr}")
        text = ssh_out(f"python3 {HPC_SCRATCH}/pw/nxf_attribution.py {run_dir}")
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(text)

    rows = {}
    for line in text.splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) != 5:
            continue
        product, sample, transform, upstream, task_dir = parts
        rows[product] = dict(sample=sample, transform=transform, task_dir=task_dir,
                             upstream=[u for u in upstream.split(",") if u])
    if not rows:
        raise SystemExit(f"nxf_attribution found no products under {run_dir}")
    return rows


def published_products(run_dir):
    out = {}
    listing = ssh_out(f"find {run_dir}/results -mindepth 2 -maxdepth 2 -type f "
                      f"-printf '%h\\t%f\\n' 2>/dev/null || true")
    for line in listing.splitlines():
        if "\t" not in line:
            continue
        d, name = line.split("\t", 1)
        dirname = re.sub(r"^\d+_", "", Path(d).name)
        if "-" not in dirname:
            continue
        ns, _, rest = dirname.partition("-")
        out.setdefault(f"{ns}::{rest}", {})[name] = f"{d}/{name}"
    return out


def harvest(campaign, task_key=None):
    spec = CAMPAIGNS[campaign]
    run_dir, key = find_run_dir(task_key)

    attrib = attribution(run_dir)
    products = published_products(run_dir)
    found = products.get(spec["dtype"])
    if not found:
        raise SystemExit(
            f"the run published no {spec['dtype']}.\n"
            f"  Present dtypes: {', '.join(sorted(products)) or '(none)'}"
        )

    out = STAGING / campaign / "input"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)

    mapping, unresolved = {}, []
    for name, remote in sorted(found.items()):
        a = attrib.get(name)
        sample = a["sample"] if a else "?"
        if sample in ("?", "*", None):
            unresolved.append((name, sample or "(not in nxf_work)"))
            continue
        stem = Path(name).stem
        key_name = (f"{sample}.{spec['extension']}" if campaign == "metabuli"
                    else f"{sample}__{stem}.{spec['extension']}")
        entry = dict(sample=sample, remote=remote, product=name)
        if spec.get("attribute_binner"):
            entry["binner"] = binner_of(a) or "unattributed"
        mapping[key_name] = entry

    if unresolved:
        print(f"WARNING: {len(unresolved)} product(s) did not resolve to exactly one "
              f"sample — they are NOT in this campaign", file=sys.stderr)
        for p, f in unresolved[:5]:
            print(f"  {p} -> {f}", file=sys.stderr)

    if not mapping:
        raise SystemExit("nothing resolved; refusing to run an empty campaign")

    print(f"{campaign}: {len(mapping)} input(s) from run {key}; pulling to {out}")
    listfile = STAGING / campaign / "_remote_list.txt"
    listfile.write_text("\n".join(v["remote"] for v in mapping.values()) + "\n")
    r = _run(["rsync", "-a", "--info=progress2", f"--files-from={listfile}",
              "--no-relative", f"{HPC_HOST}:/", str(out)], timeout=14400)
    if r.returncode != 0:
        raise SystemExit(f"rsync failed:\n{r.stderr}")

    for name, meta in mapping.items():
        got = out / Path(meta["remote"]).name
        dst = out / name
        if got != dst:
            got.rename(dst)
        if spec["prefix_headers"]:
            _prefix_headers(dst, meta["sample"])

    (STAGING / campaign / "mapping.json").write_text(json.dumps(mapping, indent=2))
    print(f"{campaign}: staged {len(mapping)} file(s), mapping.json written")
    return mapping


def _prefix_headers(path, sample):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(path) as fi, open(tmp, "w") as fo:
        for line in fi:
            if line.startswith(">"):
                body = line[1:].rstrip("\n")
                sid, _, rest = body.partition(" ")
                if not sid.startswith(f"{sample}__"):
                    body = f"{sample}__{sid}" + (f" {rest}" if rest else "")
                fo.write(f">{body}\n")
            else:
                fo.write(line)
    tmp.replace(path)


PUBLISHED = os.environ.get(
    "MSM_PUBLISHED_TREE", "project-rpp/steven_c_gmcf3495/metagenomics")


def _sh(cmd, timeout=14400):
    r = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if r.returncode != 0:
        raise SystemExit(f"failed: {cmd}\n{r.stderr[-2000:]}")
    return r.stdout


def harvest_published(campaign, workdir=None):
    spec = CAMPAIGNS[campaign]
    out = STAGING / campaign / "input"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    mapping = {}

    if campaign == "metabuli":
        remote = f"{PUBLISHED}/assembly/fna"
        _sh(f"rsync -a {HPC_HOST}:{remote}/ {out}/")
        for p in sorted(out.glob("*.fna")):
            sample = p.stem
            mapping[p.name] = dict(sample=sample,
                                   remote=f"{remote}/{p.name}",
                                   product=p.name)
            _prefix_headers(p, sample)

    elif campaign == "gtdbtk":
        remote = f"{PUBLISHED}/binning/quality_bins"
        work = Path(workdir or (STAGING / campaign / "_tars"))
        if work.exists():
            shutil.rmtree(work)
        (work / "tars").mkdir(parents=True)
        (work / "x").mkdir(parents=True)
        _sh(f"rsync -a {HPC_HOST}:{remote}/ {work}/tars/")
        for t in sorted((work / "tars").glob("*.tar")):
            _sh(f"tar xf {t} -C {work}/x")
        for p in sorted((work / "x").rglob("*.fa")):
            parts = p.stem.split(".")
            if len(parts) != 3 or parts[1] not in BINNERS:
                raise SystemExit(f"unexpected quality-bin member name: {p.name}")
            sample, binner, _n = parts
            dst = out / f"{p.stem}.{spec['extension']}"
            if dst.exists():
                raise SystemExit(f"two bins claim {dst.name}")
            shutil.copyfile(p, dst)
            mapping[dst.name] = dict(sample=sample, binner=binner,
                                     remote=f"{remote}/{sample}.tar",
                                     product=p.name)
        shutil.rmtree(work)
    else:
        raise SystemExit(f"no published-tree route for {campaign}")

    if not mapping:
        raise SystemExit("nothing staged; refusing to run an empty campaign")
    (STAGING / campaign / "mapping.json").write_text(json.dumps(mapping, indent=2))
    print(f"{campaign}: staged {len(mapping)} file(s) from the published tree, "
          f"mapping.json written")
    return mapping


def build_metabuli_query():
    src = STAGING / "metabuli" / "input"
    files = sorted(src.glob("*.fna"))
    if not files:
        raise SystemExit(f"no staged assemblies in {src}")
    qdir = STAGING / "metabuli" / "query"
    qdir.mkdir(parents=True, exist_ok=True)
    q = qdir / "gmcf3495_contigs.fna"

    seen, dups, n = set(), [], 0
    with open(q, "w") as fo:
        for f in files:
            with open(f) as fi:
                for line in fi:
                    if line.startswith(">"):
                        sid = line[1:].split(None, 1)[0]
                        n += 1
                        if sid in seen:
                            dups.append(sid)
                        seen.add(sid)
                        if SEP_GUARD not in sid:
                            raise SystemExit(
                                f"{f.name}: header id {sid!r} carries no '{SEP_GUARD}' "
                                f"sample prefix — the batch could not be split apart")
                    fo.write(line)
    if dups:
        print(f"!! {len(dups)} DUPLICATE sequence id(s), e.g. {dups[:5]}", file=sys.stderr)
        raise SystemExit(1)
    print(f"metabuli query: {len(files)} assemblies, {n} contigs, "
          f"{n - len(seen)} duplicate ids -> {q}")
    return q, len(files), n


SEP_GUARD = "__"


def ledger_path(campaign):
    return STAGING / campaign / "ledger.json"


def load_ledger(campaign):
    p = ledger_path(campaign)
    return json.loads(p.read_text()) if p.exists() else dict(campaign=campaign, batches=[])


def save_ledger(campaign, led):
    p = ledger_path(campaign)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(led, indent=2))


def plan_batches(campaign, batch_size=None):
    spec = CAMPAIGNS[campaign]
    src = STAGING / campaign / "input"
    files = sorted(p.name for p in src.glob(f"*.{spec['extension']}"))
    if not files:
        raise SystemExit(f"no staged inputs in {src} — run `harvest {campaign}` first")

    size = batch_size or spec["batch_size"] or len(files)
    led = load_ledger(campaign)

    done = [b for b in led["batches"] if b.get("state") == "done"]
    claimed = {m for b in done for m in b["members"]}

    staged = set(files)
    vanished = sorted(claimed - staged)
    if vanished:
        print(f"  !! {len(vanished)} member(s) of a completed batch are no longer "
              f"staged (e.g. {vanished[0]}); the harvest has drifted")

    remaining = [f for f in files if f not in claimed]
    nxt = max((int(b["id"][1:]) for b in done), default=-1) + 1
    fresh = [dict(id=f"b{nxt + i:03d}", state="pending", members=m,
                  worker=None, started=None, finished=None)
             for i, m in enumerate(remaining[j:j + size]
                                   for j in range(0, len(remaining), size))]

    led["batches"] = done + fresh
    save_ledger(campaign, led)

    covered = claimed | {m for b in fresh for m in b["members"]}
    assert staged <= covered, f"{len(staged - covered)} staged input(s) in no batch"
    print(f"{campaign}: {len(files)} input(s) -> {len(led['batches'])} batch(es) of <= {size}"
          + (f"  ({len(done)} already done covering {len(claimed)}, kept)" if done else ""))
    return led


def run_campaign(campaign, only=None, keep_worker=False):
    spec = CAMPAIGNS[campaign]
    led = load_ledger(campaign)
    if not led["batches"]:
        led = plan_batches(campaign)

    src = STAGING / campaign / "input"
    for batch in led["batches"]:
        if batch["state"] == "done":
            continue
        if only and batch["id"] not in only:
            continue

        bdir = STAGING / campaign / batch["id"]
        bin_dir, out_dir = bdir / "in", bdir / "out"
        if bin_dir.exists():
            shutil.rmtree(bin_dir)
        bin_dir.mkdir(parents=True)
        out_dir.mkdir(parents=True, exist_ok=True)
        for name in batch["members"]:
            os.link(src / name, bin_dir / name)

        worker = f"{campaign}-{batch['id']}-{os.getpid()}"
        batch.update(state="running", worker=worker, started=_now())
        save_ledger(campaign, led)

        env = dict(os.environ,
                   WORKER=worker,
                   GTDB_RELEASE=GTDB_RELEASE,
                   EXTENSION=spec["extension"])
        if keep_worker:
            env["KEEP_WORKER"] = "1"

        print(f"== {campaign}/{batch['id']}: {len(batch['members'])} input(s) -> {out_dir}")
        r = subprocess.run([str(ARBUTUS / spec["submit"]), str(bin_dir), str(out_dir)],
                           env=env, text=True)
        if r.returncode != 0:
            batch.update(state="failed", finished=_now())
            save_ledger(campaign, led)
            print(f"!! {campaign}/{batch['id']} FAILED (rc={r.returncode}); "
                  f"stopping so the failure is diagnosed rather than repeated",
                  file=sys.stderr)
            return 1
        batch.update(state="done", worker=None, finished=_now())
        save_ledger(campaign, led)

    merge(campaign)
    return 0


def merge(campaign):
    led = load_ledger(campaign)
    outs = [STAGING / campaign / b["id"] / "out"
            for b in led["batches"] if b["state"] == "done"]
    if not outs:
        print(f"{campaign}: nothing done yet; nothing to merge")
        return
    merged = STAGING / campaign / "merged"
    merged.mkdir(parents=True, exist_ok=True)

    by_name = {}
    for d in outs:
        per_batch = {}
        for f in sorted(d.rglob("*")):
            if not (f.is_file() and f.suffix in (".tsv", ".txt", ".csv")):
                continue
            prev = per_batch.get(f.name)
            if prev is None:
                per_batch[f.name] = f
                continue
            if prev.read_bytes() != f.read_bytes():
                print(f"  !! {d.parent.name}: {f.name} differs between "
                      f"{prev.relative_to(d)} and {f.relative_to(d)}; "
                      f"keeping the shallower one")
            if len(f.relative_to(d).parts) < len(prev.relative_to(d).parts):
                per_batch[f.name] = f
        for name, f in per_batch.items():
            by_name.setdefault(name, []).append(f)

    for name, parts in sorted(by_name.items()):
        parts = sorted(parts)
        dst = merged / name
        parts = [p for p in parts if p.stat().st_size > 0]
        if not parts:
            print(f"  (all parts empty — skipping {name})")
            continue

        first_lines = []
        for p in parts:
            with open(p) as fi:
                first_lines.append(fi.readline())
        has_header = len(parts) > 1 and len(set(first_lines)) == 1 and first_lines[0] != ""

        with open(dst, "w") as fo:
            for i, p in enumerate(parts):
                with open(p) as fi:
                    lines = fi.readlines()
                if has_header and i:
                    lines = lines[1:]
                fo.writelines(lines)
        print(f"  merged {len(parts)} part(s){' (shared header)' if has_header else ''} -> {dst}")

    _write_bin_index(campaign, merged)
    _split_per_sample(campaign, merged)


def _split_per_sample(campaign, merged):
    if not CAMPAIGNS.get(campaign, {}).get("split_per_sample"):
        return
    import split_metabuli_batch as smb

    cls = sorted(merged.glob("*_classifications.tsv"))
    rep = sorted(merged.glob("*_report.tsv"))
    if not cls or not rep:
        print("  (no batch classifications/report — skipping per-sample split)")
        return
    outdir = merged.parent / "per_sample"
    written = smb.split(cls[0], rep[0], outdir)
    print(f"  split {len(written)} sample(s) -> {outdir}")

    problems = smb.verify_roundtrip(rep[0], outdir)
    if problems:
        print(f"  !! {len(problems)} clade count(s) do not sum back to the batch report:")
        for p in problems[:10]:
            print(f"     {p}")
        raise SystemExit(1)
    print("  per-sample clade counts sum back to the batch report")


def _write_bin_index(campaign, merged):
    if not CAMPAIGNS.get(campaign, {}).get("attribute_binner"):
        return
    src = STAGING / campaign / "mapping.json"
    if not src.exists():
        print("  (no mapping.json — skipping bin_index.tsv)")
        return
    mapping = json.loads(src.read_text())
    dst = merged / "bin_index.tsv"
    unattributed = 0
    with open(dst, "w") as f:
        f.write("bin_id\tsample\tbinner\n")
        for name, meta in sorted(mapping.items()):
            binner = meta.get("binner", "unattributed")
            unattributed += binner == "unattributed"
            f.write(f"{Path(name).stem}\t{meta['sample']}\t{binner}\n")
    note = f"; {unattributed} unattributed" if unattributed else ""
    print(f"  wrote {dst} ({len(mapping)} bins{note})")


def reap(dry_run=False):
    any_stranded = False
    for campaign in CAMPAIGNS:
        led = load_ledger(campaign)
        dirty = False
        for b in led["batches"]:
            if b.get("state") != "running" or not b.get("worker"):
                continue
            any_stranded = True
            w = b["worker"]
            print(f"{'(dry-run) ' if dry_run else ''}tearing down {w} ({campaign}/{b['id']})")
            if dry_run:
                continue
            down = CAMPAIGNS[campaign]["submit"].replace("-submit", "-down")
            subprocess.run([str(ARBUTUS / down), w], text=True)
            b.update(state="pending", worker=None, finished=None, started=None)
            dirty = True
        if dirty:
            save_ledger(campaign, led)
    if not any_stranded:
        print("no workers recorded as running")

    r = _openstack("server", "list", "-f", "value", "-c", "Name")
    if r.returncode == 0:
        loose = [n for n in r.stdout.split()
                 if n.startswith(("metabuli-", "gtdbtk-")) and "ref" not in n]
        if loose:
            print(f"WARNING: servers on the tenancy not in any ledger: {loose}", file=sys.stderr)
        else:
            print("tenancy clean: no unledgered campaign servers")
    else:
        print(f"WARNING: could not list servers, so strays are UNCHECKED "
              f"(rc={r.returncode}): {r.stderr.strip()[:200]}", file=sys.stderr)


def status():
    for campaign in CAMPAIGNS:
        led = load_ledger(campaign)
        if not led["batches"]:
            print(f"{campaign}: not planned")
            continue
        counts = {}
        for b in led["batches"]:
            counts[b["state"]] = counts.get(b["state"], 0) + 1
        n = sum(len(b["members"]) for b in led["batches"])
        print(f"{campaign}: {len(led['batches'])} batch(es), {n} input(s) — {counts}")
        for b in led["batches"]:
            if b["state"] != "done":
                print(f"    {b['id']:5s} {b['state']:8s} n={len(b['members']):4d} "
                      f"worker={b['worker'] or '-'}")


def main():
    p = argparse.ArgumentParser(description="drive the metabuli + GTDB-Tk Arbutus campaigns")
    sub = p.add_subparsers(dest="command", required=True)

    ph = sub.add_parser("harvest", help="pull this campaign's inputs off fir")
    ph.add_argument("campaign", choices=list(CAMPAIGNS))
    ph.add_argument("--task-key", default=None)
    ph.add_argument("--from-published", action="store_true",
                    help="stage from the published tree (sample/binner come from "
                         "filenames) rather than the attribution graph")

    sub.add_parser("query", help="build + gate the single concatenated metabuli query")

    pp = sub.add_parser("plan", help="shard staged inputs into batches")
    pp.add_argument("campaign", choices=list(CAMPAIGNS))
    pp.add_argument("--batch-size", type=int, default=None)

    pr = sub.add_parser("run", help="submit each pending batch, then merge")
    pr.add_argument("campaign", choices=list(CAMPAIGNS))
    pr.add_argument("--only", action="append", default=None, help="batch id(s)")
    pr.add_argument("--keep-worker", action="store_true",
                    help="leave the worker up after the batch (for measuring)")

    pm = sub.add_parser("merge", help="re-merge finished batches")
    pm.add_argument("campaign", choices=list(CAMPAIGNS))

    prp = sub.add_parser("reap", help="tear down workers the ledger still thinks are up")
    prp.add_argument("--dry-run", action="store_true")

    sub.add_parser("status")

    a = p.parse_args()
    if a.command == "harvest":
        if a.from_published:
            harvest_published(a.campaign)
        else:
            harvest(a.campaign, a.task_key)
        return 0
    if a.command == "query":
        build_metabuli_query(); return 0
    if a.command == "plan":
        plan_batches(a.campaign, a.batch_size); return 0
    if a.command == "run":
        return run_campaign(a.campaign, a.only, a.keep_worker)
    if a.command == "merge":
        merge(a.campaign); return 0
    if a.command == "reap":
        reap(a.dry_run); return 0
    status(); return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
