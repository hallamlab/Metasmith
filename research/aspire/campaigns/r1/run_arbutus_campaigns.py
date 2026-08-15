"""run_arbutus_campaigns.py — drive the two off-cluster taxonomy campaigns.

The r1 DAG deliberately leaves two products out, because their reference
databases are too big to keep on fir (metabuli r232 ~744 GB, GTDB-Tk r232
~110 GB). Both run instead as on-demand Arbutus services. This driver is the
connector metasmith does not have: it harvests inputs off fir, batches them,
invokes the service's own `*-submit.sh`, and merges what comes back.

  metabuli  — contig taxonomy over the 34 megahit assemblies
  gtdbtk    — bin taxonomy over every quality bin the aggregator kept

Two campaigns, one machine. The shape is identical and only the input dtype,
the batching rule and the merge differ, so the ledger and batching live here
once rather than twice.

WHY THE LEDGER EXISTS
A campaign is hours of wall clock across several 32-core workers, driven from a
laptop-side process that can be compacted, killed or restarted. Everything that
must survive that is in `<campaign>_ledger.json`: which batches exist, which
have finished, and -- critically -- the name of any worker currently believed to
be up. `metabuli-submit.sh` traps EXIT and tears its own worker down, but a
driver killed hard cannot run that trap, and a stranded worker holds 32 of the
~84 cores free on this tenancy. `reap` reads the ledger and kills what the trap
did not.

WHY BATCHING IS NOT AN OPTIMISATION
Metabuli streams the database past the queries, and with --max-ram below the
database size that is several passes whose cost is per *job*, not per query. All
34 assemblies as one invocation pays the scan once; 34 separate jobs pay it 34
times. Arbutus tops out at 120 GB of RAM, so a 744 GB database is ~7 passes --
which is affordable exactly once. Hence: metabuli is ONE batch.

GTDB-Tk batches for the opposite reason: its ANI pre-screen has a ~10 minute
floor regardless of batch size, and memory scales with the batch, so batches
want to be as large as 120 GB allows. That number is measured by the first
batch, not guessed -- the service's own documentation rests on one and two
genomes, and this is hundreds of novel MAGs.

Usage:
  python run_arbutus_campaigns.py harvest metabuli     # fir -> local staging
  python run_arbutus_campaigns.py harvest gtdbtk
  python run_arbutus_campaigns.py plan gtdbtk --batch-size 150
  python run_arbutus_campaigns.py run metabuli         # submit, poll, merge
  python run_arbutus_campaigns.py run gtdbtk
  python run_arbutus_campaigns.py status
  python run_arbutus_campaigns.py reap                 # kill stranded workers
"""
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
# Scratch, not /tmp: on fir's login node /tmp shadows stdlib module names, so a
# python3 script run from there imports the wrong `types`/`csv` and dies.
HPC_SCRATCH  = os.environ.get("MSM_HPC_SCRATCH", "/scratch/phyberos/gmcf3495")

GTDB_RELEASE = "r232"

# Products to harvest, by campaign. The manifest filename renders `::` as `-`.
CAMPAIGNS = {
    "metabuli": dict(
        dtype="sequences::megahit_assembly",
        # metabuli classify takes exactly ONE query file in --seq-mode 3 (the
        # second positional is the database directory), so the batch is a single
        # concatenated file -- metabuli-run-batch.sh joins them on the worker.
        # One file means one sequence-id namespace, and megahit restarts contig
        # numbering per assembly (k141_1, k141_2, ...), so two samples collide
        # outright and the results cannot be split back apart. Harvest rewrites
        # headers to <sample>__<contig>; metabuli-run-batch.sh checks for
        # collisions and refuses, which is the safety net rather than the
        # mechanism. See RUN_LOG F6.
        prefix_headers=True,
        batch_size=None,          # one batch, always -- see module docstring
        submit="metabuli-submit.sh",
        extension="fna",
        # One job emits ONE report covering all 34 samples. classifications.tsv
        # partitions on the prefix, but report.tsv is an aggregate and has to be
        # recomputed per sample -- see split_metabuli_batch.py.
        split_per_sample=True,
    ),
    "gtdbtk": dict(
        dtype="binning_local::quality_bin_fasta",
        prefix_headers=False,     # each bin is its own genome; ids stay as-is
        batch_size=150,           # provisional; `plan` re-sizes from batch 1
        submit="gtdbtk-submit.sh",
        extension="fna",
        attribute_binner=True,    # see binner_of()
    ),
}


# "Which binner produced this MAG" is the first question anyone reading a
# three-binner run asks, and it is nowhere in the filesystem: the aggregator
# copies each surviving bin to a metasmith-hashed name and does NOT carry its
# `{binner}__{stem}` label into the filename, despite its docstring saying it
# does. These are the transform names to look for upstream of a product.
BINNERS = ("metabat2", "semibin2", "comebin")


def binner_of(attrib_row):
    """The binner a product descends from, or None.

    Reads the upstream transform closure that `nxf_attribution.py` computed from
    the real file graph. `taxonomy::checkm_stats` is the case this exists for:
    three steps under one dtype, all running the same transform, each consuming
    exactly one binner's bins -- so the binner is unique upstream even though it
    is absent from the product's own transform name.

    *** `binning_local::quality_bin_fasta` IS NOT RESOLVABLE THIS WAY. *** The
    aggregator consumes all three binners at once, so all three are upstream of
    every bin it emits and this returns None for every one of them. Those bins
    are byte-identical copies of a per-binner bin fasta, so the honest route is
    to match them by content; that is deliberately not built here, because the
    binning steps have not run yet and a matcher written against no data is a
    guess with a test suite. Until then quality bins publish as "unattributed",
    which is true rather than convenient.
    """
    if not attrib_row:
        return None
    found = {b for b in BINNERS if b in attrib_row.get("upstream", ())}
    return found.pop() if len(found) == 1 else None


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _run(cmd, **kw):
    """subprocess.run, but a missing binary is a return code rather than a raise.

    `reap` is the one command that must never itself fail -- it is what stops a
    stranded 32-core worker from billing all night -- so an absent `openstack`
    has to degrade to a warning, not a traceback that skips the ledger sweep.
    """
    try:
        return subprocess.run(cmd, text=True, capture_output=True, **kw)
    except FileNotFoundError as e:
        return subprocess.CompletedProcess(cmd, 127, "", str(e))


OS_ENV = os.environ.get("MSM_OPENSTACK_ENV", "arbutus")


def _openstack(*args, timeout=300):
    """Run an openstack command, falling back to `conda run -n arbutus`.

    The campaign driver runs in the metasmith env, where `openstack` is not on
    PATH -- so a bare call fails and `reap`, whose entire job is noticing a
    stranded 32-core worker, degrades to a shrug. Trying the conda env
    explicitly means the safety net works from whichever env the driver happens
    to be invoked in, which at 4am is not a thing anyone should have to
    remember.
    """
    direct = _run(["openstack", *args], timeout=timeout)
    if direct.returncode != 127:
        return direct
    return _run(["conda", "run", "-n", OS_ENV, "openstack", *args], timeout=timeout)


def ssh_out(cmd, timeout=600):
    r = _run(["ssh", "-o", "BatchMode=yes", HPC_HOST, cmd], timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(f"ssh failed: {cmd}\n{r.stderr}")
    return r.stdout


# ── harvest ──────────────────────────────────────────────────────────────────
#
# ATTRIBUTION, AND WHY IT NO LONGER GOES THROUGH THE METASMITH API
# Every product this driver ships has to carry the sample it came from, and the
# sample id survives into the run in exactly one place: the read filenames
# (<sample>_R[12].fastq.gz). Everything downstream is content-hashed, so getting
# from a megahit assembly back to "S17" is a lineage question.
#
# This used to ask metasmith's own lineage graph, through DataInstanceLibrary.
# *** THAT GRAPH IS WRONG ON 0.19.1. *** Measured on run dcYCo2Px: of 13
# interleave steps, 0 agreed with what nextflow actually staged. See
# nxf_attribution.py for the evidence and the precise failure -- the short
# version is that `trace.jsonl`'s `consumes` disagrees with the three artifacts
# that agree with each other and with the filesystem, the mis-assignment is per
# input slot so nothing looks malformed, and one product in thirteen resolved to
# exactly one sample and the WRONG one, which the len(found) != 1 guard below
# cannot see.
#
# So attribution now comes from `nxf_attribution.py`, which reads the binding
# out of each task's own `.command.sh` and walks the real file graph in
# nxf_work. Two consequences worth knowing:
#
#   - it is exact, and independently corroborated: all 34 interleaved outputs
#     sit at 0.716-0.875 of their input size, a tight band across libraries
#     spanning 59 kB to 86 GB, where a scrambled assignment would spray;
#   - *** IT NO LONGER NEEDS A CLEAN WORKFLOW EXIT. *** publishDir fills
#     results/<n>_<dtype>/ as each step finishes, and nxf_work is always there,
#     so both inputs exist mid-run. The old route needed CollectResults, which
#     runs only after nextflow exits -- that is why T7 was serialised behind a
#     clean T6 exit, and it no longer is. A killed run proves it: KMQ5eomS has
#     11 populated product directories and an EMPTY manifest.
#
# The results tree also settles a second defect the old path had: product nodes
# carried paths like `out/1-1-1.<hash>-<hash>.fq.gz`, and `<run_dir>/results/out/`
# does not exist. The file is published under `results/<n>_<dtype dashed>/`, so
# the dtype comes from the directory name and the path is real.

def find_run_dir(task_key=None):
    """The run dir of the r1 run. Explicit key wins; else the newest."""
    if task_key is None:
        keys_file = ROOT / ".cache" / "task_keys.json"
        if not keys_file.exists():
            raise SystemExit("no .cache/task_keys.json — has the DAG been submitted?")
        keys = json.loads(keys_file.read_text())
        # the r1 DAG key, not the container-prefetch one
        task_key = [v for k, v in keys.items() if "r1_metag" in k or "setup" not in k][-1]
    return f"{HPC_MSM_HOME}/runs/{task_key}", task_key


ATTRIB = Path(__file__).resolve().parent / "nxf_attribution.py"


def attribution(run_dir, refresh=True):
    """product basename -> {sample, transform, upstream, task_dir}.

    Runs nxf_attribution.py where nxf_work is. It walks thousands of small
    files, so it belongs on the cluster; what comes back is one short line per
    product. Cached per run dir because `plan`, `run` and `publish` all want it.
    """
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
    """dtype -> {basename: remote path}, read off the published results tree.

    publishDir lays results out as `results/<n>_<namespace>-<name>/`, with the
    numeric prefix present only for some steps, so the dtype is the directory
    name minus that prefix with the first dash restored to `::`. Type names
    carry underscores but never dashes, which is what makes the first dash the
    right split point.
    """
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
        # `?` is no read ancestor, `*` is several. Either means the join
        # assumption does not hold for this product, and mislabelled taxonomy is
        # worse than missing taxonomy, so it is dropped rather than guessed at.
        if sample in ("?", "*", None):
            unresolved.append((name, sample or "(not in nxf_work)"))
            continue
        stem = Path(name).stem
        key_name = (f"{sample}.{spec['extension']}" if campaign == "metabuli"
                    else f"{sample}__{stem}.{spec['extension']}")
        entry = dict(sample=sample, remote=remote, product=name)
        # GTDB-Tk keys its results by the input filename stem, so sample comes
        # back for free but binner does not.
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

    # Rename hash-named files to their resolved names, and prefix headers where
    # the campaign needs globally unique sequence ids.
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
    """Rewrite `>k141_3 flag=…` as `>{sample}__k141_3 flag=…`, in place.

    Only the id (up to the first whitespace) is touched; the rest of the
    description is preserved, since assembly stats downstream still read it.
    """
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


# ── harvest, from the published tree ─────────────────────────────────────────
#
# WHY THIS EXISTS ALONGSIDE harvest()
# harvest() reconstructs each product's sample (and binner) by walking the
# nextflow attribution graph inside one run directory. That works for
# assemblies and is exact -- but see binner_of(): quality bins are NOT
# resolvable that way, because the aggregator consumes all three binners at
# once, so every binner is upstream of every bin it emits.
#
# Publication already solved both problems, by naming. In
# `~/project-rpp/steven_c_gmcf3495/metagenomics` an assembly is
# `assembly/fna/<sample>.fna` and a quality bin is a tar member named
# `<sample>.<binner>.<n>.fa`. The sample and the binner are in the filename, so
# there is no lineage question left to answer and no run directory to consult
# -- which also decouples the campaigns from scratch cleanup entirely.
#
# THE EXTENSION MISMATCH IS DELIBERATE, NOT A BUG BEING PAPERED OVER.
# Published bins are `.fa`; both CAMPAIGNS specs declare `fna`, and that string
# is what plan_batches() globs on AND what the submit scripts pass as the
# service's --extension. Staging renames `.fa` -> `.fna` so those two agree,
# rather than leaving a glob that silently matches nothing and yields an empty
# campaign that still exits 0. The stem is untouched, so GTDB-Tk's
# `user_genome` column stays `<sample>.<binner>.<n>` and carries both labels
# into the merged table for free.

PUBLISHED = os.environ.get(
    "MSM_PUBLISHED_TREE", "project-rpp/steven_c_gmcf3495/metagenomics")


def _sh(cmd, timeout=14400):
    r = subprocess.run(cmd, shell=True, text=True, capture_output=True, timeout=timeout)
    if r.returncode != 0:
        raise SystemExit(f"failed: {cmd}\n{r.stderr[-2000:]}")
    return r.stdout


def harvest_published(campaign, workdir=None):
    """Stage a campaign's inputs from the published tree on fir.

    Returns the mapping dict, and writes the same `mapping.json` schema
    harvest() writes so plan/run/merge/split downstream are unchanged.
    """
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
            # BEFORE concatenation, never after: megahit restarts contig
            # numbering per assembly (k141_1, k141_2, ...), so in one query
            # namespace two samples collide outright and the classifications
            # could never be split back apart. See split_metabuli_batch.py.
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
            # <sample>.<binner>.<n>.fa -- the whole point of harvesting here.
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
    """Concatenate the prefixed assemblies to the ONE query file, and gate on ids.

    metabuli classify takes exactly one query file positional in sequence mode
    (the second positional is the database directory). The duplicate check is a
    GATE and not a report: a collision here means two samples share a contig id
    in one namespace, and every downstream per-sample table would be silently
    wrong rather than absent.
    """
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


# ── ledger ───────────────────────────────────────────────────────────────────
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

    # A completed batch is preserved by what it CONTAINS, not by its position.
    # This campaign is meant to be re-planned with a different size -- the pilot
    # batch is deliberately small so peak RSS can be measured, and the rest is
    # sized from what it shows (T7). Keying preservation on the id `b000` made
    # that resize silently lossy: re-planning 1000 bins at 400 after a done
    # batch of 150 kept b000's OLD 150 members while the new slicing assumed
    # b000 held 400, so the 250 in between belonged to no batch at all and the
    # ledger still read as a complete, consistent plan. Nothing downstream would
    # have noticed -- the merged table would simply have been short.
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


# ── run ──────────────────────────────────────────────────────────────────────
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
        # Recorded BEFORE the worker exists: metabuli-up.sh can fail with the
        # reference clone already created, and `reap` needs a name to chase.
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
    """Concatenate per-batch tables into one, keeping a single header."""
    led = load_ledger(campaign)
    outs = [STAGING / campaign / b["id"] / "out"
            for b in led["batches"] if b["state"] == "done"]
    if not outs:
        print(f"{campaign}: nothing done yet; nothing to merge")
        return
    merged = STAGING / campaign / "merged"
    merged.mkdir(parents=True, exist_ok=True)

    # One part per BATCH per table name. A basename repeated at two depths
    # inside one batch is the same table written twice, not two parts:
    # gtdbtk 2.7 emits gtdbtk.bac120.summary.tsv at the batch root AND again
    # under classify/, and rglob bucketed both, so a single-batch smoke test
    # merged "2 part(s)" and every genome appeared twice in the merged table.
    # Across a real campaign that duplicates every bin. Prefer the shallowest
    # path -- the tool's canonical output -- and treat a same-name-different-
    # content pair as something to say out loud rather than silently resolve.
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
        # Drop empty parts BEFORE deciding anything. An empty part is not a
        # hypothetical: GTDB-Tk writes no ar53 summary for a batch with no
        # archaea, and a truncated batch leaves a zero-byte table. Left in the
        # list it breaks the merge in both directions -- it makes the header
        # vote disagree (so every part keeps its header, and the merged table
        # gets a header row in its middle, which reads as a data row whose
        # classification is the literal word "classification"), and if it sorts
        # first it shifts every real part off index 0 (so EVERY header is
        # stripped and the table has none at all). Filtering first makes both
        # impossible rather than making each one a separate guard.
        parts = [p for p in parts if p.stat().st_size > 0]
        if not parts:
            print(f"  (all parts empty — skipping {name})")
            continue

        # Decide once whether these parts share a header line, rather than
        # re-reading the first part per iteration. Identical first lines across
        # every part is the only safe signal: a headerless table whose first
        # data rows happened to match would be a one-row loss, so require ALL
        # parts to agree and require more than one part.
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
    """Turn the batch's single report into per-sample ones.

    Only metabuli needs this. GTDB-Tk's tables are already one row per genome,
    so partitioning them is the merge's own job; metabuli's report is a tree of
    counts summed over every sample in the batch and cannot be partitioned at
    all, only rebuilt. The batching that makes the service affordable is what
    creates the problem, so the fix belongs next to it.
    """
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

    # The batch report's counts are sums over samples, so the parts must add
    # back up. A recomputation that is wrong but plausible is the failure this
    # catches, and it is the only check available without a second tool.
    problems = smb.verify_roundtrip(rep[0], outdir)
    if problems:
        print(f"  !! {len(problems)} clade count(s) do not sum back to the batch report:")
        for p in problems[:10]:
            print(f"     {p}")
        raise SystemExit(1)
    print("  per-sample clade counts sum back to the batch report")


def _write_bin_index(campaign, merged):
    """Emit bin_id -> sample, binner alongside the merged GTDB-Tk tables.

    GTDB-Tk keys every row by `user_genome`, which is the input filename stem --
    `{sample}__{hash}`. Sample is therefore recoverable from the table itself,
    but binner is not recorded anywhere downstream of the aggregator (see
    binner_of()). Writing the join here means the published deliverable can
    answer "which binner produced this MAG" without re-opening the lineage
    graph on a cluster the reader may not have.
    """
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


# ── safety ───────────────────────────────────────────────────────────────────
def reap(dry_run=False):
    """Tear down any worker a ledger still believes is up.

    The submit scripts trap EXIT and clean up after themselves; this exists for
    the case they cannot -- a hard-killed driver, a lost session. It is cheap to
    run and should be run on every wake, because a stranded worker holds 32 of
    roughly 84 free cores and its reference clone eats a 25-volume quota.
    """
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
            # Back to pending, not failed: the batch never got a verdict, so it
            # is unstarted work rather than a result to diagnose. `run` picks it
            # up again on the next pass.
            b.update(state="pending", worker=None, finished=None, started=None)
            dirty = True
        if dirty:
            save_ledger(campaign, led)
    if not any_stranded:
        print("no workers recorded as running")

    # Independent of the ledger: the tenancy itself is the source of truth, and
    # a worker created by a driver that died before writing the ledger would be
    # invisible above.
    r = _openstack("server", "list", "-f", "value", "-c", "Name")
    if r.returncode == 0:
        # `ref` excludes the two reference builders; worker-0N and devbox-01 are
        # pre-existing lab infrastructure and are not ours to reap, so the match
        # is deliberately narrow -- a false negative costs 32 idle cores, a false
        # positive kills someone else's machine.
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
