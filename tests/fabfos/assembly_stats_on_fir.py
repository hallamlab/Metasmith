#!/usr/bin/env python3
"""Coverage of the recovered fosmid inserts, one job per SCADC pool, on fir.

    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py
    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py --offline
    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py --preflight
    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py --run --pools pool03_CAATCGAC
    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py --run
    PYTHONPATH=src python tests/fabfos/assembly_stats_on_fir.py --summarize <dir>/results

WHY THIS EXISTS
---------------
`data/fabfos/runs/scadc_fosmids/sequences/inserts` holds 183 putative fosmid inserts recovered from 35 pools,
but nothing states how much of each pool's sequencing actually lands on that set.
The recovery chain calls junctions from positive vector evidence and never looks
at depth, so coverage has never been measured here at all.

`tests/README.md` and `test_fosmids_workflow.py` already describe the lane this
driver executes -- the insert FASTA standing in as the assembly, `assembly_stats`
grouped on `read_metadata` so one FASTA against N read sets resolves to N jobs --
but it has only ever been compile-checked. `assembly_stats` has never been run in
this project.

THE FAN-OUT IS THE WHOLE DESIGN
-------------------------------
One `fabfos::putative_inserts` item is added with ALL 35 `read_metadata` items as
its parents. That is the aggregate-then-distribute lineage Orchestrator.groovy's
`group()` is explicitly written for: it filters candidate combinations by set
INTERSECTION rather than equality, so an item carrying every pool's hash pairs
with each pool's own items. One insert file therefore reaches all 35 jobs.

It is verified before anything is deployed, via `len(step.group_by_instances)` --
the per-step task count read straight off the resolved plan. A collapse to a
single job plans, stages and runs without complaint; it just measures one pool.

THE PHANTOM ASSEMBLY, AND WHY THE PLAN IS CHECKED RATHER THAN MASKED
---------------------------------------------------------------------
`assembly_stats` consumes `sequences::assembly`, and megahit and spades produce
it. `tests/README.md` records the planner taking that path and reporting coverage
of an assembly it built itself rather than of the inserts.
`test_fosmids_workflow.py` heads it off by pinning the stats target to the
inserts TARGET; that is not available here, because the inserts are a GIVEN and
`TargetBuilder.Add` accepts only other targets as parents.

Narrowing the domain with `TransformInstanceLibrary.AsView` was the obvious
substitute and does not work: the view supports planning but not staging --
`WorkflowTask.Pack` calls `GetKey()` on every transform library and the view
class defines neither `GetKey` nor `PrepTransfer`.

The whole domain is offered instead, and the resolved plan is ASSERTED to contain
exactly `seqkit_reads` and `assembly_stats` before anything is deployed. That is
the real guard either way -- a mask makes the wrong plan impossible to express,
an assertion makes it impossible to run, and only the second one also catches the
planner reaching a wrong shape some other way. In practice the assemblers do not
appear: nothing here demands an assembly except `assembly_stats`, and the given
insert FASTA satisfies that at zero cost.

WHERE THE DATA LIVES
--------------------
The reads are the 32 GB of host-filtered pair-aware interleaved FASTQ already on
fir from the assembly run. They are declared at their absolute fir paths, so
metasmith binds them in place and never uploads them. Only the small things
travel with the task: the 35 read_metadata JSONs (written via AddValue, a
relative path) and the 5.9 MB insert FASTA (copied into the library, likewise
relative).

Each metadata file is named `read_metadata_<pool>.json` on purpose. Staged
filenames are content hashes, so a retrieved output says nothing about its pool,
and the run index cannot say either -- every step-2 output inherits all 35 read
sets as parents through the shared reference. That name in the producing task's
`.command.sh` is the only one-to-one link back, which is what
`--attribute-from-run` reads while the run directory still exists.

THE DEV OVERLAY IS TWO ARTIFACTS, AND ONLY ONE OF THEM IS OBVIOUS
------------------------------------------------------------------
The agent home carries `dev/metasmith`, a copy of the engine that is bound OVER
the one inside the container so the executing engine matches the planner. It also
carries `dev/metasmith.tar`, and THAT is what compute nodes actually use:
`RenderBootstrap` stages the tarball to node-local `/tmp/msm_devstage_$USER/`,
keyed by the tarball's own `stat -c %Y-%s`. The directory is what the LOGIN node
binds; the tarball is what every slurm task binds.

Nothing in the engine keeps the two in step -- both are hand-maintained -- so
updating only the directory produces a run that stages cleanly on the login node
and then fails identically in every task, against engine code weeks old. That is
exactly how this driver's first attempt failed. After moving the engine pin:

    rsync -a --exclude=__pycache__ src/metasmith/src/metasmith/ \\
        fir:<agent_home>/dev/metasmith/
    ssh fir 'cd <agent_home>/dev && tar -cf metasmith.tar --exclude=__pycache__ metasmith'

The stage key is derived from the tarball's mtime and size, so rebuilding it is
also what invalidates the stale node-local copies.

ONE SSH SESSION, NEVER A RETRY LOOP
------------------------------------
fir's ssh config sets ControlMaster no behind a ProxyCommand guard; a loop around
the connect is a Duo push per iteration and a prior run in this project was
halted by an account lockout that way. On failure this exits with a message and
the human connects once by hand.
"""
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from collections import Counter

import yaml
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

from fabfos.pipelines.common import resolve_library_root  # noqa: E402

from metasmith.python_api import (  # noqa: E402
    Agent, DataInstanceLibrary, DataTypeLibrary, Duration, Resources, Runtime,
    Size, SshSource, TargetBuilder, TransformInstanceLibrary,
)

LIB = resolve_library_root()
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

INSERT_DIR = REPO / "data" / "fabfos" / "runs" / "scadc_fosmids" / "sequences" / "inserts"
# The shipped set, and the reference actually mapped against, are allowed to
# differ: the reference may carry the pCC1fos backbone as an extra record so the
# ~15% of every pool that lands on the vector is counted rather than lost. Both
# are rebound from `--reference` / `--membership` in main(), together -- the
# membership table is where the expected pool set comes from, so a reference from
# one insert set checked against another's pools measures the wrong thing.
INSERTS = INSERT_DIR / "inserts.fna"
MEMBERSHIP = INSERT_DIR / "insert_metadata" / "membership.csv"

# The plan must contain these and nothing else. An assembler in the resolved
# plan means coverage of a phantom assembly rather than of the inserts -- see
# this module's docstring.
EXPECTED_TRANSFORMS = {"assembly_stats", "seqkit_reads"}

SETUP_COMMANDS = ["module load apptainer"]
AGENT_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.19.0-fabfos"

# NOT `host_filtered_short_reads`, which is what these files actually are.
#
# `assembly_stats` and `seqkit_reads` both require the generic `sequences::reads`,
# and `reads` pins `qc: none` as a PROPERTY. Every clean_* type overrides that
# with `qc: filtered`, and metasmith matches by property superset -- so a
# differing value is a mismatch, not a refinement. The consequence is that
# `sequences::reads` is satisfiable only by raw reads: no host-filtered or
# otherwise cleaned read set can reach either transform. `test_fosmids_workflow`
# does not hit this because it supplies raw `short_reads_pe` and lets
# `background_filter` run inside the plan.
#
# Declaring these as `short_reads_pe` is therefore a deliberate, driver-local
# understatement: interleaved short reads, which they are, with the type silent
# about the host depletion, which it cannot express here. Nothing in this two-
# transform plan branches on `qc`. What it costs is stated in the results:
# `fraction_reads_mapped` is over host-DEPLETED reads and is not comparable to a
# fraction over raw reads. The alternative -- relaxing `reads` or the two
# requirements in the shared library -- changes types every other plan resolves
# against, for a run that needs no such change.
READS_TYPE = "sequences::short_reads_pe"
READS_SUFFIX = ".host_filtered.fq.gz"
INSERTS_TYPE = "fabfos::putative_inserts"

# Tool images assembly_stats + seqkit_reads reach for. --preflight checks each
# one is already in fir's image store: a compute node has no outbound network,
# so an image that is not there when the task starts cannot be pulled and the
# task dies hours into a queue rather than seconds into a check.
TOOL_ENVS = ["minimap2.env", "samtools.env", "bedtools.env", "seqkit.env"]

# `assembly_stats` declares 4 cpus / 64 GB / 12 h, sized for mapping a read set
# onto a metagenome assembly. The reference here is 5.9 MB of fosmid inserts, so
# those numbers are queue-hostile for no gain. --stock-resources falls back to
# what the library declares.
TRIMMED_RESOURCES = {
    "assembly_stats": Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=3)),
    "seqkit_reads": Resources(cpus=4, memory=Size.GB(8), duration=Duration(hours=1)),
}


def ssh_once(host: str, command: str) -> str:
    """Run one non-interactive command on the host. Never called in a loop."""
    r = subprocess.run(["ssh", "-o", "BatchMode=yes", host, command],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise SystemExit(
            f"ssh to {host} failed ({r.returncode}):\n{r.stderr.strip()[-2000:]}\n"
            f"Connect once by hand (`ssh {host}`), leave it open, and re-run. "
            f"Do NOT retry in a loop -- that is what causes an account lockout."
        )
    return r.stdout


def pools_from_inserts() -> set[str]:
    """The pools the insert set was actually built from, per the data.

    Every id in membership.csv is `pool:assembler:contig[:start-end]`, so the
    pool set is stated by the recovery output rather than inferred from whatever
    happens to be sitting in a directory on fir.
    """
    pools: set[str] = set()
    with open(MEMBERSHIP) as f:
        for row in csv.DictReader(f):
            for key in ("contig", "centroid"):
                pools.add(row[key].split(":")[0])
    if not pools:
        raise SystemExit(f"no pools parsed from {MEMBERSHIP}")
    return pools


def discover_pools(host: str, reads_dir: str) -> dict[str, str]:
    """pool_barcode -> absolute path of its interleaved reads, from the host."""
    out = ssh_once(host, f"ls -1 {reads_dir}/*{READS_SUFFIX}")
    pools: dict[str, str] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        name = Path(line).name
        pools[name[: -len(READS_SUFFIX)]] = line
    if not pools:
        raise SystemExit(f"no {READS_SUFFIX} files under {host}:{reads_dir}")
    return pools


def build_inputs(staging: Path, pools: dict[str, str]) -> DataInstanceLibrary:
    """read_metadata + reads per pool, and the insert FASTA once, parented to all.

    The insert item's parent set is every pool's metadata. That is what makes one
    file fan out to N jobs; see the docstring on the orchestrator's intersection
    filter. Parenting it to a single pool would run one job; parenting it to none
    would leave `assembly_stats`' `asm` requirement (pinned to `meta`) unmatched.
    """
    xgdb = staging / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    for ns, yml in (("sequences", "sequences.yml"), ("fabfos", "fabfos.yml")):
        inputs.AddTypeLibrary(namespace=ns,
                              lib=DataTypeLibrary.Load(LIB / "data_types" / yml))

    metas = []
    for pool in sorted(pools):
        # Interleaved throughout: background_filter emitted pair-aware
        # interleaved FASTQ, which is what "paired" selects downstream.
        meta = inputs.AddValue(
            name=f"read_metadata_{pool}.json",
            value={"parity": "paired", "length_class": "short"},
            dtype="sequences::read_metadata",
        )
        metas.append(meta)
        # Absolute -> referenced in place on fir. Relative -> staged.
        inputs.AddItem(pools[pool], READS_TYPE, parents={meta})

    # Copied in, so it is a RELATIVE member of the library and travels with the
    # task. 5.4 MB; the reads it is mapped against stay where they are.
    shutil.copy(INSERTS, xgdb / INSERTS.name)
    inputs.AddItem(INSERTS.name, INSERTS_TYPE, parents=set(metas))
    inputs.Save()
    return inputs


def check_plan(task, n_pools: int) -> list[str]:
    """Everything that must hold before a single sbatch is issued."""
    problems: list[str] = []

    names = {Path(s.transform._path).stem for s in task.plan.steps}
    if names != EXPECTED_TRANSFORMS:
        problems.append(
            f"plan uses {sorted(names)}, expected exactly "
            f"{sorted(EXPECTED_TRANSFORMS)}. An assembler in this list means "
            f"coverage of a phantom assembly, not of the inserts."
        )

    given = Counter(g.dtype_name for g in task.plan.given)
    for dtype, want in ((READS_TYPE, n_pools),
                        ("sequences::read_metadata", n_pools),
                        (INSERTS_TYPE, 1)):
        if given.get(dtype, 0) != want:
            problems.append(
                f"plan carries {given.get(dtype, 0)} x [{dtype}], expected {want}")

    # The decisive check: task count per step, read off the resolved plan. The
    # plan has two steps whether it runs 1 pool or 35, so step count says
    # nothing -- this does.
    for step in task.plan.steps:
        stem = Path(step.transform._path).stem
        n = len(step.group_by_instances)
        if n != n_pools:
            problems.append(
                f"step [{stem}] would run {n} task(s) for {n_pools} pools -- "
                f"the fan-out collapsed")
    return problems


def _sif_name(uri: str) -> str:
    """docker://staphb/samtools:1.23 -> docker..staphb_samtools..1.23.sif"""
    body = uri.split("://", 1)[1]
    repo, _, tag = body.rpartition(":")
    return f"docker..{repo.replace('/', '_')}..{tag}.sif"


def preflight(host: str, agent_home: str, container: str) -> int:
    """One ssh round-trip checking the three things this run needs and no
    previous run on fir has exercised.

    Every one of these is a mid-run failure otherwise: a compute node has no
    outbound network, so a tool image that is not already in the store cannot be
    pulled when the task reaches for it, and `pigz` runs inside the AGENT
    container rather than a tool container, so no env declaration covers it.
    """
    sif_dir = "${APPTAINER_CACHEDIR:-$HOME/.apptainer/cache}"
    uris = {}
    for e in TOOL_ENVS:
        content = (LIB / "resources" / "env" / e).read_text()
        for line in content.splitlines():
            if line.startswith("container:"):
                uris[e] = line.split(":", 1)[1].strip()
    wanted = {e: _sif_name(u) for e, u in uris.items()}

    checks = "; ".join(
        f'[ -f {sif_dir}/{s} ] && echo "OK   {e} -> {s}" || echo "MISSING {e} -> {s}"'
        for e, s in wanted.items())
    agent_sif = _sif_name(container)
    cmd = "; ".join([
        f'echo "--- agent home ---"',
        f'[ -d {agent_home}/dev/metasmith ] && echo "OK   dev/metasmith bind present" '
        f'|| echo "MISSING dev/metasmith -- Deploy() binds it but does NOT create it"',
        f'echo "--- tool images in {sif_dir} ---"',
        checks,
        f'echo "--- pigz inside the agent image ---"',
        f'module load apptainer >/dev/null 2>&1; '
        f'apptainer exec {sif_dir}/{agent_sif} bash -c '
        f'"command -v pigz >/dev/null && echo \'OK   pigz\' || echo \'MISSING pigz '
        f'-- assembly_stats compresses per-bp coverage with it via LocalShell\'" 2>/dev/null',
    ])
    out = ssh_once(host, cmd)
    print(out)
    bad = [ln for ln in out.splitlines() if ln.startswith("MISSING")]
    if bad:
        print(f"\n{len(bad)} preflight check(s) failed.", file=sys.stderr)
        return 3
    return 0


def render_dag(dest: Path, task) -> None:
    """Draw the resolved plan, and never let that stop the run.

    `RenderDAG` needs the `graphviz` python package AND a `dot` binary; neither
    is in this repo's envs. The DAG is a picture of a plan that `check_plan` has
    already accepted, so a missing renderer is a note, not a failure -- a
    35-job run should not be blocked on a diagram.
    """
    try:
        task.plan.RenderDAG(dest, blacklist_namespaces={"lib", "env"})
        print(f"DAG -> {dest.with_suffix('.svg')}", flush=True)
    except Exception as e:
        print(f"DAG not rendered ({type(e).__name__}: {e}); plan checks below "
              f"are the real gate", flush=True)


def check_tasks(host: str, agent_home: str, task_key: str) -> int:
    """Count FAILED rows in the run's nxf_tasks.csv. Returns that count.

    This, not the log tail, is the reliable "completed is not succeeded" check.
    `slurm.nf` sets errorStrategy='ignore' once a process exhausts its retries,
    so a task that died on every attempt leaves the workflow green with its
    outputs simply absent -- and the phrase that says so may be well above the
    tail the waiter returns. The smoke run for this driver failed exactly that
    way: two instant seqkit_reads failures, a "completed" status, zero outputs.
    """
    csv_glob = f"{agent_home}/runs/{task_key}/_metasmith/logs.*/nxf_tasks.csv"
    out = ssh_once(host, f"cat {csv_glob} 2>/dev/null | sort -u")
    rows = [ln for ln in out.splitlines() if ln and not ln.startswith("task_id,")]
    failed = [ln for ln in rows if ",FAILED," in ln]
    print(f"    nextflow tasks: {len(rows)} recorded, {len(failed)} FAILED")
    for ln in failed:
        print(f"      {ln}", file=sys.stderr)
    return len(failed)


POOL_MAP = "pool_map_from_run.tsv"


def _output_token(path: str | Path) -> str:
    """`2_alignment-bam/1-1-1.hkZW85eMnzy8a8bR-c47DY4W4.bam` -> the middle token.

    Names are `<indices>.<lineage token>-<instance key>.<ext>`; the token is what
    the producing task's work directory carries, and so is the join key back to it.
    """
    return Path(path).name.split(".")[1].split("-")[0]


def attribute_from_run(results: Path, host: str, run_dir: str) -> int:
    """Write `pool_map_from_run.tsv`: output token -> pool, from the work directory.

    THE RUN INDEX CANNOT DO THIS, and the reason is the fan-out itself. Every
    step-2 output's recorded parents are the flattened union of its ancestry, and
    the reference FASTA is parented to all 35 read sets so that one file reaches
    all 35 jobs -- so every output names all 35 read sets as parents and none of
    them singly. It is unambiguous only in a one-pool run, which is exactly the
    case that hides the problem.

    What IS one-to-one is the task that produced the output. Each task's
    `.command.sh` stages `read_metadata_<pool>.json`, named for its pool precisely
    so this join exists. One ssh call looks each token up in `nxf_work` and reads
    the name back. It has to happen while the run directory still exists; after
    that the map is part of the results and `--summarize` needs no host.
    """
    tokens = sorted({_output_token(p) for p in results.rglob("*")
                     if p.is_file() and "." in p.name and p.name.count(".") > 1
                     and p.name.startswith("1-")})
    if not tokens:
        raise SystemExit(f"no attributable product filenames under {results}")

    # The pool barcode is grepped out of `.command.sh` rather than a specific
    # filename, because the two steps name it differently: `assembly_stats`
    # stages `read_metadata_<pool>.json`, `seqkit_reads` never sees that item and
    # stages only the reads, whose own name carries the barcode. One pattern
    # covers both, and nothing else staged into either task carries a pool token.
    probe = "; ".join(
        f'for d in {run_dir}/nxf_work/*/*/; do ls "$d" 2>/dev/null | grep -q {t} '
        f'&& echo -e "{t}\\t$(grep -hoE \'pool[0-9]+_[A-Z]+\' '
        f'"$d/.command.sh" 2>/dev/null | head -1)"; done'
        for t in tokens)
    out = ssh_once(host, probe + "; exit 0")

    found: dict[str, str] = {}
    for line in out.splitlines():
        if "\t" not in line:
            continue
        token, meta = line.split("\t", 1)
        meta = meta.strip()
        if meta:
            found[token] = meta
    missing = set(tokens) - set(found)
    if missing:
        raise SystemExit(f"could not resolve {len(missing)} token(s) in {run_dir}: "
                         f"{sorted(missing)[:5]}")
    with open(results / POOL_MAP, "w") as f:
        f.write("output_token\tpool\n")
        for t in sorted(found):
            f.write(f"{t}\t{found[t]}\n")
    print(f"attributed {len(found)} output token(s) -> {results/POOL_MAP}")
    return 0


def attribute(results: Path) -> dict[Path, str]:
    """retrieved product file -> pool, from `pool_map_from_run.tsv`.

    metasmith names an output by its lineage hash, so a retrieved file says
    nothing about its pool on its own. `attribute_from_run` above is where the
    name comes from, and why it cannot come from the run index.

    It REFUSES rather than guessing when an output is not in the map. A mis-named
    coverage track is worse than a missing one.
    """
    m = results / POOL_MAP
    if not m.exists():
        raise SystemExit(
            f"{m} is missing. Run --attribute-from-run <results> --run-dir <host "
            f"run dir> while the run directory still exists on the host.")
    by_token = dict(line.split("\t") for line in
                    m.read_text().splitlines()[1:] if "\t" in line)

    out: dict[Path, str] = {}
    unresolved: list[str] = []
    for p in sorted(results.rglob("*")):
        if not p.is_file() or p.name.count(".") < 2 or not p.name.startswith("1-"):
            continue
        rel = p.relative_to(results)
        pool = by_token.get(_output_token(p))
        if pool is None:
            unresolved.append(str(rel))
        else:
            out[rel] = pool
    if unresolved:
        raise SystemExit(f"{len(unresolved)} output(s) absent from {m}, "
                         f"e.g. {unresolved[0]}")
    return out


def summarize(results: Path, out_dir: Path) -> int:
    """Write the per-pool summary and the insert x pool coverage matrix."""
    pool_of = attribute(results)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "pool_map.tsv", "w") as f:
        f.write("product_path\tpool\n")
        for p in sorted(pool_of):
            f.write(f"{p}\t{pool_of[p]}\n")

    def _load(pred):
        got = {}
        for p, pool in pool_of.items():
            if not pred(p):
                continue
            local = results / p
            if not local.exists():       # not retrieved (BAM, per-bp coverage)
                continue
            got[pool] = local
        return got

    stats = _load(lambda p: "assembly_stats" in str(p))
    qc = _load(lambda p: "read_qc_stats" in str(p))
    cov = _load(lambda p: "per_contig_coverage" in str(p))

    # Set-level numbers are the INSERT SET's and identical in all 35 JSONs, so
    # they are stated once rather than repeated down a column.
    shared = {}
    rows = []
    for pool in sorted(stats):
        s = json.loads(stats[pool].read_text())
        q = json.loads(qc[pool].read_text()) if pool in qc else {}
        here = {k: s[k] for k in ("length", "N50", "GC", "number_of_contigs")}
        # Every job measured the SAME insert FASTA, so these four must agree
        # across all 35 JSONs. If they do not, some job was handed a different
        # assembly and the whole comparison is between different references.
        if shared and here != shared:
            raise SystemExit(
                f"insert set differs between pools: {shared} vs {here} ({pool})")
        shared = here
        raw = s.get("_raw_mapping", {})
        rows.append(dict(
            pool=pool,
            reads=q.get("reads"),
            bases=q.get("bases"),
            mean_quality=q.get("mean_quality"),
            total_alignment_records=raw.get(
                "total (QC-passed reads + QC-failed reads)"),
            mapped=raw.get("mapped"),
            fraction_reads_mapped=s["fraction_reads_mapped"],
        ))

    cols = ["pool", "reads", "bases", "mean_quality",
            "total_alignment_records", "mapped", "fraction_reads_mapped"]
    with open(out_dir / "pool_summary.tsv", "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in rows:
            f.write("\t".join("" if r[c] is None else str(r[c]) for c in cols) + "\n")

    with open(out_dir / "insert_set.json", "w") as f:
        json.dump(shared, f, indent=2)

    # insert x pool fold coverage. The per-contig TSV lists EVERY insert, with a
    # zero row for the ones this pool does not cover, so an absent insert is a
    # real zero here and not missing data.
    matrix: dict[str, dict[str, float]] = {}
    lengths: dict[str, int] = {}
    for pool in sorted(cov):
        with open(cov[pool]) as f:
            for row in csv.DictReader(f, delimiter="\t"):
                contig = row["contig"]
                matrix.setdefault(contig, {})[pool] = float(row["fold_coverage"])
                lengths[contig] = int(row["contig_length"])
    pools = sorted(cov)
    with open(out_dir / "insert_coverage_matrix.tsv", "w") as f:
        f.write("\t".join(["insert_id", "length"] + pools) + "\n")
        for contig in sorted(matrix):
            vals = [f"{matrix[contig].get(p, 0.0):.4f}" for p in pools]
            f.write("\t".join([contig, str(lengths[contig])] + vals) + "\n")

    print(f"=== wrote to {out_dir} ===")
    print(f"  pool_map.tsv              {len(pool_of)} products named")
    print(f"  pool_summary.tsv          {len(rows)} pools")
    print(f"  insert_coverage_matrix.tsv {len(matrix)} inserts x {len(pools)} pools")
    print(f"  insert_set.json           {shared}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="fir")
    ap.add_argument("--reads-dir",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "host_filtered_reads_pairaware_20260724")
    ap.add_argument("--agent-home",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "scadc_multiassembly_iso",
                    help="reused deliberately: it carries a dev/metasmith tree "
                         "that the msm wrapper binds OVER the engine in the "
                         "container. Deploy() binds that tree but does not "
                         "create it, so a fresh home silently drops it.")
    ap.add_argument("--container", default=AGENT_CONTAINER)
    ap.add_argument("--reference", type=Path, default=None,
                    help="FASTA to map against, instead of the shipped "
                         "inserts.fna. Use the vector-inclusive reference to get "
                         "a mapped fraction that accounts for the whole library.")
    ap.add_argument("--membership", type=Path, default=None,
                    help="membership.csv naming the pools --reference was built "
                         "from; required with --reference unless the shipped one "
                         "still applies.")
    ap.add_argument("--slurm-account", default="rrg-shallam-ab_cpu",
                    help="charged on every sbatch. slurm.nf ships the literal "
                         "placeholder '<slurm_account>', which sbatch rejects.")
    ap.add_argument("--pools", nargs="*", default=None,
                    help="pool barcodes to measure; default is all of them. "
                         "Use one pool for the smoke run.")
    ap.add_argument("--run", action="store_true",
                    help="deploy, stage and execute. Default is plan-only: "
                         "resolve, check, render the DAG, touch nothing else.")
    ap.add_argument("--offline", action="store_true",
                    help="fabricate the read paths instead of listing them on "
                         "the host, so the plan shape can be checked with no "
                         "ssh at all. Implies plan-only.")
    ap.add_argument("--preflight", action="store_true",
                    help="report the agent home, apptainer cache and tool envs, "
                         "then exit")
    ap.add_argument("--attribute-from-run", metavar="RESULTS_DIR",
                    help="write pool_map_from_run.tsv by looking each output "
                         "token up in --run-dir on the host. Must happen before "
                         "the run directory is cleaned.")
    ap.add_argument("--run-dir", help="host-side run directory, for "
                                      "--attribute-from-run")
    ap.add_argument("--summarize", metavar="RESULTS_DIR",
                    help="name every product in an already-retrieved results "
                         "directory and write the summary tables beside it, "
                         "then exit. No host contact.")
    ap.add_argument("--no-deploy", action="store_true")
    ap.add_argument("--stock-resources", action="store_true",
                    help="use the transforms' own declared resources instead of "
                         "the trim this driver applies")
    ap.add_argument("--timeout-s", type=float, default=12 * 3600)
    ap.add_argument("--poll-s", type=float, default=60.0)
    ap.add_argument("--out", default=None,
                    help="local directory to retrieve results into; default is "
                         "under data/scratch/resolve/, named for the run.")
    a = ap.parse_args()

    if a.preflight:
        return preflight(a.host, a.agent_home, a.container)
    if a.attribute_from_run:
        if not a.run_dir:
            raise SystemExit("--attribute-from-run needs --run-dir")
        return attribute_from_run(Path(a.attribute_from_run).resolve(),
                                  a.host, a.run_dir)
    if a.summarize:
        res = Path(a.summarize).resolve()
        return summarize(res, res.parent)

    plan_only = not a.run or a.offline

    # Rebound before anything reads them. They travel together on purpose: see
    # where they are defined.
    global INSERTS, MEMBERSHIP
    if a.reference:
        INSERTS = a.reference.resolve()
        MEMBERSHIP = (a.membership or
                      INSERTS.parent / "insert_metadata" / "membership.csv").resolve()
        print(f"=== reference {INSERTS}\n=== membership {MEMBERSHIP}", flush=True)
    elif a.membership:
        raise SystemExit("--membership without --reference: the two must agree")
    if not MEMBERSHIP.exists():
        raise SystemExit(f"{MEMBERSHIP} does not exist")

    if not INSERTS.exists():
        raise SystemExit(
            f"{INSERTS} is not checked out. "
            f"`mamba run -n dvc dvc checkout data/fabfos/runs/scadc_fosmids/sequences/inserts.dvc`")

    expected = pools_from_inserts()
    print(f"=== {len(expected)} pools named by {MEMBERSHIP.name} ===", flush=True)

    if a.offline:
        available = {p: f"{a.reads_dir}/{p}{READS_SUFFIX}" for p in sorted(expected)}
        print("    --offline: read paths fabricated, host not contacted")
    else:
        print(f"=== listing {a.host}:{a.reads_dir} ===", flush=True)
        available = discover_pools(a.host, a.reads_dir)
        # A difference either way is an error, not a silent subset: a pool the
        # inserts came from but whose reads are gone would leave that pool
        # unmeasured with nothing in the output saying so.
        only_inserts = expected - set(available)
        only_host = set(available) - expected
        if only_inserts or only_host:
            print(f"\nPOOL SET MISMATCH.\n"
                  f"  in the inserts but not on {a.host}: {sorted(only_inserts)}\n"
                  f"  on {a.host} but not in the inserts: {sorted(only_host)}",
                  file=sys.stderr)
            return 3

    if a.pools:
        missing = [p for p in a.pools if p not in available]
        if missing:
            raise SystemExit(f"requested pool(s) not present: {missing}\n"
                             f"available: {sorted(available)}")
        pools = {p: available[p] for p in a.pools}
    else:
        pools = available
    print(f"    {len(pools)} of {len(available)} pools", flush=True)

    ts = int(time.time())
    staging = REPO / "data" / "fabfos" / "scratch" / "resolve" / f"insert_stats_{ts}"
    staging.mkdir(parents=True, exist_ok=True)

    inputs = build_inputs(staging, pools)

    resources = [DataInstanceLibrary.Load(LIB / f"resources/{n}")
                 for n in ("env", "lib")]
    transforms = [TransformInstanceLibrary.Load(LIB / "transforms/assembly")]

    home = SshSource(host=a.host, path=a.agent_home).AsSource()
    agent = Agent(home=home, runtime=Runtime.APPTAINER,
                  container=a.container, setup_commands=SETUP_COMMANDS)

    print("=== planning ===", flush=True)
    targets = TargetBuilder()
    # Unpinned -- a given cannot be a target's parent. check_plan() below is
    # what guarantees the assembly being measured is the given insert FASTA and
    # not one the planner decided to build.
    targets.Add("sequences::assembly_stats")

    # The WHOLE library, NOT `inputs.AsSamples(...)`. AsSamples caches each
    # sample's descendant set keyed on that sample's ANCESTOR set; a
    # read_metadata item has no ancestors, so all 35 share the empty key and
    # every sample view gets the first pool's reads. The plan still resolves and
    # still runs -- it just measures pool01 thirty-five times.
    task = agent.GenerateWorkflow(
        samples=[inputs],
        resources=resources,
        transforms=transforms,
        targets=targets,
    )
    if not task.ok:
        print("\nPLAN DID NOT RESOLVE. Planner hints:", file=sys.stderr)
        print(getattr(task.plan, "hints", task), file=sys.stderr)
        return 3

    problems = check_plan(task, len(pools))
    print(f"resolved workflow: {len(task.plan.steps)} steps", flush=True)
    for step in sorted(task.plan.steps, key=lambda s: s.order):
        stem = Path(step.transform._path).stem
        print(f"  [{step.order}] {stem}: {len(step.group_by_instances)} task(s)")

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    render_dag(ARTIFACTS / "insert_stats_dag", task)

    if problems:
        print("\nPLAN REJECTED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 3
    print("plan checks: OK", flush=True)

    if plan_only:
        print("\nplan-only: nothing deployed, nothing run. Pass --run to execute.")
        return 0

    if not a.no_deploy:
        print("=== Deploy() ===", flush=True)
        try:
            agent.Deploy()
        except subprocess.CalledProcessError as e:
            print(f"\ndeploy failed ({e}). Connect once by hand and re-run; "
                  f"never retry in a loop.", file=sys.stderr)
            return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # "update", never "clear": the agent home is shared with the assembly run's
    # results, and a task key is content-derived so a changed workflow gets its
    # own directory anyway.
    agent.StageWorkflow(task, on_exist="update")

    # The slurm preset, explicitly. The default is `local`, which would run all
    # 35 alignments on fir's login node.
    nxf_config = agent.GetNxfConfigPresets()["slurm"]
    params = {"slurmAccount": a.slurm_account}
    overrides = None if a.stock_resources else TRIMMED_RESOURCES
    print(f"=== executor: slurm, account {a.slurm_account}, "
          f"resources: {'stock' if a.stock_resources else 'trimmed'} ===", flush=True)
    agent.RunWorkflow(task, config_file=nxf_config, params=params,
                      resource_overrides=overrides)

    print(f"=== waiting (timeout {a.timeout_s / 3600:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_s, poll_s=a.poll_s)
    # `errored` means the pid lock is gone and the completion sentinel has not
    # appeared -- and on this run neither implication holds. The one-pool smoke
    # reported `errored` two seconds before writing the sentinel, and the 35-pool
    # run reported it while the agent was demonstrably alive and had been in
    # "compiling results" for a quarter of an hour: the final step hashes ~20 GB
    # of BAM on a login node and takes far longer than the fan-out did.
    #
    # So the verdict is not believed on its own. The LOG is the evidence: keep
    # polling for the sentinel, and give up only when the log has also stopped
    # growing. A genuinely dead run goes quiet and is still reported as errored,
    # which is the case this is meant to keep catching.
    if result["status"] == "errored":
        print("=== engine reports errored; checking whether the log is still "
              "moving before believing it ===", flush=True)
        last_len, _last_marker = -1, None
        stalled, deadline = 0, time.monotonic() + a.timeout_s
        while time.monotonic() < deadline:
            lines = agent.TailWorkflowLog(task.GetKey(), source="agent",
                                          lines=40).get("lines", [])
            if any("run completed at" in ln for ln in lines):
                result = {**result, "status": "completed", "tail": lines}
                break
            marker = lines[-1] if lines else ""
            stalled = stalled + 1 if len(lines) == last_len and marker == _last_marker \
                else 0
            _last_marker, last_len = marker, len(lines)
            if stalled >= 3:      # three quiet polls in a row: it really is gone
                result = {**result, "tail": lines}
                break
            time.sleep(a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 3600:.2f} h ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # "completed" IS NOT "succeeded" -- read the task table, not the log tail.
    n_failed = check_tasks(a.host, a.agent_home, task.GetKey())
    if n_failed:
        print(f"\n{n_failed} TASK(S) FAILED AND NEXTFLOW IGNORED IT. The results "
              f"below are incomplete.", file=sys.stderr)

    src = agent.GetResultSource(task)
    out = Path(a.out).resolve() if a.out else (staging / "results")
    out.mkdir(parents=True, exist_ok=True)
    # Everything except the BAMs. Measured on the 35-pool insert run rather than
    # guessed: the whole per-bp coverage tree is 60 MB (bedGraph run-length
    # intervals, pigz'd) against 20 GB of BAM, because minimap2 is not run with
    # --sam-hit-only and every unmapped read is in there too. So the per-bp
    # tracks come back and the BAMs stay on fir for as long as the run does.
    print(f"=== retrieving (all products except BAM): {src.GetPath()} -> {out} ===",
          flush=True)
    subprocess.run([
        "rsync", "-a", "--info=stats1",
        "--include=*/",
        "--include=_metadata/***",
        "--include=*assembly_stats*/***",
        "--include=*per_contig_coverage*/***",
        "--include=*per_bp_coverage*/***",
        "--include=*read_qc_stats*/***",
        "--exclude=*",
        f"{a.host}:{src.GetPath()}/", f"{out}/",
    ], check=True)

    print(f"\nretrieved {len(list(out.rglob('*.json')))} json + "
          f"{len(list(out.rglob('*.tsv')))} tsv + "
          f"{len(list(out.rglob('*.gz')))} gz under {out}")
    print(f"BAMs remain at {a.host}:{src.GetPath()}")

    # While the run directory still exists: name every output from the task that
    # produced it. After this the results are self-describing.
    attribute_from_run(out, a.host, str(Path(src.GetPath()).parent))
    summarize(out, out.parent)
    return 2 if n_failed else 0


if __name__ == "__main__":
    sys.exit(main())
