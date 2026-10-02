"""What every ASPIRE campaign on sockeye shares: the input library, the plan, the run and its retrieval.

A campaign module builds a `Campaign` and hands `main` its own extra subcommands. Every ssh here
rides the awm ssh domain's connection, and every download runs on the login node, since compute
nodes have no internet.
"""

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "research" / "fabfos" / "examples"))
sys.path.insert(0, str(REPO / "research" / "aspire"))

from _driver import (  # noqa: E402
    SOCKEYE_ACCOUNT, SOCKEYE_HOST, SOCKEYE_IMAGE_STORE, check_schedulable,
    check_staged_executor, check_tasks, envs_from_plan, pin_external_leaf_ids,
    provision_dev_overlay_remote, retrieve, sockeye_agent, ssh_once,
)
from aspire_asv_pipeline import read_metadata  # noqa: E402
from metasmith.models.dag_renderer import DagMode  # noqa: E402
from metasmith.python_api import (  # noqa: E402
    DataInstanceLibrary, TransformInstanceLibrary,
)

MLIB = REPO / "src" / "metasmith_libraries"
HOST = os.environ.get("ASPIRE_SOCKEYE_HOST", SOCKEYE_HOST)
REFS = os.environ.get("ASPIRE_REFS", "/arc/project/st-shallam-1/aspire_refs")
SILVA = f"{REFS}/silva_138_2"
SILVA_FILES = ("silva.arb", "silva_seqs.qza", "silva_tax.qza", "silva_nb_classifier.qza")
IMAGE_STORE = os.environ.get("ASPIRE_IMAGE_STORE", SOCKEYE_IMAGE_STORE)
CONTAINER = os.environ.get("ASPIRE_AGENT_CONTAINER", "docker://quay.io/hallamlab/metasmith:0.23.0")
# The files a campaign with SpiecEasi switched off hands `spieceasi_external`.
EXTERNAL_GRAPHS = {"network_all.graphml": "aspire::external_graph_all",
                   "network_thr.graphml": "aspire::external_graph_thr",
                   "node_features.csv": "aspire::external_node_features"}
SWITCHES = ("spieceasi", "network_modules", "asv_mag_link", "graph_network")


def aspire_image() -> str:
    for line in (MLIB / "resources" / "env" / "aspire.env").read_text().splitlines():
        if line.startswith("container:"):
            return line.split("://", 1)[1].strip()
    raise SystemExit("resources/env/aspire.env names no container")


@dataclass
class Sample:
    sid: str
    r1: str
    r2: str


# A dedup run on the remote host: the assembly, the cluster table and the run's centroid bins
# under bins/, which `collect_mags` lays out for the ASV-MAG linker after barrnap.
@dataclass
class MagSet:
    root: str

    @property
    def assembly(self) -> str:
        return f"{self.root}/assembly.fna"

    @property
    def cluster_table(self) -> str:
        return f"{self.root}/cluster_table.tsv"

    @cached_property
    def bins(self) -> list[str]:
        return [f"{self.root}/bins/{n}" for n in ssh_once(HOST, f"ls {self.root}/bins").split()]


@dataclass
class Campaign:
    name: str
    here: Path
    root: str
    samples: list[Sample]
    study_sheet: str
    params: str
    targets: list[str]
    mito_reference: str
    contaminant_reference: str
    switches_on: set[str] = field(default_factory=set)
    mags: MagSet | None = None
    external_graphs: str | None = None
    measurements: str | None = None
    resource_overrides: dict = field(default_factory=dict)

    @property
    def agent_home(self) -> str:
        return f"{self.root}/agent_home"

    @property
    def local_results(self) -> Path:
        return REPO / "data" / "aspire" / self.name

    @property
    def work(self) -> Path:
        return REPO / "cache" / "aspire" / self.name

    def remote_inputs(self) -> list[str]:
        paths = [f"{SILVA}/{n}" for n in SILVA_FILES] + [self.mito_reference, self.contaminant_reference]
        paths += [p for s in self.samples for p in (s.r1, s.r2)]
        if self.mags:
            paths += [self.mags.assembly, self.mags.cluster_table, *self.mags.bins]
        if self.external_graphs:
            paths += [f"{self.external_graphs}/{n}" for n in EXTERNAL_GRAPHS]
        return paths


def agent(c: Campaign):
    return sockeye_agent(host=HOST, agent_home=c.agent_home, container=CONTAINER,
                         image_store=IMAGE_STORE)


def build_inputs(c: Campaign):
    c.work.mkdir(parents=True, exist_ok=True)
    lib = DataInstanceLibrary(c.work / "inputs.xgdb")
    lib.Purge()
    for t in ("aspire.yml", "amplicon.yml", "sequences.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    study = lib.AddValue("study_metadata.tsv", c.study_sheet, "aspire::study_metadata")
    lib.AddValue("params.yml", c.params, "aspire::params", parents={study})
    for base in SWITCHES:
        arm = "on" if base in c.switches_on else "off"
        lib.AddValue(f"policy_{base}.txt", arm, f"aspire::{base}_{arm}", parents={study})
    if c.measurements is not None:
        lib.AddValue("sample_measurements.tsv", c.measurements, "aspire::sample_measurements",
                     parents={study})
    for s in c.samples:
        meta = lib.AddValue(f"{s.sid}.read_metadata.json", read_metadata(s.sid, "paired"),
                            "sequences::read_metadata", parents={study})
        pair = lib.AddValue(f"{s.sid}.read_pair.txt", s.sid, "sequences::read_pair", parents={meta})
        for path, dtype in ((s.r1, "zipped_forward_short_reads"), (s.r2, "zipped_reverse_short_reads")):
            lib.AddItem(path, f"sequences::{dtype}", parents={pair})
    pin_external_leaf_ids(lib)
    lib.Save()
    return lib


def build_references(c: Campaign):
    lib = DataInstanceLibrary(c.work / "references.xgdb")
    lib.Purge()
    for t in ("aspire.yml", "amplicon.yml", "sequences.yml", "binning_local.yml"):
        lib.AddTypeLibrary(MLIB / "data_types" / t)
    lib.AddItem(SILVA, "amplicon::silva_db")
    lib.AddItem(c.mito_reference, "aspire::mito_reference_source")
    lib.AddItem(c.contaminant_reference, "aspire::contaminant_reference_source")
    if c.mags:
        asm = lib.AddItem(c.mags.assembly, "sequences::assembly")
        lib.AddItem(c.mags.cluster_table, "binning_local::cluster_table", parents={asm})
        for b in c.mags.bins:
            lib.AddItem(b, "binning_local::quality_bin_fasta", parents={asm})
    if c.external_graphs:
        for name, dtype in EXTERNAL_GRAPHS.items():
            lib.AddItem(f"{c.external_graphs}/{name}", dtype)
    pin_external_leaf_ids(lib)
    lib.Save()
    return lib


def plan(c: Campaign, smith):
    inputs = build_inputs(c)
    task = smith.GenerateWorkflow(
        samples=list(inputs.AsSamples("aspire::study_metadata")),
        resources=[DataInstanceLibrary.Load(MLIB / "resources" / n) for n in ("env", "lib")]
                  + [build_references(c)],
        transforms=[TransformInstanceLibrary.Load(MLIB / "transforms" / n)
                    for n in ("aspire", "logistics", *(("metagenomics",) if c.mags else ()))],
        targets=c.targets,
    )
    if not task.ok:
        sys.exit(f"did not solve: dropped {sorted(task.plan.dropped_targets)}")
    n = len(c.samples)
    got = {}
    for g in task.plan.given:
        got[g.dtype_name] = got.get(g.dtype_name, 0) + 1
    for dtype in ("sequences::read_metadata", "sequences::zipped_forward_short_reads",
                  "sequences::zipped_reverse_short_reads"):
        if got.get(dtype, n) != n:
            sys.exit(f"{dtype}: the plan carries {got.get(dtype, 0)}, the campaign has {n} samples")
    print(f"plan: {len(task.plan.steps)} steps over {n} samples, key={task.GetKey()}")
    return task


def sif_name(uri: str) -> str:
    return uri.replace("://", "..").replace(":", "..").replace("/", "_") + ".sif"


def plan_images(task) -> list[str]:
    uris = [CONTAINER]
    for name in envs_from_plan(task):
        for line in (MLIB / "resources" / "env" / name).read_text().splitlines():
            if line.startswith("container:"):
                uris.append(line.split(":", 1)[1].strip())
    return uris


def cmd_side_load_images(c: Campaign, _):
    # Built here, not on the login node: unpacking layers onto sockeye's GPFS scratch takes most
    # of an hour per image, and a dropped ssh session kills the build with it.
    uris = plan_images(plan(c, agent(c)))
    present = ssh_once(HOST, "; ".join(f"[ -e {IMAGE_STORE}/{sif_name(u)}.verified ] && echo {u}"
                                       for u in uris) + "; true").split()
    local_image = aspire_image()
    tmp = c.work / "apptainer_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "APPTAINER_TMPDIR": str(tmp)}
    for uri in uris:
        if uri in present:
            print(f"present  {uri}")
            continue
        sif = c.work / sif_name(uri)
        if not sif.exists():
            source, tar = uri, c.work / "aspire.tar"
            if uri == f"docker://{local_image}":
                subprocess.run(["docker", "save", "-o", str(tar), local_image], check=True)
                source = f"docker-archive://{tar}"
            partial = sif.with_suffix(".partial")
            subprocess.run(["apptainer", "build", "--force", str(partial), source], check=True, env=env)
            partial.rename(sif)
            tar.unlink(missing_ok=True)
        remote = f"{IMAGE_STORE}/{sif.name}"
        subprocess.run(["rsync", "-a", "--partial", "-e", "ssh -o BatchMode=yes", str(sif),
                        f"{HOST}:{remote}"], check=True)
        ssh_once(HOST, f"module load gcc/9.4.0 apptainer/1.3.1 && "
                       f"apptainer exec --no-home --cleanenv {remote} true && : > {remote}.verified")
        sif.unlink()
        print(f"loaded   {uri}")
    return 0


def cmd_check_refs(c: Campaign, _):
    wanted = c.remote_inputs()
    out = ssh_once(HOST, "; ".join(f'[ -s {p} ] || echo "MISSING {p}"' for p in wanted))
    missing = [ln for ln in out.splitlines() if ln.startswith("MISSING")]
    print("\n".join(missing) if missing else f"all {len(wanted)} inputs present on {HOST}")
    return 1 if missing else 0


def cmd_run(c: Campaign, args):
    smith = agent(c)
    task = plan(c, smith)
    if args.plan_only:
        return 0
    if cmd_check_refs(c, args):
        return 1
    smith.Deploy()
    provision_dev_overlay_remote(HOST, c.agent_home)
    smith.StageWorkflow(task, on_exist="update")
    if check_staged_executor(HOST, c.agent_home, task.GetKey()):
        return 3
    report = smith.MaterialiseImages(task)
    print(f"images: {report['fetched']} fetched, {report['already_present']} present")
    if check_schedulable(HOST, SOCKEYE_ACCOUNT, c.resource_overrides, workdir=c.agent_home):
        return 4
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm"],
        params={"slurmAccount": SOCKEYE_ACCOUNT},
        resource_overrides=c.resource_overrides,
    )
    print(f"launched {task.GetKey()}; poll with `status`")
    return 0


def cmd_status(c: Campaign, _):
    smith = agent(c)
    task = plan(c, smith)
    result = smith.WaitForWorkflow(task, timeout_s=1, poll_s=1)
    print(f"status: {result['status']}")
    for line in result["tail"]:
        print(f"    {line}")
    return check_tasks(HOST, c.agent_home, task.GetKey(), attempt="latest")


def cmd_retrieve(c: Campaign, _):
    smith = agent(c)
    task = plan(c, smith)
    if check_tasks(HOST, c.agent_home, task.GetKey(), attempt="latest"):
        return 1
    retrieve(HOST, smith.GetResultSource(task).GetPath(), c.local_results)
    return 0


# A product the plan does not target never reaches the results directory. Its producer's
# work directory holds the real files; every consumer's holds symlinks to them. A workflow
# names a product by a key the plan assigns, not the type's, so the step is found by name.
def cmd_fetch_intermediates(c: Campaign, args):
    key = args.key or plan(c, agent(c)).GetKey()
    run = f"{c.agent_home}/runs/{key}"
    dest = c.work / f"intermediates_{key}"
    for t in args.transforms:
        rows = ssh_once(HOST, f"tail -q -n +2 $(ls -d {run}/_metasmith/logs.2* | tail -1)/nxf_tasks.csv"
                              f" | awk -F, '$4 ~ /^p[0-9]+__{t}(_cached)? / && $5 == \"COMPLETED\" {{print $2}}'").split()
        assert len(rows) == 1, f"[{t}] want one completed task in {run}, found {rows}"
        out = dest / t
        out.mkdir(parents=True, exist_ok=True)
        subprocess.run(["rsync", "-a", "--no-links", "--exclude", ".*",
                        f"{HOST}:{run}/nxf_work/{rows[0]}*/", f"{out}/"], check=True)
        print(f"{t}: {out}")
    return 0


def cmd_dag(c: Campaign, _):
    task = plan(c, agent(c))
    reports = c.here / "reports"
    reports.mkdir(exist_ok=True)
    # RenderDAG reads a dotted basename's suffix as the format, so the stem carries no dot.
    for mode, suffix in ((DagMode.PLAIN, ""), (DagMode.STEPS, "_steps"), (DagMode.LEGEND, "_legend")):
        print(task.plan.RenderDAG(str(reports / f"{c.name}{suffix}"), format="svg",
                                  show_step_order=True, mode=mode))
    return 0


COMMON = {
    "side-load-images": cmd_side_load_images, "check-refs": cmd_check_refs, "run": cmd_run,
    "status": cmd_status, "retrieve": cmd_retrieve, "dag": cmd_dag,
    "fetch-intermediates": cmd_fetch_intermediates,
}


def main(doc: str, make_campaign, extra: dict | None = None, add_args=None):
    """`make_campaign(args)` builds the campaign; `extra` maps more subcommands to `fn(c, args)`."""
    ap = argparse.ArgumentParser(description=doc, formatter_class=argparse.RawDescriptionHelpFormatter)
    if add_args:
        add_args(ap)
    sub = ap.add_subparsers(dest="cmd", required=True)
    commands = {**COMMON, **(extra or {})}
    for name in commands:
        p = sub.add_parser(name)
        if name == "run":
            p.add_argument("--plan-only", action="store_true")
        if name == "fetch-intermediates":
            p.add_argument("transforms", nargs="+", help="transform names, e.g. filter_table")
            p.add_argument("--key", help="a run planned from an earlier tree, instead of re-planning")
    args = ap.parse_args()
    return commands[args.cmd](make_campaign(args), args)
