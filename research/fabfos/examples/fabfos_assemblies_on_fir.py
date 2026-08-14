#!/usr/bin/env python3
"""Re-assemble the SCADC fosmid pools on fir, keeping the assembly graphs.

    PYTHONPATH=src python examples/fabfos_assemblies_on_fir.py --plan-only
    PYTHONPATH=src python examples/fabfos_assemblies_on_fir.py --pools pool03_CAATCGAC
    PYTHONPATH=src python examples/fabfos_assemblies_on_fir.py            # all 35

WHY THIS EXISTS
---------------
The previous multi-assembly run (AT7jCizU) kept only the contig FASTA from each
assembler. Everything else lived in a node-local workspace that is wiped at job
end, so the assembler's own graph -- the thing that actually says whether a
fosmid contig is a closed circle -- was discarded. Both assembly transforms now
declare the graph as a second product, and this driver re-runs the two
assemblers over the same 35 pools so those graphs exist.

It stops at the assemblies. Clustering and junction resolution are not settled,
and carrying the run past this point would bake in decisions nobody has made.

WHAT IS GIVEN, AND WHERE IT LIVES
---------------------------------
The reads are 32 GB of already-host-filtered, pair-aware interleaved FASTQ that
are ALREADY on fir from the previous run. They are declared at their fir paths
and never uploaded: metasmith binds an item's own path into the task container,
so an item declared at its real remote path is bound where it really is. The
only things staged with the task are the 35 tiny read_metadata JSONs, written
here via AddValue (a RELATIVE path inside the library, which is what makes an
item travel with the task rather than be referenced in place).

THE INPUT LIBRARY IS REBUILT, NOT REUSED -- AND THAT IS NOT OPTIONAL
--------------------------------------------------------------------
AT7jCizU registered its experiment item as `fosmids::recovery_experiment`. That
namespace has since been renamed to `fabfos::`, so a reused library would fail
to resolve against the current types. Nothing is carried over.

That item is absent here anyway: it is a downstream marker for the recovery
lane, and this run has no downstream.

WHY THE GRAPHS COME BACK WITHOUT BEING TARGETS
-----------------------------------------------
`publish_intermediates` defaults to True, so every declared product of every
executed step is published -- not just the target endpoints. The two graphs are
co-products of the two assembly steps, so they arrive as their own results
directories. Naming them as targets as well would be redundant and would risk
the planner treating them as a separate demand.

THE AGENT HOME IS REUSED ON PURPOSE
------------------------------------
`scadc_multiassembly_iso` already carries the extracted relay, the 0.19.0-fabfos
agent image, and -- load-bearing -- a `dev/metasmith` tree that the msm wrapper
binds OVER the engine inside the container. That tree was verified byte-identical
to this repo's engine pin, so the executing engine matches the planner. Deploying
into a fresh directory would silently drop that bind. Deploy() does not create
`dev/`; it only binds it if present.

ONE SSH SESSION, NEVER A RETRY LOOP
------------------------------------
The engine's AgentShell opens a single interactive `ssh <host>` and runs every
remote command through it. fir's ssh config sets ControlMaster no behind a
ProxyCommand guard; a loop around the connect is a Duo push per iteration and a
prior run in this project was halted by an account lockout that way. On failure
this exits with a message, and the human connects once by hand.
"""
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))

from fabfos.pipelines.common import resolve_library_root  # noqa: E402

from metasmith.python_api import (  # noqa: E402
    Agent, ContainerRuntime, DataInstanceLibrary, DataTypeLibrary, SshSource,
    TargetBuilder, TransformInstanceLibrary,
)

LIB = resolve_library_root()

# Only the assembly domain is offered. The reads are given as
# `sequences::host_filtered_short_reads`, so megahit and spades are the entire
# plan; handing the planner the full FabFos domain list would offer it dozens of
# transforms it cannot use and a few it could chain past the intended stop.
DOMAINS = ["assembly"]

# fir's Lmod name for apptainer, matching the deployed agent's setup_commands.
SETUP_COMMANDS = ["module load apptainer"]

# The agent image is a fabfos-branch build of engine 0.19.0. It is NOT derived
# from a build hash the way the published releases are, so it cannot be computed
# from the source tree -- it is named explicitly, and the .sif is already in
# fir's apptainer cache.
AGENT_CONTAINER = "docker://quay.io/hallamlab/metasmith:0.19.0-fabfos"

READS_TYPE = "sequences::host_filtered_short_reads"
READS_SUFFIX = ".host_filtered.fq.gz"


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


def discover_pools(host: str, reads_dir: str) -> dict[str, str]:
    """pool_barcode -> absolute path of its interleaved reads, from the host.

    Listed on the far side rather than assumed from a local manifest: the reads
    ARE the far side, and a pool named here but absent there would plan
    perfectly and fail hours later inside a container.
    """
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
    """One library holding every pool: read_metadata + its reads, per pool.

    The reads are parented to their own pool's metadata. That parent link is
    what the Orchestrator pairs on at runtime -- both input manifests list all
    35 entries and the lineage file says, positionally, which metadata each read
    file belongs to. A read file parented to the wrong metadata would assemble
    the wrong pool and nothing would complain, so the pairing is verified in
    main() before anything is staged.
    """
    xgdb = staging / "inputs.xgdb"
    if xgdb.exists():
        shutil.rmtree(xgdb)
    inputs = DataInstanceLibrary(xgdb)
    inputs.AddTypeLibrary(namespace="sequences",
                          lib=DataTypeLibrary.Load(LIB / "data_types/sequences.yml"))

    for pool in sorted(pools):
        # Interleaved throughout: the previous run's background_filter emitted
        # pair-aware interleaved FASTQ, which is what "paired" selects here
        # (megahit --12, metaSPAdes --12).
        meta = inputs.AddValue(
            name=f"read_metadata_{pool}.json",
            value={"parity": "paired", "length_class": "short"},
            dtype="sequences::read_metadata",
        )
        # Absolute -> referenced in place on fir. Relative (above) -> staged.
        inputs.AddItem(pools[pool], "sequences::host_filtered_short_reads",
                       parents={meta})
    inputs.Save()
    return inputs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="fir")
    ap.add_argument("--reads-dir",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "host_filtered_reads_pairaware_20260724")
    ap.add_argument("--agent-home",
                    default="/scratch/phyberos/metasmith_fabfos/"
                            "scadc_multiassembly_iso",
                    help="reused deliberately -- see this module's docstring on "
                         "the dev/metasmith bind that must not be lost")
    ap.add_argument("--container", default=AGENT_CONTAINER)
    ap.add_argument("--slurm-account", default="rrg-shallam-ab",
                    help="charged on every sbatch. slurm.nf ships the literal "
                         "placeholder '<slurm_account>', which sbatch rejects.")
    ap.add_argument("--pools", nargs="*", default=None,
                    help="pool barcodes to assemble; default is all of them. "
                         "Use one small pool for a smoke run.")
    ap.add_argument("--plan-only", action="store_true",
                    help="resolve and render the DAG; touch no remote host "
                         "beyond the single ls that discovers the pools")
    ap.add_argument("--no-deploy", action="store_true",
                    help="skip Deploy(); the agent home is already provisioned "
                         "and Deploy is only re-run when it might not be")
    ap.add_argument("--timeout-s", type=float, default=20 * 3600)
    ap.add_argument("--poll-s", type=float, default=120.0)
    ap.add_argument("--out", default=None,
                    help="local directory to retrieve results into. Default is "
                         "under .awm/data/runs/, named for the run.")
    a = ap.parse_args()

    ts = int(time.time())
    staging = REPO / ".awm" / "data" / "runs" / f"fabfos_assemblies_{ts}"
    staging.mkdir(parents=True, exist_ok=True)

    print(f"=== discovering pools under {a.host}:{a.reads_dir} ===", flush=True)
    available = discover_pools(a.host, a.reads_dir)
    if a.pools:
        missing = [p for p in a.pools if p not in available]
        if missing:
            raise SystemExit(
                f"requested pool(s) not present on {a.host}: {missing}\n"
                f"available: {sorted(available)}")
        pools = {p: available[p] for p in a.pools}
    else:
        pools = available
    print(f"    {len(pools)} of {len(available)} pools: {', '.join(sorted(pools))}",
          flush=True)

    inputs = build_inputs(staging, pools)

    resources = [DataInstanceLibrary.Load(LIB / f"resources/{n}")
                 for n in ("env", "lib")]
    transforms = [TransformInstanceLibrary.Load(LIB / f"transforms/{d}")
                  for d in DOMAINS]

    home = SshSource(host=a.host, path=a.agent_home).AsSource()
    agent = Agent(home=home, runtime=ContainerRuntime.APPTAINER,
                  container=a.container, setup_commands=SETUP_COMMANDS)

    print("=== planning ===", flush=True)
    targets = TargetBuilder()
    # Both assemblers, side by side. The graphs are co-products of these two
    # steps and are published as intermediates -- see the docstring.
    targets.Add("sequences::spades_assembly")
    targets.Add("sequences::megahit_assembly")

    # The WHOLE library, deliberately -- NOT `inputs.AsSamples(...)`.
    #
    # AsSamples on this topology is silently wrong on the pinned engine. It
    # caches each sample's descendant set keyed on that sample's ANCESTOR set,
    # and a read_metadata item has no ancestors, so all 35 share the empty key
    # and every sample view is handed the FIRST pool's reads. The plan still
    # resolves, still says "[35] samples", and still runs -- it just assembles
    # pool01 thirty-five times. Caught by checking the staged input manifest:
    # 35 read_metadata paths against 1 read path.
    #
    # Handing over the library whole uses the runtime fan-out instead: both
    # manifests carry all 35 entries and the Orchestrator pairs them by the
    # positional lineage in workflow.lineage_of_given.json (verified: 0 of 35
    # mispaired). This is also the shape the previous fir run used.
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

    # A collapse of the sample fan-out does not fail, it just plans a smaller
    # run that looks identical from the outside -- so it is checked here rather
    # than discovered from a results directory with one file in it.
    given = Counter(g.dtype_name for g in task.plan.given)
    for t in (READS_TYPE, "sequences::read_metadata"):
        if given.get(t, 0) != len(pools):
            print(f"\nFAN-OUT COLLAPSED: the plan carries {given.get(t, 0)} "
                  f"[{t}] for {len(pools)} pools. Every pool must appear once, "
                  f"or the run assembles one pool repeatedly.", file=sys.stderr)
            return 3

    print(f"resolved workflow: {len(task.plan.steps)} steps", flush=True)
    for i, step in enumerate(task.plan.steps):
        name = getattr(getattr(step, "transform", None), "name", None) or f"step{i}"
        print(f"  [{i}] {name}")

    dag = (REPO / "reports/dag/fabfos_assemblies").resolve()
    dag.parent.mkdir(parents=True, exist_ok=True)
    task.plan.RenderDAG(dag)
    print(f"DAG -> {dag.with_suffix('.svg')}", flush=True)

    if a.plan_only:
        print("\n--plan-only: nothing deployed, nothing run.")
        return 0

    if not a.no_deploy:
        print("=== Deploy() ===", flush=True)
        try:
            agent.Deploy()
        except subprocess.CalledProcessError as e:
            print(f"\ndeploy failed ({e}). See this module's docstring: connect "
                  f"once by hand and re-run; never retry in a loop.",
                  file=sys.stderr)
            return 4

    print(f"=== task key: {task.GetKey()} ===", flush=True)
    # "update" rather than "clear": the agent home is SHARED with the previous
    # run's results, and a task key is content-derived, so a changed workflow
    # gets a fresh directory anyway. "clear" here would be a loaded gun pointed
    # at whichever run happened to collide.
    agent.StageWorkflow(task, on_exist="update")

    # The slurm preset, explicitly. The default is `local`, which would run a
    # 96-core assembler on fir's login node.
    nxf_config = agent.GetNxfConfigPresets()["slurm"]
    params = {"slurmAccount": a.slurm_account}
    print(f"=== executor: slurm, account {a.slurm_account} ===", flush=True)
    # No resource_overrides: spades asks for 96 cores / 128 GB and megahit for
    # 32 in the transforms themselves, which is where the measurement that
    # justifies those numbers is recorded.
    agent.RunWorkflow(task, config_file=nxf_config, params=params)

    print(f"=== waiting (timeout {a.timeout_s / 3600:.1f}h, poll {a.poll_s:.0f}s) ===",
          flush=True)
    result = agent.WaitForWorkflow(task, timeout_s=a.timeout_s, poll_s=a.poll_s)
    print(f"=== status: {result['status']} after {result['elapsed_s'] / 3600:.2f} h ===",
          flush=True)
    for line in result["tail"]:
        print(f"    {line}")
    if result["status"] != "completed":
        return 2

    # "completed" IS NOT "succeeded". slurm.nf sets errorStrategy='ignore' once
    # a process exhausts its retries, so a task that died every attempt leaves
    # the workflow green and its outputs simply absent. The previous run lost 2
    # of 35 spades assemblies exactly this way and still reported completion.
    swallowed = [ln for ln in result["tail"]
                 if "Error is ignored" in ln or "terminated with an error" in ln]
    if swallowed:
        print("\nA TASK FAILED AND NEXTFLOW IGNORED IT:", file=sys.stderr)
        for ln in swallowed:
            print(f"    {ln}", file=sys.stderr)

    src = agent.GetResultSource(task)
    out = Path(a.out).resolve() if a.out else (staging / "results")
    out.mkdir(parents=True, exist_ok=True)
    print(f"=== retrieving: {src.GetPath()} -> {out} ===", flush=True)
    subprocess.run(["rsync", "-a", "--info=stats1",
                    f"{a.host}:{src.GetPath()}/", f"{out}/"], check=True)

    # Completeness is checked, not assumed. Four files per pool: two contig
    # FASTAs and two graphs.
    counts = {d.name: len([p for p in d.iterdir() if p.is_file()])
              for d in sorted(out.iterdir()) if d.is_dir() and not d.name.startswith("_")}
    print("=== results ===", flush=True)
    for name, n in counts.items():
        flag = "" if n == len(pools) else f"   <-- expected {len(pools)}"
        print(f"  {name}: {n}{flag}")
    incomplete = [n for n, c in counts.items() if c != len(pools)]
    if len(counts) != 4 or incomplete:
        print(f"\nINCOMPLETE: expected 4 output types x {len(pools)} pools.",
              file=sys.stderr)
        return 2
    print(f"\nok: {len(pools)} pools x (contigs + graph) x 2 assemblers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
