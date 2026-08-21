#!/usr/bin/env python
"""Drive `annotation_trio_from_assembly` on a local docker agent and verify it.

    assembly --> prodigal --> orfs --> chunks --> kofamscan       --> merge
                                              --> diamond_uniref50 --> merge
                                              --> interproscan     --> merge

The three reference databases are downloaded by the plan's first three steps
unless `--givens` supplies them from disk. Those downloads are the only steps
that can run at the same time as each other, so the executor is sized to hold
all three at once and the trace is checked afterwards for the overlap.

A green nextflow exit proves nothing here: the `local` preset ignores a step
that has exhausted its retries and `failOnIgnore` is off, so a run that
produced nothing still exits 0. Every check below is against what the run
left on disk.

    python run_annotation_trio.py [--givens] [--targets kofam,uniref50,interpro]
"""
import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

WORKSPACE = Path(__file__).parent.resolve()
REPO = WORKSPACE.parents[1]
sys.path.insert(0, str(REPO/"src"))

from metasmith.python_api import (                                  # noqa: E402
    Agent, AgentPaths, DataInstanceLibrary, Duration, Resources, Runtime,
    Size, Source, TargetBuilder, TransformInstanceLibrary,
)

MLIB = REPO/"src/metasmith_libraries"

# Pinned rather than taken from CONTAINER_TAG: a `-bp` anywhere in this tree
# writes build_hash.txt and the tag becomes `0.20.4+<hash>`, which was never
# pushed. This is the image the agent actually has.
AGENT_IMAGE = "docker://quay.io/hallamlab/metasmith:0.20.4"

DEFAULT_HOME = Path.home()/"msm.annotation-trio"
DEFAULT_ASSEMBLY = Path(
    "/home/tony/agentic_workspace/projects/metasmith/fabfos/bench-eydallin"
    "/research/fabfos/annotation_lanes/cache/inserts.fna"
)

TARGETS = {
    "kofam":    "annotation::kofamscan_results",
    "uniref50": "annotation::diamond_uniref50_results",
    "interpro": "annotation::interproscan_results",
}

# References already materialised in this repo by DVC. Read-only hardlinks into
# a shared cache, so they are registered by absolute path and bind-mounted in
# place -- never localized, which would copy 24 GB and take write access to a
# cache other worktrees share.
GIVENS = {
    "ref::uniref50_diamond_db": REPO/"data/fabfos/processed/uniref50_dmnd/uniref50.dmnd",
    "ref::kofamscan_profiles":  REPO/"data/fabfos/processed/kofam_ref/profiles",
    "ref::kofamscan_ko_list":   REPO/"data/fabfos/processed/kofam_ref/ko_list.tsv",
}

# `ref::interproscan_data` is a tgz the download builds by running the tool's
# own setup.py over the fetched index; nothing on disk here is that artifact,
# so the interpro arm always downloads.
GIVEN_TARGETS = {"kofam", "uniref50"}

DOWNLOAD_STEPS = ("downloadUniRef50DB", "downloadKofamDB", "downloadInterProScanDB")

# The executor holds 12 of 16 cores and 40 of 58 GB, leaving the box usable
# while this runs. Per-transform rather than one `"*"`: overrides SET rather
# than cap, so a single entry would hand all three downloads the same shape and
# -- at 8 cpus each under a 12-cpu executor -- silently serialise them. These
# three total 11 cpus and 22 GB, so nextflow can hold all three at once.
EXECUTOR = dict(cpus=12, memory="40 GB", queueSize=4)
OVERRIDES = {
    "downloadUniRef50DB":     Resources(cpus=8, memory=Size.GB(12), duration=Duration(hours=12)),
    "downloadKofamDB":        Resources(cpus=1, memory=Size.GB(4),  duration=Duration(hours=6)),
    "downloadInterProScanDB": Resources(cpus=2, memory=Size.GB(6),  duration=Duration(hours=8)),
    # One 36 KB assembly. The library's declared 64 GB asks are sized for real
    # metagenomes and exceed the executor, which nextflow rejects at submit.
    "prodigal":                Resources(cpus=4, memory=Size.GB(8),  duration=Duration(hours=2)),
    "chunkOrfsForAnnotation":  Resources(cpus=2, memory=Size.GB(4),  duration=Duration(hours=1)),
    "diamond_uniref50":        Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=8)),
    "kofamscan":               Resources(cpus=8, memory=Size.GB(12), duration=Duration(hours=8)),
    "interproscan":            Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=8)),
    "merge_kofamscan":         Resources(cpus=1, memory=Size.GB(4),  duration=Duration(hours=1)),
    "merge_diamond_uniref50":  Resources(cpus=1, memory=Size.GB(4),  duration=Duration(hours=1)),
    "merge_interproscan":      Resources(cpus=1, memory=Size.GB(4),  duration=Duration(hours=1)),
}

STEP_RE = re.compile(r"^(p\d+__[A-Za-z0-9_]+)")


def log(msg: str):
    print(f"[trio] {msg}", flush=True)


def parse_trace(lines: list[str]) -> list[dict]:
    if not lines: return []
    header = lines[0].split("\t")
    rows = []
    for ln in lines[1:]:
        if not ln.strip(): continue
        parts = ln.split("\t")
        if len(parts) != len(header): continue
        rows.append(dict(zip(header, parts)))
    return rows


def trace_time(s: str):
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d %H:%M:%S.%f")
    except (ValueError, AttributeError):
        return None


def trace_duration_s(s: str):
    # nextflow writes "1h 2m 3s", "450ms", "-"
    s = (s or "").strip()
    if not s or s == "-": return None
    total, unit = 0.0, {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}
    for value, suffix in re.findall(r"([\d.]+)\s*(ms|[smhd])", s):
        total += float(value)*unit[suffix]
    return total or None


def build_inputs(inputs_dir: Path, assembly: Path, use_givens: bool) -> DataInstanceLibrary:
    if inputs_dir.exists():
        shutil.rmtree(inputs_dir)
    inputs = DataInstanceLibrary(inputs_dir)
    for tl in ["sequences.yml", "ref.yml", "annotation.yml"]:
        inputs.AddTypeLibrary(MLIB/"data_types"/tl)
    inputs.AddItem(assembly, "sequences::assembly")
    if use_givens:
        for dtype, path in GIVENS.items():
            assert path.exists(), f"given reference missing: [{path}]"
            log(f"given  {dtype:32s} {path}")
            inputs.AddItem(path, dtype)
    # No LocalizeContents: it copies every absolute entry into the library, and
    # two of the givens are 17 GB and 7 GB. Absolute paths are bind-mounted at
    # stage time instead, which is how the GUI registers inputs too.
    inputs.Save()
    return inputs


def verify(smith: Agent, task, results_path: Path, wanted: list[str],
           expect_downloads: bool, wait: dict) -> list[str]:
    checks: list[tuple[str, bool, str]] = []

    def add(name, ok, detail=""):
        checks.append((name, bool(ok), detail))

    add("the run reached its completion sentinel",
        wait.get("status") == "completed", f"status={wait.get('status')}")

    trace = smith.ReadWorkflowTrace(task)
    rows = parse_trace(trace.get("lines", []))
    run_dir = Path(trace.get("run_dir", ""))
    add("the run wrote a trace", bool(rows), f"{len(rows)} task row(s) in {trace.get('file')}")

    log_text = ""
    for name in ("agent.log", "nxf.log", "main.log"):
        f = run_dir/name
        if f.is_file():
            log_text += f.read_text(errors="replace")

    by_step: dict[str, list[dict]] = {}
    for r in rows:
        m = STEP_RE.match(r.get("name", ""))
        if m: by_step.setdefault(m.group(1), []).append(r)

    # Not every planned step becomes a nextflow process: one whose product is
    # already in the task cache is pruned when the workflow is compiled, and a
    # cache hit is success rather than a step that vanished. So the set to check
    # against is the one nextflow declared, which it names in its own log --
    # comparing against `task.plan.steps` calls every cached re-run a failure.
    declared: set[str] = set()
    for ln in log_text.splitlines():
        if "Process names:" in ln:
            declared = {x.strip() for x in ln.split("Process names:", 1)[1].split(",") if x.strip()}
    n_steps = len(task.plan.steps)
    pruned = n_steps - len(declared)
    add("every process nextflow declared appears in the trace",
        bool(declared) and declared <= set(by_step),
        f"{len(by_step)} of {len(declared)} declared ran"
        + (f"; {pruned} of {n_steps} plan step(s) served from cache" if pruned > 0 else "")
        + (f"; missing {sorted(declared - set(by_step))}" if declared - set(by_step) else ""))

    bad = sorted({
        f"{s}={r['status']}" for s, rs in by_step.items() for r in rs
        if r.get("status") not in {"COMPLETED", "CACHED"}
    })
    add("no task ended in any state but COMPLETED or CACHED", not bad, "; ".join(bad))

    # Nextflow's local executor refuses at submit any process asking for more
    # than the executor holds, and the preset's `errorStrategy = ignore` turns
    # that into a silent zero-output run. This is the string it prints.
    errored = sorted({
        ln.split("Process `")[-1].split("`")[0]
        for ln in log_text.splitlines() if "terminated with an error exit status" in ln
    })
    add("no step terminated with an error exit status", not errored, "; ".join(errored))
    over = [ln.strip() for ln in log_text.splitlines()
            if "requirement exceeds available" in ln]
    add("no step was refused for exceeding the executor",
        not over, "; ".join(sorted(set(over))[:3]))
    raised = subprocess.run(
        ["bash", "-c",
         f'grep -rIl -- "error while executing transform" "{run_dir}" 2>/dev/null || true'],
        capture_output=True, text=True).stdout.strip().splitlines()
    add("no transform protocol raised", not raised, f"{len(raised)} step log(s)")

    if expect_downloads:
        subs = {}
        for s, rs in by_step.items():
            for tag in DOWNLOAD_STEPS:
                if s.endswith(f"__{tag}"):
                    t = trace_time(rs[0].get("submit", ""))
                    d = trace_duration_s(rs[0].get("duration", ""))
                    if t is not None:
                        subs[tag] = (t, d)
        parts = []
        for k, (t, d) in sorted(subs.items()):
            span = f" for {d:.0f}s" if d else " (no duration recorded)"
            parts.append(f"{k} submitted {t.strftime('%H:%M:%S')}{span}")
        detail = "; ".join(parts)
        ok = False
        if len(subs) == len(DOWNLOAD_STEPS) and all(d for _, d in subs.values()):
            last_submit = max(t for t, _ in subs.values())
            first_finish = min(t.timestamp()+d for t, d in subs.values())
            ok = last_submit.timestamp() < first_finish
        add("the three downloads were in flight at the same time", ok, detail)

    if results_path.is_dir():
        try:
            results = DataInstanceLibrary.Load(results_path)
            found: dict[str, list[Path]] = {}
            for path, type_name, _ in results.Iterate():
                full = path if path.is_absolute() else results_path/path
                found.setdefault(str(type_name), []).append(full)
        except Exception as e:                                       # noqa: BLE001
            found = {}
            add("the results directory reads as a library", False, f"{e}")
        for target in wanted:
            paths = found.get(target, [])
            sizes = [p.stat().st_size for p in paths if p.is_file()]
            add(f"[{target}] collected and non-empty",
                bool(sizes) and all(s > 0 for s in sizes),
                ", ".join(f"{p.name} ({s:,} B)" for p, s in zip(paths, sizes)) or "not produced")
    else:
        add("results were collected", False, f"no directory at {results_path}")

    # Split, because the two cases mean different things. A target that does not
    # resolve is this run having produced nothing usable. An INTERMEDIATE that
    # does not resolve is the known publish/promotion mismatch: cache promotion
    # moves a step's product from nxf_work into task_cache and the `rellink`
    # published under results/ still names the work dir. The targets are
    # unaffected, so that is reported rather than failed on.
    dangling = subprocess.run(
        ["bash", "-c", f'find "{results_path}" -xtype l 2>/dev/null || true'],
        capture_output=True, text=True).stdout.strip().splitlines()
    target_dirs = {t.replace("::", "-") for t in wanted}
    broken_targets = [d for d in dangling
                      if Path(d).parent.name in target_dirs]
    add("every target link resolves", not broken_targets,
        "; ".join(broken_targets))
    if dangling:
        log(f"NOTE: {len(dangling) - len(broken_targets)} intermediate publish(es) "
            f"do not resolve -- promoted into task_cache, publish still names nxf_work")

    print()
    print("="*78)
    print("VERIFICATION")
    print("="*78)
    failures = []
    for name, ok, detail in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  --  {detail}" if detail else ""))
        if not ok: failures.append(name)
    print("="*78)
    return failures


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--home", type=Path, default=DEFAULT_HOME,
                    help="agent home; its task_cache is what makes a re-run cheap")
    ap.add_argument("--assembly", type=Path, default=DEFAULT_ASSEMBLY)
    ap.add_argument("--givens", action="store_true",
                    help="register the on-disk reference databases so the "
                         "downloads are never planned")
    ap.add_argument("--targets", default=None,
                    help=f"comma-separated subset of {sorted(TARGETS)}; "
                         f"defaults to all three, or to the two --givens can serve")
    ap.add_argument("--on-exist", default="clear",
                    choices=["skip", "error", "clear", "update", "update_workflow", "update_data"])
    ap.add_argument("--timeout", type=float, default=48*3600)
    ap.add_argument("--no-materialise", action="store_true",
                    help="skip the image pre-fetch; the first task needing each "
                         "image then fetches it mid-run")
    ap.add_argument("--plan-only", action="store_true")
    args = ap.parse_args()

    if args.targets:
        keys = [k.strip() for k in args.targets.split(",") if k.strip()]
    elif args.givens:
        keys = sorted(GIVEN_TARGETS)
    else:
        keys = sorted(TARGETS)
    unknown = [k for k in keys if k not in TARGETS]
    assert not unknown, f"unknown target(s) {unknown}; pick from {sorted(TARGETS)}"
    wanted = [TARGETS[k] for k in keys]
    if args.givens and "interpro" in keys:
        log("NOTE: --givens cannot serve ref::interproscan_data, so the "
            "interpro arm will still download it")
    expect_downloads = (not args.givens) or ("interpro" in keys)

    log(f"repo      {REPO}")
    log(f"home      {args.home}")
    log(f"assembly  {args.assembly}")
    log(f"targets   {', '.join(wanted)}")
    assert args.assembly.is_file(), f"assembly not found [{args.assembly}]"

    smith = Agent(home=Source.FromLocal(args.home), runtime=Runtime.DOCKER,
                  container=AGENT_IMAGE)
    if not (args.home/"msm").exists():
        log("deploying the agent")
        smith.Deploy(assertive=True)
    else:
        log("agent already deployed")

    inputs = build_inputs(args.home/"annotation_trio.inputs.xgdb", args.assembly, args.givens)
    env_lib = DataInstanceLibrary.Load(MLIB/"resources/env")
    transforms = [
        TransformInstanceLibrary.Load(MLIB/"transforms/logistics"),
        TransformInstanceLibrary.Load(MLIB/"transforms/metagenomics"),
        TransformInstanceLibrary.Load(MLIB/"transforms/functionalAnnotation"),
    ]

    log("solving")
    targets = TargetBuilder()
    for t in wanted: targets.Add(t)
    t0 = time.time()
    task = smith.GenerateWorkflow(
        samples=[inputs], resources=[env_lib], transforms=transforms, targets=targets)
    log(f"solved in {time.time()-t0:.1f}s, ok={task.ok}, key={task.GetKey()}")
    if not task.ok:
        log(f"FAILED to plan: {task}")
        return 1
    for step in task.plan.steps:
        log(f"  p{step.order:02}  {step.transform.name}")
    if args.plan_only:
        return 0

    log("staging")
    smith.StageWorkflow(task, on_exist=args.on_exist)
    if not args.no_materialise:
        log("materialising tool images (this is the pull, not the run)")
        report = smith.SetupEnvironment(task)
        log(f"images: {report.get('fetched', 0)} fetched, "
            f"{report.get('already_present', 0)} already present")

    log(f"running with executor={EXECUTOR}")
    overrides = {k: v for k, v in OVERRIDES.items()}
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["local"],
        params=dict(executor=dict(EXECUTOR), process=dict(tries=1)),
        resource_overrides=overrides,
    )

    log(f"waiting (timeout {args.timeout/3600:.1f}h)")
    # grace_s well above the default 5s: WaitForWorkflow calls a run errored
    # when the sentinel is absent and PID.lock is gone, and a docker agent takes
    # ~20s to start the container that writes that lock -- so the default
    # declares every container run dead before it has begun.
    wait = smith.WaitForWorkflow(task, timeout_s=args.timeout, poll_s=10.0, grace_s=300.0)
    log(f"wait returned status={wait['status']} after {wait['elapsed_s']:.0f}s")
    for ln in wait.get("tail", []):
        print(f"    | {ln}")

    try:
        smith.CheckWorkflow(task)
    except Exception as e:                                           # noqa: BLE001
        log(f"CheckWorkflow raised: {e}")

    results_path = smith.GetResultSource(task).GetPath()
    failures = verify(smith, task, results_path, wanted, expect_downloads, wait)
    if failures:
        log(f"{len(failures)} check(s) failed")
        return 1
    log("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
