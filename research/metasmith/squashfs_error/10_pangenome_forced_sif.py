#!/usr/bin/env python3
"""Run the pangenome workflow on a fresh agent with the sandbox arm shut out.

Deploy -> stage -> run -> collect -> verify, against a home that has never held
an image, so "no sandbox" is a property of the store rather than a claim about
what the code chose.

Three things have to be arranged for that to mean anything, and only the first
is the documented lever:

1. The agent declares `rootfs="sif"`, which drops the unpack rung from the
   fallback chain and names the `.sif` outright at every launch.
2. `APPTAINER_CACHEDIR` is repointed into the fresh home. Without this the store
   root is the caller's shared `~/.apptainer/cache`, which already holds an
   unpacked `.sandbox` for all three tool images. That used to be enough on its
   own to make a "forced sif" run quietly a sandbox run, back when the override
   was a host env var the materialise check and the run command never consulted;
   a declared mode now narrows both. Repointing the store is still what makes
   the run start from nothing.
3. The agent image is hardlinked in rather than pulled: its tag is a local build
   that was never published, so `apptainer pull` would 404. The `[ ! -e <sif> ]`
   gate in `MakeMaterialiseCommand` is what makes seeding work at all.

The store export goes in `setup_commands`, which is the only place that reaches
all three shells where the store path gets expanded: deploy (`Agent._run_setup`,
ahead of `ProvisionSteps`), the run launcher (`agents/runner.py`), and every
tool launch (`via_file_watcher.ExecAsync` writes them atop each compile script).

Evidence, not absence: a sampler thread walks `ps` for the duration and records
every `apptainer` command line and every `squashfuse_ll` reader. A green run is
one where no sampled command line names a `.sandbox`, at least one names a
`.sif`, and a FUSE reader was actually seen -- the last because apptainer can
convert a SIF to a temporary sandbox on its own, which would otherwise pass as a
SIF run.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO/"src"))

from metasmith.constants import CONTAINER_TAG, AgentPaths
from metasmith.env import ContainerDef, Environment, Runtime
from metasmith.ops import agent as ops_agent
from metasmith.ops import runtime as ops_runtime

DEFAULT_BUNDLE = Path(
    "/home/tony/agentic_workspace/projects/metasmith/gui/scratch/gui-main"
    "/workflows/wooden-stallion"
)
# NOT this tree's own CONTAINER_TAG. The bundle's transform library calls
# `ExecutionContext.SourceOf`, which does not exist at 0.20.0 -- the step dies in
# the protocol with an AttributeError, `errorStrategy ignore` swallows it, and the
# workflow reports completion having produced only step 1. The image has to be the
# one the recipe was authored against; `--image` overrides.
DEFAULT_IMAGE = "docker://quay.io/hallamlab/metasmith:0.20.1-bf54d6f"
THIS_TREE_IMAGE = f"docker://quay.io/hallamlab/metasmith:{CONTAINER_TAG}"

# The two targets the workflow declares. Collected results name a target with an
# unnumbered directory; intermediates carry the step number.
WANT_TARGETS = ["pangenome-heatmap", "pangenome-ppanggolin_matrix"]


def log(msg: str):
    print(f"[driver] {msg}", flush=True)


def sh(cmd: str, check: bool = True) -> str:
    r = subprocess.run(["bash", "-c", cmd], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {cmd}\n{r.stdout}\n{r.stderr}")
    return r.stdout


# ----------------------------------------------------------------------------
# store paths
#
# `Environment.GetLocalPath()` returns a path with a literal `${APPTAINER_CACHEDIR:-...}`
# segment in it -- it is a shell expression expanded on the execution host, not a
# path this process can stat. Only `_cached_name()` is usable here, so the store
# root is spelled out by the caller and the two are joined by hand.
# ----------------------------------------------------------------------------

def cached_name(image: str) -> str:
    env = Environment(
        image=image, runtime=Runtime.APPTAINER,
        container=ContainerDef(cache=Path("/unused")),
    )
    return env._cached_name()


def store_listing(store: Path) -> dict:
    if not store.is_dir():
        return {"sifs": [], "sandboxes": [], "exists": False}
    return {
        "exists": True,
        "sifs": sorted(p.name for p in store.iterdir() if p.suffix == ".sif"),
        "sandboxes": sorted(p.name for p in store.iterdir() if p.name.endswith(".sandbox")),
    }


# ----------------------------------------------------------------------------
# the sampler
# ----------------------------------------------------------------------------

_APPTAINER_RE = re.compile(r"\bapptainer\b")
_SQUASHFUSE_RE = re.compile(r"squashfuse_ll")


class Sampler(threading.Thread):
    """Poll `ps` for apptainer invocations and FUSE readers.

    Sampling rather than log-scraping because `MakeRunCommand` emits a *run-time*
    ternary (`$(if [ -d <sandbox> ]; then ... )`) -- the log records the ternary,
    and only the process table records which branch it took.
    """

    def __init__(self, out_path: Path, period: float = 1.5):
        super().__init__(daemon=True)
        self.out_path = out_path
        self.period = period
        # not `_stop`: threading.Thread has a private `_stop()` method and join()
        # calls it, so shadowing it with an Event raises inside join
        self._halt = threading.Event()
        self.apptainer_cmds: set[str] = set()
        self.squashfuse_cmds: set[str] = set()
        self.samples = 0

    def run(self):
        with open(self.out_path, "w") as f:
            while not self._halt.is_set():
                try:
                    out = subprocess.run(
                        ["ps", "-eo", "pid,pgid,stat,args", "--no-headers"],
                        capture_output=True, text=True, timeout=20,
                    ).stdout
                except Exception:
                    self._halt.wait(self.period)
                    continue
                self.samples += 1
                for line in out.splitlines():
                    # the sampler's own ps and this script itself would otherwise
                    # match on their arguments
                    if "10_pangenome_forced_sif" in line: continue
                    if _SQUASHFUSE_RE.search(line):
                        rec = line.strip()
                        if rec not in self.squashfuse_cmds:
                            self.squashfuse_cmds.add(rec)
                            f.write(json.dumps({"kind": "squashfuse", "line": rec})+"\n")
                            f.flush()
                    elif _APPTAINER_RE.search(line):
                        rec = line.strip()
                        if rec not in self.apptainer_cmds:
                            self.apptainer_cmds.add(rec)
                            f.write(json.dumps({"kind": "apptainer", "line": rec})+"\n")
                            f.flush()
                self._halt.wait(self.period)

    def halt(self):
        self._halt.set()
        self.join(timeout=10)


# ----------------------------------------------------------------------------
# stages
# ----------------------------------------------------------------------------

def prepare(home: Path, bundle_src: Path, bundle_dst: Path, fresh: bool):
    if fresh:
        for p in (home, bundle_dst):
            if p.exists():
                log(f"removing previous [{p}]")
                shutil.rmtree(p)
    home.mkdir(parents=True, exist_ok=True)
    assert bundle_src.is_dir(), f"bundle not found [{bundle_src}]"
    assert (bundle_src/"task.yml").is_file(), f"[{bundle_src}] is not a task bundle"
    if not bundle_dst.exists():
        # copied rather than staged in place: staging reads the bundle and the
        # source is another scope's live GUI project
        log(f"copying bundle -> {bundle_dst}")
        sh(f'rsync -a --exclude "/runs/" "{bundle_src}/" "{bundle_dst}/"')
    log(f"bundle {sh(f'du -sh {bundle_dst}').split()[0]}")


def seed_agent_image(store: Path, image: str, source_sif: Path | None) -> Path:
    """Put the agent SIF where `MakeMaterialiseCommand`'s `[ ! -e ]` gate finds it."""
    store.mkdir(parents=True, exist_ok=True)
    target = store/f"{cached_name(image)}.sif"
    if target.exists():
        log(f"agent sif already seeded [{target.name}]")
        return target
    if source_sif is None:
        log("no --sif given; deploy will pull (this only works for a published tag)")
        return target
    assert source_sif.is_file(), f"source sif not found [{source_sif}]"
    try:
        os.link(source_sif, target)
        how = "hardlinked"
    except OSError:
        shutil.copy2(source_sif, target)
        how = "copied"
    # A SIF that apptainer cannot read is indistinguishable from a good one until
    # a container fails to start, and the `[ -e ]` gate above would have skipped
    # the pull that could have fixed it.
    sh(f"apptainer sif list {target} > /dev/null")
    log(f"{how} agent sif -> {target} ({target.stat().st_size/1e9:.2f} GB)")
    return target


def write_agent(agent_path: Path, home: Path, store: Path, image: str) -> list[str]:
    setup = [
        "#!/bin/bash",
        f"export APPTAINER_CACHEDIR={store}",
    ]
    info = ops_agent.save_agent(
        path=str(agent_path),
        home_uri=str(home),
        container=image,
        runtime="APPTAINER",
        setup_commands=setup,
        rootfs="sif",
    )
    log(f"agent [{info['name']}] home={info['home']} runtime={info['runtime']} rootfs=sif")
    for line in setup[1:]:
        log(f"  setup: {line}")
    return setup


# ----------------------------------------------------------------------------
# verification
# ----------------------------------------------------------------------------

def verify(
    *, home: Path, store: Path, results: Path, sampler: Sampler,
    wait_result: dict, run_key: str,
) -> tuple[bool, list[str]]:
    checks: list[tuple[str, bool, str]] = []

    def add(name: str, ok: bool, detail: str = ""):
        checks.append((name, ok, detail))

    # -- the run itself ------------------------------------------------------
    #
    # The sentinel is NOT a success signal on its own. Steps carry
    # `errorStrategy ignore`, so a step that dies in its protocol is retried,
    # given up on, and the run writes "run completed at" having produced nothing
    # from it -- which is exactly how a version-mismatched transform library read
    # as a green run once. Both halves have to be asserted.
    status = wait_result.get("status")
    add("workflow reached the completion sentinel", status == "completed", f"status={status}")

    run_root = home/AgentPaths.STAGED/run_key
    agent_logs = sorted((run_root/AgentPaths.INTERNALS).glob("logs.*/agent.log"))
    errored: list[str] = []
    tracebacks: list[str] = []
    if agent_logs:
        latest = agent_logs[-1]
        errored = [
            ln.strip() for ln in latest.read_text(errors="replace").splitlines()
            if "terminated with an error exit status" in ln
        ]
        tracebacks = sh(
            f'grep -rIl -- {json_quote("error while executing transform")} '
            f'"{run_root/AgentPaths.INTERNALS}" 2>/dev/null || true', check=False,
        ).strip().splitlines()
    add(
        "no step terminated with an error exit status",
        not errored,
        f"{len(errored)} failed attempt(s): "
        + "; ".join(sorted({e.split("Process `")[-1].split("`")[0] for e in errored})),
    )
    add(
        "no transform protocol raised",
        not tracebacks,
        f"{len(tracebacks)} step log(s) carry a protocol traceback",
    )

    # -- the store ------------------------------------------------------------
    listing = store_listing(store)
    add(
        "image store holds no .sandbox",
        listing["exists"] and not listing["sandboxes"],
        f"sandboxes={listing['sandboxes']}",
    )
    add(
        "image store holds a .sif per image",
        len(listing["sifs"]) >= 1,
        f"sifs={listing['sifs']}",
    )

    # -- what actually ran ----------------------------------------------------
    sandbox_cmds = [c for c in sampler.apptainer_cmds if ".sandbox" in c]
    sif_cmds = [c for c in sampler.apptainer_cmds if ".sif" in c]
    add(
        "no sampled apptainer invocation named a .sandbox",
        not sandbox_cmds,
        f"{len(sandbox_cmds)} offending line(s)",
    )
    add(
        "at least one sampled apptainer invocation named a .sif",
        bool(sif_cmds),
        f"{len(sif_cmds)} of {len(sampler.apptainer_cmds)} sampled apptainer lines",
    )
    # apptainer will silently convert a SIF to a temporary sandbox when it cannot
    # serve it; without a reader in the chain a green run proves nothing about the
    # squashfs path.
    add(
        "a squashfuse_ll reader was observed serving a rootfs",
        bool(sampler.squashfuse_cmds),
        f"{len(sampler.squashfuse_cmds)} distinct reader(s)",
    )

    # -- logs -----------------------------------------------------------------
    run_root = home/AgentPaths.STAGED/run_key
    hits: list[str] = []
    if run_root.is_dir():
        for pat in ("build --sandbox", "Converting SIF file to temporary sandbox"):
            out = sh(
                f'grep -rIl -- {json_quote(pat)} "{run_root}" 2>/dev/null || true',
                check=False,
            ).strip()
            if out:
                hits.append(f"{pat!r} in {len(out.splitlines())} file(s)")
    add("run logs mention no sandbox build or conversion", not hits, "; ".join(hits))

    # -- results --------------------------------------------------------------
    for target in WANT_TARGETS:
        d = results/target
        files = sorted(p.name for p in d.iterdir()) if d.is_dir() else []
        add(f"target [{target}] collected", bool(files), f"{files}")
    heatmaps = sorted((results/"pangenome-heatmap").glob("*.svg")) if (results/"pangenome-heatmap").is_dir() else []
    add("heatmap is an svg with content", bool(heatmaps) and heatmaps[0].stat().st_size > 1000,
        f"{[f'{p.name} ({p.stat().st_size} B)' for p in heatmaps]}")

    dangling = sh(f'find "{results}" -xtype l 2>/dev/null || true', check=False).strip()
    add("no dangling links in collected results", not dangling,
        f"{len(dangling.splitlines())} broken link(s)" if dangling else "")

    print()
    print("=" * 78)
    print("VERIFICATION")
    print("=" * 78)
    failures = []
    for name, ok, detail in checks:
        mark = "PASS" if ok else "FAIL"
        print(f"  [{mark}] {name}" + (f"  --  {detail}" if detail else ""))
        if not ok:
            failures.append(name)
    print("=" * 78)
    return (not failures), failures


def json_quote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


# ----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", default="sif-pangenome", help="scratch home name under ./scratch/")
    ap.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE, help="task bundle directory")
    ap.add_argument("--image", default=DEFAULT_IMAGE, help="agent container image")
    ap.add_argument(
        "--sif", type=Path,
        default=Path(f"{os.environ.get('APPTAINER_CACHEDIR', Path.home()/'.apptainer/cache')}")
        / f"{cached_name(DEFAULT_IMAGE)}.sif",
        help="a prebuilt SIF of --image to seed the fresh store with",
    )
    ap.add_argument("--keep", action="store_true", help="reuse an existing home instead of starting fresh")
    ap.add_argument("--timeout", type=float, default=5400.0, help="seconds to wait for the run")
    ap.add_argument("--skip-run", action="store_true", help="deploy and verify the store only")
    args = ap.parse_args()

    scratch = REPO/"scratch"
    home = (scratch/args.name).resolve()
    bundle = (scratch/f"{args.name}.bundle").resolve()
    results = (scratch/f"{args.name}.results").resolve()
    agent_path = (scratch/f"{args.name}.agent.yml").resolve()
    store = home/AgentPaths.CONTAINER_CACHE
    sampler_log = scratch/f"{args.name}.ps.jsonl"

    log(f"repo   {REPO}")
    log(f"home   {home}")
    log(f"store  {store}")
    log(f"image  {args.image}")

    prepare(home, args.bundle, bundle, fresh=not args.keep)
    if results.exists() and not args.keep:
        shutil.rmtree(results)

    seed_agent_image(store, args.image, args.sif if args.sif and args.sif.is_file() else None)
    write_agent(agent_path, home, store, args.image)

    sampler = Sampler(sampler_log)
    sampler.start()
    t0 = time.time()
    try:
        log("=== deploy ===")
        d = ops_agent.deploy(str(agent_path), assertive=False)
        log(f"deployed: {d}")
        log(f"store after deploy: {store_listing(store)}")

        if args.skip_run:
            log("--skip-run: stopping before stage")
            return 0

        log("=== stage ===")
        s = ops_runtime.stage(str(agent_path), str(bundle), on_exist="clear")
        key = s["task_key"]
        log(f"staged: {s}")
        log(f"store after stage: {store_listing(store)}")

        log("=== run ===")
        r = ops_runtime.run(str(agent_path), key)
        log(f"launched: {r}")

        log(f"=== wait (timeout {args.timeout:.0f}s) ===")
        w = ops_runtime.wait(str(agent_path), key, timeout_s=args.timeout, poll_s=5.0)
        log(f"wait: status={w.get('status')} elapsed={w.get('elapsed_s')}s run_dir={w.get('run_dir')}")
        for line in (w.get("tail") or [])[-25:]:
            print(f"    | {line}")

        log("=== collect ===")
        c = ops_runtime.collect(str(agent_path), key, str(results))
        for e in c.get("errors", []):
            log(f"  collect error: {e}")
        log(f"collected -> {results}")
    finally:
        sampler.halt()
        log(f"sampler: {sampler.samples} samples, "
            f"{len(sampler.apptainer_cmds)} apptainer lines, "
            f"{len(sampler.squashfuse_cmds)} squashfuse lines -> {sampler_log}")

    ok, failures = verify(
        home=home, store=store, results=results, sampler=sampler,
        wait_result=w, run_key=key,
    )
    log(f"total elapsed {time.time()-t0:.0f}s")
    if not ok:
        log(f"FAILED: {len(failures)} check(s) -> {failures}")
        return 1
    log("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
