"""HPC vanilla-deploy smoke runner.

Usage:
    python -m main.local_mock.smoke_hpc_deploy --host {sockeye|fir|mira}

Mirrors the SSH-deploy shape documented at
docs/source/setup/deployment.rst lines 28-53. Drives a single round-trip:

  Deploy -> Generate -> Stage -> Run -> Wait -> Result

against the agnostic `examples/` library at repo root. No HPC-specific
helpers, no workarounds. If a host needs anything beyond
`module load apptainer` to succeed, that is a documentation gap to file,
not to paper over here.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

from metasmith.python_api import (
    Agent, SshSource, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"

HOST_PROFILES = {
    "sockeye": dict(scratch_root="/scratch", setup_commands=["module load apptainer"]),
    "fir":     dict(scratch_root="/scratch", setup_commands=["module load apptainer"]),
    "mira":    dict(scratch_root=None,       setup_commands=[]),
}


def resolve_remote_user(host: str) -> str:
    res = subprocess.run(
        ["ssh", host, "echo $USER"],
        capture_output=True, text=True, check=True,
    )
    return res.stdout.strip()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", choices=sorted(HOST_PROFILES), required=True)
    ap.add_argument("--timeout-s", type=float, default=900.0)
    args = ap.parse_args(argv)

    profile = HOST_PROFILES[args.host]
    ts = int(time.time())

    user = resolve_remote_user(args.host)
    if profile["scratch_root"]:
        agent_path = f"{profile['scratch_root']}/{user}/metasmith_smoke_{ts}"
    else:
        agent_path = f"/home/{user}/metasmith_smoke_{ts}"
    print(f"==> smoke target: {args.host}:{agent_path}", flush=True)

    home = SshSource(host=args.host, path=agent_path).AsSource()
    smith = Agent(
        home=home,
        runtime=Runtime.APPTAINER,
        setup_commands=profile["setup_commands"],
    )

    print("==> Deploy()", flush=True)
    smith.Deploy()

    runs_dir = REPO_ROOT / ".awm" / "data" / "smoke_results"
    runs_dir.mkdir(parents=True, exist_ok=True)

    inputs = DataInstanceLibrary(runs_dir / f"{args.host}-{ts}-inputs.xgdb")
    inputs.AddTypeLibrary(EXAMPLES / "data_types" / "examples.yml")
    inputs.AddValue("smoke", args.host, "examples::name")
    inputs.Save()

    containers = DataInstanceLibrary(runs_dir / f"{args.host}-{ts}-containers.xgdb")
    containers.AddTypeLibrary(EXAMPLES / "data_types" / "containers.yml")
    containers.AddItem(EXAMPLES / "metasmith.oci", "containers::metasmith.oci")
    containers.Save()

    transforms = TransformInstanceLibrary.Load(EXAMPLES)
    targets = TargetBuilder()
    targets.Add("examples::greeting")

    print("==> GenerateWorkflow()", flush=True)
    task = smith.GenerateWorkflow(
        samples=[inputs],
        resources=[containers],
        transforms=[transforms],
        targets=targets,
    )
    if not task.ok:
        print(f"!! plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        return 3

    print(f"==> task key: {task.GetKey()}", flush=True)
    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(task)

    print(f"==> WaitForWorkflow (timeout {args.timeout_s}s)", flush=True)
    result = smith.WaitForWorkflow(task, timeout_s=args.timeout_s, poll_s=10.0)
    print(f"==> status: {result['status']} after {result['elapsed_s']:.1f}s", flush=True)
    for line in result["tail"]:
        print(f"    {line}")

    if result["status"] != "completed":
        return 2

    src = smith.GetResultSource(task)
    print(f"==> result source: {src.GetPath()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
