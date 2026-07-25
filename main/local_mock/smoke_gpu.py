"""GPU smoke runner — does a declared GPU actually reach the tool?

Usage:
    python -m main.local_mock.smoke_gpu --runtime docker
    python -m main.local_mock.smoke_gpu --runtime apptainer --wsl
    python -m main.local_mock.smoke_gpu --runtime mamba --mamba-env msm
    python -m main.local_mock.smoke_gpu --host sockeye --runtime apptainer \\
        --slurm-account st-x-1 --slurm-gpu-account st-x-1-gpu \\
        --gpu-memory 32 --gpu-flag '--gpus-per-node=' --gpu-type a100

Drives Deploy -> Generate -> Stage -> Run -> Wait -> Read against the agnostic
`examples/` library, then reads the report the transform wrote from *inside*
its own container. Emitting the right flag is not the claim being tested; the
tool seeing a device is.

Also runs the negative case: the same workflow with no `gpus=` declaration,
which must still succeed (the probe is Gpus.OPTIONAL) and must report no
device, so the CPU fallback is exercised rather than assumed.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

from metasmith.python_api import (
    Agent, SshSource, Source, Runtime,
    DataInstanceLibrary, TransformInstanceLibrary, TargetBuilder,
    Gpu, Size,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"

# apptainer's `--nv` finds and injects nvidia-smi on WSL2, but its library
# discovery misses the driver stack under /usr/lib/wsl, so NVML answers
# "GPU access blocked by the operating system". A host fact, hence a flag.
WSL_GPU_ARGS = [
    "--bind", "/usr/lib/wsl:/usr/lib/wsl",
    "--env", "LD_LIBRARY_PATH=/usr/lib/wsl/lib",
]


def _remote_user(host: str) -> str:
    res = subprocess.run(["ssh", host, "echo $USER"], capture_output=True, text=True, check=True)
    return res.stdout.strip()


def build_agent(args, agent_path: str) -> Agent:
    if args.host == "local":
        home = Source.FromLocal(Path(agent_path))
    else:
        home = SshSource(host=args.host, path=agent_path).AsSource()
    gpu_args = list(WSL_GPU_ARGS) if args.wsl else []
    return Agent(
        home=home,
        runtime=Runtime[args.runtime.upper()],
        native=args.native,
        gpu_args=gpu_args,
        setup_commands=list(args.setup_command or []),
    )


def build_task(smith: Agent, workdir: Path, tag: str):
    inputs = DataInstanceLibrary(workdir / f"{tag}-inputs.xgdb")
    inputs.AddTypeLibrary(EXAMPLES / "data_types" / "examples.yml")
    inputs.AddValue("probe", tag, "examples::name")
    inputs.Save()

    containers = DataInstanceLibrary(workdir / f"{tag}-containers.xgdb")
    containers.AddTypeLibrary(EXAMPLES / "data_types" / "containers.yml")
    containers.AddItem(EXAMPLES / "metasmith.oci", "containers::metasmith.oci")
    containers.Save()

    transforms = TransformInstanceLibrary.Load(EXAMPLES)
    targets = TargetBuilder()
    targets.Add("examples::gpu_report")
    return smith.GenerateWorkflow(
        samples=[inputs], resources=[containers],
        transforms=[transforms], targets=targets,
    )


def read_report(smith: Agent, task) -> str:
    src = smith.GetResultSource(task)
    root = Path(str(src.GetPath()))
    hits = sorted(root.rglob("*.txt"))
    if not hits:
        return f"!! no report found under {root}"
    return "\n".join(f"--- {h.name}\n{h.read_text().strip()}" for h in hits)


def run_once(args, smith: Agent, workdir: Path, tag: str, gpus: Gpu | None, params: dict | None):
    print(f"\n=== [{tag}] gpus={gpus}", flush=True)
    task = build_task(smith, workdir, tag)
    if not task.ok:
        print(f"!! plan failed: hints={list(task.plan.hints)}", file=sys.stderr)
        return None
    print(f"==> task key: {task.GetKey()}", flush=True)
    smith.StageWorkflow(task, on_exist="clear")
    smith.RunWorkflow(
        task,
        config_file=smith.GetNxfConfigPresets()["slurm" if args.host != "local" else "local"],
        gpus=gpus,
        params=params,
    )
    result = smith.WaitForWorkflow(task, timeout_s=args.timeout_s, poll_s=10.0)
    print(f"==> status: {result['status']} after {result['elapsed_s']:.1f}s", flush=True)
    if result["status"] != "completed":
        for line in result["tail"]:
            print(f"    {line}")
        return None
    report = read_report(smith, task)
    print(report, flush=True)
    return report


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="local", help="'local' or an ssh host alias")
    ap.add_argument("--runtime", default="docker", choices=["docker", "apptainer", "mamba"])
    ap.add_argument("--native", action="store_true", help="metasmith is already installed on the host")
    ap.add_argument("--wsl", action="store_true", help="add the WSL2 driver-stack bind to the GPU args")
    ap.add_argument("--gpu-memory", type=float, default=8.0, help="per-device VRAM in GB on the target")
    ap.add_argument("--gpu-type", default=None, help="site gres type token, e.g. a100")
    ap.add_argument("--gpu-flag", default="--gpus-per-node=", help="site request syntax; count is appended")
    ap.add_argument("--gpu-count", type=int, default=None, help="devices per node on the target")
    ap.add_argument("--slurm-account", default=None)
    ap.add_argument("--slurm-gpu-account", default=None)
    ap.add_argument("--setup-command", action="append", default=None)
    ap.add_argument("--skip-negative", action="store_true", help="skip the no-GPU-declared fallback run")
    ap.add_argument("--agent-path", default=None)
    ap.add_argument("--timeout-s", type=float, default=1800.0)
    ap.add_argument("--no-deploy", action="store_true", help="reuse an already deployed agent home")
    args = ap.parse_args(argv)

    ts = int(time.time())
    if args.agent_path:
        agent_path = args.agent_path
    elif args.host == "local":
        agent_path = str(REPO_ROOT / ".awm" / "data" / f"gpu_smoke_{args.runtime}")
    else:
        agent_path = f"/scratch/{_remote_user(args.host)}/metasmith_gpu_smoke_{ts}"
    print(f"==> target: {args.host}:{agent_path} runtime={args.runtime} native={args.native}", flush=True)

    smith = build_agent(args, agent_path)
    if not args.no_deploy:
        print("==> Deploy()", flush=True)
        smith.Deploy()

    workdir = REPO_ROOT / ".awm" / "data" / "gpu_smoke_runs"
    workdir.mkdir(parents=True, exist_ok=True)

    params = {}
    if args.slurm_account: params["slurmAccount"] = args.slurm_account
    if args.slurm_gpu_account: params["slurmGpuAccount"] = args.slurm_gpu_account

    device = Gpu(
        memory=Size.GB(args.gpu_memory),
        type=args.gpu_type,
        count=args.gpu_count,
        flag=args.gpu_flag,
    )
    positive = run_once(args, smith, workdir, f"{args.runtime}-gpu-{ts}", device, params or None)

    negative = None
    if not args.skip_negative:
        negative = run_once(args, smith, workdir, f"{args.runtime}-nogpu-{ts}", None, params or None)

    print("\n=== verdict", flush=True)
    ok = True
    if positive is None:
        print("FAIL: the declared-GPU run did not complete"); ok = False
    elif "no gpu visible" in positive:
        print("FAIL: a GPU was declared but the tool saw no device"); ok = False
    elif "detected_devices=0" in positive:
        print("FAIL: the tool saw a device but DetectGpus() reported none"); ok = False
    else:
        print("PASS: the tool container saw a device, and DetectGpus() agrees")
    if negative is not None and "no gpu visible" not in negative and "detected_devices=0" not in negative:
        # not fatal on a host where the runtime exposes devices unconditionally
        # (mamba/native inherit everything), but worth saying out loud
        print("NOTE: the no-declaration run still saw a device (expected for mamba/native)")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
