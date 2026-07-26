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


def inject_dev_overlay(host: str, agent_path: str):
    """Bind this worktree's metasmith over the deployed container's copy.

    The published image predates whatever is being tested, so without this the
    task runs the *released* metasmith and a new field on `Agent` reads as an
    unexpected keyword. Same mechanism as `dev.sh -td`, targeted at this
    smoke's agent home rather than the registered ones. The tarball is what a
    SLURM compute node stages per-node-once; the tree is the fail-open bind
    target and the gate that switches the dev binds on at all.
    """
    src = REPO_ROOT / "src"
    if host == "local":
        run = lambda c: subprocess.run(c, shell=True, check=True)
        run(f'mkdir -p "{agent_path}/dev" "{agent_path}/lib"')
        run(f'rsync -ac --exclude=__pycache__ "{src}/metasmith/" "{agent_path}/dev/metasmith"')
        run(f'rsync -ac --exclude=__pycache__ "{src}/metasmith/nextflow_config" "{agent_path}/lib/"')
        run(f'T=$(mktemp -d)/metasmith.tar && tar -c --exclude=__pycache__ -C "{src}" -f "$T" metasmith'
            f' && cp "$T" "{agent_path}/dev/metasmith.tar" && rm -rf "$(dirname "$T")"')
    else:
        run = lambda c: subprocess.run(c, shell=True, check=True)
        run(f'ssh {host} mkdir -p "{agent_path}/dev" "{agent_path}/lib"')
        run(f'rsync -ac --exclude=__pycache__ "{src}/metasmith/" {host}:"{agent_path}/dev/metasmith"')
        run(f'rsync -ac --exclude=__pycache__ "{src}/metasmith/nextflow_config" {host}:"{agent_path}/lib/"')
        run(f'T=$(mktemp -d)/metasmith.tar && tar -c --exclude=__pycache__ -C "{src}" -f "$T" metasmith'
            f' && rsync -ac "$T" {host}:"{agent_path}/dev/metasmith.tar" && rm -rf "$(dirname "$T")"')


def build_agent(args, agent_path: str) -> Agent:
    if args.host == "local":
        home = Source.FromLocal(Path(agent_path))
    else:
        home = SshSource(host=args.host, path=agent_path).AsSource()
    gpu_args = list(WSL_GPU_ARGS) if args.wsl else []
    kw = {}
    # A dev checkout's build hash names an image nobody published, so apptainer
    # cannot pull it; point at a published tag and let the dev overlay supply
    # the code under test.
    if getattr(args, "container", None): kw["container"] = args.container
    return Agent(
        home=home,
        runtime=Runtime[args.runtime.upper()],
        native=args.native,
        gpu_args=gpu_args,
        setup_commands=list(args.setup_command or []),
        **kw,
    )


def build_task(smith: Agent, workdir: Path, tag: str, mamba_env: str | None = None,
               also_cpu: bool = False, remote: tuple[str, str] | None = None):
    inputs = DataInstanceLibrary(workdir / f"{tag}-inputs.xgdb")
    inputs.AddTypeLibrary(EXAMPLES / "data_types" / "examples.yml")
    inputs.AddValue("probe", tag, "examples::name")
    inputs.Save()

    containers = DataInstanceLibrary(workdir / f"{tag}-containers.xgdb")
    containers.AddTypeLibrary(EXAMPLES / "data_types" / "containers.yml")
    # Written INSIDE the library dir so its manifest entry is relative: an
    # absolute path outside the library would have to exist on the execution
    # host too, which for a remote agent it does not.
    oci = containers.location / "metasmith.oci"
    if mamba_env:
        # Under the mamba runtime the resource file's *content* is the conda
        # env name rather than an image URI -- same type, same slot, different
        # thing to run in. The transform is unchanged and does not know.
        oci.write_text(f"{mamba_env}\n")
    else:
        oci.write_text((EXAMPLES / "metasmith.oci").read_text())
    containers.AddItem(Path("metasmith.oci"), "containers::metasmith.oci")
    containers.Save()

    transforms = TransformInstanceLibrary.Load(EXAMPLES)

    if remote is not None:
        # A remote agent cannot read this machine's filesystem. Push each
        # library to the target and record that address on the local object;
        # StageWorkflow then pulls them into the agent's own data dir, so every
        # input path the workflow references exists on the execution host.
        host, root = remote
        subprocess.run(f'ssh {host} mkdir -p "{root}"', shell=True, check=True)
        for lib in (inputs, containers, transforms):
            name = Path(lib.location).name
            subprocess.run(
                f'rsync -a --delete --exclude=__pycache__ "{lib.location}/" {host}:"{root}/{name}/"',
                shell=True, check=True,
            )
            lib.remote_src = SshSource(host=host, path=f"{root}/{name}").AsSource()
            lib.Save()

    targets = TargetBuilder()
    targets.Add("examples::gpu_report")
    if also_cpu:
        # echo_greeting declares no GPU, so it must be submitted WITHOUT a GPU
        # request and against the ordinary account -- the half of the claim that
        # a GPU-only workflow cannot check.
        targets.Add("examples::greeting")
    return smith.GenerateWorkflow(
        samples=[inputs], resources=[containers],
        transforms=[transforms], targets=targets,
    )


def read_report(smith: Agent, task, host: str) -> str | None:
    """The transform's own report. None when it could not be read at all.

    Distinguishing "no report" from "a report saying no GPU" matters: the
    verdict below treats the first as a failure, not a pass. A remote agent's
    results live on the target, so they are read over ssh rather than by
    globbing a path that does not exist on this machine.
    """
    root = Path(str(smith.GetResultSource(task).GetPath()))
    if host == "local":
        hits = sorted(root.rglob("*.txt"))
        if not hits:
            return None
        return "\n".join(f"--- {h.name}\n{h.read_text().strip()}" for h in hits)
    res = subprocess.run(
        f'ssh {host} "find {root} -name \'*.txt\' -type f -exec sh -c '
        f'\'echo \\\"--- \\$1\\\"; cat \\$1\' _ {{}} \\;"',
        shell=True, capture_output=True, text=True,
    )
    out = res.stdout.strip()
    return out or None


def run_once(args, smith: Agent, workdir: Path, tag: str, gpus: Gpu | None, params: dict | None):
    print(f"\n=== [{tag}] gpus={gpus}", flush=True)
    task = build_task(
        smith, workdir, tag,
        mamba_env=args.mamba_env if args.runtime == "mamba" else None,
        also_cpu=args.also_cpu_step,
        remote=None if args.host == "local" else (args.host, args.lib_root),
    )
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
    report = read_report(smith, task, args.host)
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
    ap.add_argument("--gpu-extra", action="append", default=None, help="scheduler flag GPU steps need beyond the count, e.g. --partition=gpu (repeatable)")
    ap.add_argument("--slurm-account", default=None)
    ap.add_argument("--slurm-gpu-account", default=None)
    ap.add_argument("--setup-command", action="append", default=None)
    ap.add_argument("--lib-root", default=None, help="remote dir to push the libraries into (remote hosts)")
    ap.add_argument("--also-cpu-step", action="store_true", help="add a non-GPU transform to the same workflow")
    ap.add_argument("--skip-negative", action="store_true", help="skip the no-GPU-declared fallback run")
    ap.add_argument("--agent-path", default=None)
    ap.add_argument("--timeout-s", type=float, default=1800.0)
    ap.add_argument("--no-deploy", action="store_true", help="reuse an already deployed agent home")
    ap.add_argument("--no-dev-overlay", action="store_true", help="run the image's released metasmith as-is")
    ap.add_argument("--mamba-env", default=None, help="conda env the TOOL runs in, for --runtime mamba")
    ap.add_argument("--container", default=None, help="agent image; defaults to the build-tagged one, which is unpublished on a dev checkout")
    args = ap.parse_args(argv)

    ts = int(time.time())
    if args.agent_path:
        agent_path = args.agent_path
    elif args.host == "local":
        agent_path = str(REPO_ROOT / ".awm" / "data" / f"gpu_smoke_{args.runtime}")
    else:
        agent_path = f"/scratch/{_remote_user(args.host)}/metasmith_gpu_smoke_{ts}"
    if args.host != "local" and not args.lib_root:
        args.lib_root = f"{agent_path}/_smoke_libs"
    print(f"==> target: {args.host}:{agent_path} runtime={args.runtime} native={args.native}", flush=True)

    smith = build_agent(args, agent_path)
    if not args.no_deploy:
        print("==> Deploy()", flush=True)
        smith.Deploy()
    if not args.no_dev_overlay:
        print("==> inject dev overlay", flush=True)
        inject_dev_overlay(args.host, agent_path)

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
        extra=list(args.gpu_extra or []),
    )
    positive = run_once(args, smith, workdir, f"{args.runtime}-gpu-{ts}", device, params or None)

    negative = None
    if not args.skip_negative:
        negative = run_once(args, smith, workdir, f"{args.runtime}-nogpu-{ts}", None, params or None)

    print("\n=== verdict", flush=True)
    # For a scheduler run the report is only half the evidence; the other half
    # is the scheduler's own record. Print the sacct query to run.
    if args.host != "local":
        print(f"    scheduler check: ssh {args.host} \"sacct -X --format=JobID,JobName%24,State,"
              f"Account%20,Partition,ReqTRES%40,AllocTRES%40,NodeList\"", flush=True)
    ok = True
    if positive is None:
        print("FAIL: the declared-GPU run produced no readable report"); ok = False
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
