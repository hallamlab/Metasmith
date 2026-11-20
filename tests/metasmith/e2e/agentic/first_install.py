"""First-time install test: a context-free agent installs metasmith from the docs alone and runs one sample on a Slurm cluster."""
from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from tests.metasmith.e2e.agentic.drivers.claude import ClaudeDriver
from tests.metasmith.e2e.agentic.harness import jail
from tests.metasmith.e2e.agentic.harness.bootstrap_env import ensure_bootstrap_env
from tests.metasmith.e2e.agentic.harness.loop import LoopBudgets, ralph_loop
from tests.metasmith.e2e.agentic.harness.sandbox import _hardlink_channel

REPO = Path(__file__).resolve().parents[4]
DOCS = REPO / "docs" / "metasmith" / "source"
HOST = "nibi"
ACCOUNT = "def-shallam_cpu"
REMOTE_ROOT = "/scratch/phyberos/msm_e2e"
_ENA = "https://ftp.sra.ebi.ac.uk/vol1/fastq/SRR965/008/SRR9658108/SRR9658108"
SAMPLES = {
    "sra": "SRA run SRR9658108",
    "ena": f"the paired reads of SRA run SRR9658108, which ENA serves as {_ENA}_1.fastq.gz and {_ENA}_2.fastq.gz",
}
PLAYWRIGHT_MCP = Path.home() / "msm_e2e" / "tools" / "playwright-mcp"
CHROMIUM = Path.home() / ".cache" / "ms-playwright" / "chromium-1248" / "chrome-linux64" / "chrome"

GOALS = {
    "gui": (
        "You are a new metasmith user. Using only the docs in ./docs, install metasmith and start its web GUI. "
        "Then use the GUI the way a person would, through your browser tools, which drive a real browser. "
        "Do not call the GUI's HTTP API with curl and do not write Python for these steps. "
        "Through the GUI, deploy an agent to the cluster reachable as `ssh {host}` (Slurm account {account}), "
        "with its home under {remote}. Run the metagenomics_from_paired_reads workflow "
        "without its metabuli target on {sample}. Collect the results back to ./results."
    ),
    "api": (
        "You are a new metasmith user. Using only the docs in ./docs, install metasmith and use its Python API "
        "as the docs describe. Deploy an agent to the cluster reachable as `ssh {host}` (Slurm account {account}), "
        "with its home under {remote}. Run the metagenomics_from_paired_reads workflow "
        "without its metabuli target on {sample}. Collect the results back to ./results. "
        "For each API call you make, note in STEPS.md whether the docs made clear what it does, "
        "and whether it did what the docs said."
    ),
    "full": (
        "You are a new metasmith user. Install metasmith using only the docs in ./docs. "
        "Deploy an agent to the cluster reachable as `ssh {host}` (Slurm account {account}), "
        "with its home under {remote}. Run the metagenomics_from_paired_reads workflow "
        "without its metabuli target on {sample}. Collect the results back to ./results."
    ),
    "dry": (
        "You are a new metasmith user. Using only the docs in ./docs, install metasmith "
        "and print its version. Then run `ssh {host} hostname`. Do nothing else."
    ),
}

PROTOCOL = """
## How to work

- Your working directory is {sandbox}; it is your computer's home for this task. Conda already keeps its environments and package cache inside it. Do not read files outside it, apart from the cluster through `ssh {host}`.
- You may be stopped and restarted. Each time you start, first read PROGRESS.md, STEPS.md and the tail of any log you left. Do not repeat a step that already failed the same way. Keep PROGRESS.md short and current: what is done, what is running (job IDs, PIDs), what you tried that failed, what is next.
- Before each action, append to STEPS.md a numbered entry with: the command, and the doc file and section that told you to run it (or "no doc: guess"). After the action, append its outcome. If the docs were unclear, if you had to guess, or if you had to work around a problem, say so in that entry and label it AMBIGUITY, GUESS or WORKAROUND. Label a failure ERROR.
- One shell command may run for up to 10 minutes. Start anything longer, and any server, detached: `nohup setsid <command> > <log> 2>&1 &`. A detached process keeps running after your session ends. Write its PID and log path in PROGRESS.md.
- While something long runs, check it, then `sleep 540` and check again rather than ending your turn early.
- When you are done or stuck, write REPORT.md: what you achieved, then one line per issue, each labelled ERROR, AMBIGUITY, GUESS or WORKAROUND, naming the doc section involved.
- Then write CONTROL.json: `{{"action": "done"}}` if the goal is met, or `{{"action": "give_up", "reason": "<one line>"}}` if you cannot go further.
"""


def build_prompt(goal: str, sandbox: Path, remote: str, sample: str) -> str:
    text = GOALS[goal].format(host=HOST, account=ACCOUNT, remote=remote, sample=SAMPLES[sample])
    return text + "\n" + PROTOCOL.format(sandbox=sandbox, host=HOST)


def remote_exists(path: str) -> bool:
    r = subprocess.run(["ssh", HOST, f"test -e {shlex.quote(path)}"], capture_output=True, text=True)
    if r.returncode == 255:
        raise SystemExit(f"ssh {HOST} failed (is the master open?): {r.stderr.strip()}")
    return r.returncode == 0


def stage_sif(sif: Path, tag: str, remote: str) -> None:
    """Put an unpublished image where the agent's pull would land, as RELEASE_PROTOCOL.md describes."""
    store = f"{remote}/container_images"
    dst = f"{store}/docker..quay.io_hallamlab_metasmith..{tag}.sif"
    subprocess.run(["ssh", HOST, f"mkdir -p {store}"], check=True)
    subprocess.run(["rsync", "-a", str(sif), f"{HOST}:{dst}"], check=True)
    subprocess.run(["ssh", HOST, f"module load apptainer && apptainer exec --no-home --cleanenv {dst} true && touch {dst}.verified"],
                   check=True)


def ssh_config_text(sandbox: Path) -> tuple[str, str]:
    # The block lands in the sandbox's ~/.ssh/config, which the GUI edits. ssh itself
    # reads the passwd home, so the real path holds only an Include of that file.
    resolved = subprocess.run(["ssh", "-G", HOST], capture_output=True, text=True, check=True).stdout
    opts = dict(line.split(" ", 1) for line in resolved.splitlines() if " " in line)
    return (
        f"Host {HOST}\n"
        f"    HostName {opts['hostname']}\n"
        f"    User {opts['user']}\n"
        f"    ControlMaster no\n"
        f"    ControlPath {opts['controlpath']}\n"
        f"    ProxyCommand {sandbox}/.no-fresh-login\n"
        f"    BatchMode yes\n"
        f"    ServerAliveInterval 60\n"
    ), opts["controlpath"]


def build_sandbox(sandbox: Path, docs: Path, channel: Path | None) -> tuple[list[jail.BindSpec], dict[str, str]]:
    home = sandbox / "home"
    for d in (home, sandbox / "tmp", sandbox / "envs", sandbox / "pkgs"):
        d.mkdir(parents=True)
    shutil.copytree(docs, sandbox / "docs")
    (sandbox / "PROGRESS.md").touch()
    condarc = ("channels:\n  - conda-forge\n  - bioconda\n"
               f"envs_dirs:\n  - {sandbox}/envs\npkgs_dirs:\n  - {sandbox}/pkgs\n")
    if channel:
        # `-c hallamlab` resolves to the local build, so the docs' install command installs the candidate.
        _hardlink_channel(channel, sandbox / ".channels" / "hallamlab")
        condarc += f"custom_channels:\n  hallamlab: file://{sandbox}/.channels\n"
    (home / ".condarc").write_text(condarc)
    guard = sandbox / ".no-fresh-login"
    guard.write_text("#!/bin/sh\necho 'ssh: no connection to the cluster; ask the operator to reconnect' >&2\nexit 1\n")
    guard.chmod(0o755)
    ssh_conf, control_path = ssh_config_text(sandbox)
    user_ssh = home / ".ssh" / "config"
    user_ssh.parent.mkdir(mode=0o700)
    user_ssh.write_text(ssh_conf)
    user_ssh.chmod(0o600)
    (sandbox / ".ssh_include").write_text(f"Include {user_ssh}\n")

    boot = ensure_bootstrap_env()
    claude_dir = Path(shutil.which("claude")).resolve().parent
    real_home = Path(os.path.expanduser("~"))
    node_dir = Path(shutil.which("node", path=f"{real_home}/.local/bin:{os.environ['PATH']}")).resolve().parents[1]
    binds = jail.default_binds(sandbox, home, extra_ro=[boot, claude_dir, node_dir, PLAYWRIGHT_MCP, CHROMIUM.parents[1]])
    binds += [
        # ssh reads the passwd home, not $HOME, so the config lands at the real path.
        jail.BindSpec(str(sandbox / ".ssh_include"), str(real_home / ".ssh" / "config"), "ro"),
        jail.BindSpec(str(Path(control_path).parent), str(Path(control_path).parent), "ro"),
    ]
    env = {
        "HOME": str(home),
        "USER": os.environ.get("USER", "user"),
        "PATH": f"{boot}/bin:{claude_dir}:/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "TERM": "dumb",
        "TMPDIR": "/tmp",
        "CONDARC": str(home / ".condarc"),
        "MAMBA_ROOT_PREFIX": str(sandbox),
    }
    return binds, env


def write_browser_mcp(sandbox: Path, path: Path) -> None:
    node = shutil.which("node", path=f"{Path.home()}/.local/bin:{os.environ['PATH']}")
    server = {
        "command": str(Path(node).resolve()),
        "args": [str(PLAYWRIGHT_MCP / "node_modules" / "@playwright" / "mcp" / "cli.js"),
                 "--headless", "--isolated", "--no-sandbox",
                 "--executable-path", str(CHROMIUM), "--output-dir", str(sandbox / "browser")],
    }
    path.write_text(json.dumps({"mcpServers": {"browser": server}}, indent=2))


def write_jailed_claude(sandbox: Path, wrapper: Path, binds, env) -> None:
    inner = ["env", "-i", *(f"{k}={v}" for k, v in env.items()), str(Path(shutil.which("claude")).resolve())]
    argv = jail.build_bwrap_argv(
        inner, binds=binds, bwrap=jail.require_bwrap(), chdir=str(sandbox),
        tmp_bind=jail.BindSpec(str(sandbox / "tmp"), "/tmp", "rw"), keep_children=True,
    )
    wrapper.write_text("#!/bin/sh\nexec " + shlex.join(argv) + ' "$@"\n')
    wrapper.chmod(0o755)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", type=int, required=True, help="run number N; names run-N locally and on the cluster")
    ap.add_argument("--goal", choices=sorted(GOALS), default="gui")
    ap.add_argument("--local-root", type=Path, default=Path.home() / "msm_e2e")
    ap.add_argument("--docs", type=Path, default=DOCS)
    ap.add_argument("--sample", choices=sorted(SAMPLES), default="sra")
    ap.add_argument("--channel", type=Path, help="local conda channel (a conda_build dir) to serve as hallamlab")
    ap.add_argument("--sif", type=Path, help="local metasmith.sif to stage on the cluster; needs --image-tag")
    ap.add_argument("--image-tag", help="the image tag the candidate pulls, e.g. 0.25.1-abc1234")
    ap.add_argument("--model", default="haiku")
    ap.add_argument("--max-iters", type=int, default=20)
    ap.add_argument("--iter-timeout", type=float, default=4 * 3600.0)
    ap.add_argument("--max-tokens", type=int, default=60_000_000)
    ap.add_argument("--usd-per-iter", type=float, default=10.0)
    args = ap.parse_args(argv)

    # The agent's detached processes outlive each pass, so the whole run lives in one
    # systemd scope: stopping the scope is the only cleanup that reaches all of them.
    unit = f"msm-e2e-run-{args.run}"
    if os.environ.get("MSM_E2E_SCOPE") != unit:
        os.environ["MSM_E2E_SCOPE"] = unit
        os.execvp("systemd-run", ["systemd-run", "--user", "--scope", "-q", f"--unit={unit}",
                                  sys.executable, "-m", "tests.metasmith.e2e.agentic.first_install",
                                  *(argv if argv is not None else sys.argv[1:])])

    run_dir = args.local_root / f"run-{args.run}"
    remote = f"{REMOTE_ROOT}/run-{args.run}"
    if run_dir.exists():
        raise SystemExit(f"{run_dir} exists; read and delete it before starting run {args.run}")
    if args.goal != "dry" and remote_exists(remote):
        raise SystemExit(f"{HOST}:{remote} exists; read and delete it before starting run {args.run}")

    if args.sif:
        stage_sif(args.sif, args.image_tag, remote)

    sandbox = run_dir / "sandbox"
    binds, env = build_sandbox(sandbox, args.docs, args.channel)
    wrapper = run_dir / "jailed-claude"
    write_jailed_claude(sandbox, wrapper, binds, env)
    (run_dir / "harness.pid").write_text(str(os.getpid()))
    if args.goal != "dry":
        with (run_dir / "watch.out").open("w") as log:
            subprocess.Popen([sys.executable, "-m", "tests.metasmith.e2e.agentic.watch_remote", "--run", str(args.run)],
                             stdout=log, stderr=subprocess.STDOUT)

    # --strict-mcp-config with no --mcp-config drops the account's claude.ai connectors.
    # --mcp-config is variadic, so a named flag must follow it before the prompt.
    mcp = ["--strict-mcp-config"]
    if args.goal == "gui":
        write_browser_mcp(sandbox, sandbox / ".browser-mcp.json")
        mcp = ["--mcp-config", str(sandbox / ".browser-mcp.json"), *mcp]
    driver = ClaudeDriver(model=args.model, bin=str(wrapper), jail=False, timeout_s=args.iter_timeout,
                          extra_argv=mcp)
    result = ralph_loop(
        driver=driver,
        prompt=build_prompt(args.goal, sandbox, remote, args.sample),
        sandbox=sandbox,
        budgets=LoopBudgets(max_iters=args.max_iters, max_tokens=args.max_tokens,
                            max_tokens_per_iter=args.max_tokens, max_usd_per_iter=args.usd_per_iter),
        log_dir=run_dir / "logs",
    )
    summary = {
        "outcome": result.outcome.value,
        "iterations": result.iterations,
        "tokens_used": result.tokens_used,
        "control": result.terminal_control.payload if result.terminal_control else None,
        "exit_codes": [r.exit_code for r in result.iter_results],
    }
    (run_dir / "result.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2), flush=True)
    subprocess.run(["systemctl", "--user", "stop", f"{unit}.scope"])
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
