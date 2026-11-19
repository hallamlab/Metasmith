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

REPO = Path(__file__).resolve().parents[4]
DOCS = REPO / "docs" / "metasmith" / "source"
HOST = "nibi"
ACCOUNT = "def-shallam_cpu"
REMOTE_ROOT = "/scratch/phyberos/msm_e2e"
SAMPLE = "SRR9658108"

GOALS = {
    "full": (
        "You are a new metasmith user. Install metasmith using only the docs in ./docs. "
        "Deploy an agent to the cluster reachable as `ssh {host}` (Slurm account {account}), "
        "with its home under {remote}. Run the metagenomics_from_paired_reads workflow "
        "without its metabuli target on SRA run {sample}. Collect the results back to ./results."
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
- Your session ends when your turn ends, and every process you started on this machine ends with it. Run a long local command in the foreground; it may take up to 10 minutes per call.
- When a cluster job is still running, check it, then `sleep 600` and check again rather than ending your turn early.
- When you are done or stuck, write REPORT.md: what you achieved, then one line per issue, each labelled ERROR, AMBIGUITY, GUESS or WORKAROUND, naming the doc section involved.
- Then write CONTROL.json: `{{"action": "done"}}` if the goal is met, or `{{"action": "give_up", "reason": "<one line>"}}` if you cannot go further.
"""


def build_prompt(goal: str, sandbox: Path, remote: str) -> str:
    text = GOALS[goal].format(host=HOST, account=ACCOUNT, remote=remote, sample=SAMPLE)
    return text + "\n" + PROTOCOL.format(sandbox=sandbox, host=HOST)


def remote_exists(path: str) -> bool:
    r = subprocess.run(["ssh", HOST, f"test -e {shlex.quote(path)}"], capture_output=True, text=True)
    if r.returncode == 255:
        raise SystemExit(f"ssh {HOST} failed (is the master open?): {r.stderr.strip()}")
    return r.returncode == 0


def ssh_config_text(sandbox: Path) -> tuple[str, str]:
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


def build_sandbox(sandbox: Path, docs: Path) -> tuple[list[jail.BindSpec], dict[str, str]]:
    home = sandbox / "home"
    for d in (home, sandbox / "tmp", sandbox / "envs", sandbox / "pkgs"):
        d.mkdir(parents=True)
    shutil.copytree(docs, sandbox / "docs")
    (sandbox / "PROGRESS.md").touch()
    (home / ".condarc").write_text(
        "channels:\n  - conda-forge\n  - bioconda\n"
        f"envs_dirs:\n  - {sandbox}/envs\npkgs_dirs:\n  - {sandbox}/pkgs\n"
    )
    guard = sandbox / ".no-fresh-login"
    guard.write_text("#!/bin/sh\necho 'ssh: no connection to the cluster; ask the operator to reconnect' >&2\nexit 1\n")
    guard.chmod(0o755)
    ssh_conf, control_path = ssh_config_text(sandbox)
    (sandbox / ".ssh_config").write_text(ssh_conf)

    boot = ensure_bootstrap_env()
    claude_dir = Path(shutil.which("claude")).resolve().parent
    real_home = Path(os.path.expanduser("~"))
    binds = jail.default_binds(sandbox, home, extra_ro=[boot, claude_dir])
    binds += [
        # ssh reads the passwd home, not $HOME, so the config lands at the real path.
        jail.BindSpec(str(sandbox / ".ssh_config"), str(real_home / ".ssh" / "config"), "ro"),
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


def write_jailed_claude(sandbox: Path, wrapper: Path, binds, env) -> None:
    inner = ["env", "-i", *(f"{k}={v}" for k, v in env.items()), str(Path(shutil.which("claude")).resolve())]
    argv = jail.build_bwrap_argv(
        inner, binds=binds, bwrap=jail.require_bwrap(), chdir=str(sandbox),
        tmp_bind=jail.BindSpec(str(sandbox / "tmp"), "/tmp", "rw"),
    )
    wrapper.write_text("#!/bin/sh\nexec " + shlex.join(argv) + ' "$@"\n')
    wrapper.chmod(0o755)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run", type=int, required=True, help="run number N; names run-N locally and on the cluster")
    ap.add_argument("--goal", choices=sorted(GOALS), default="full")
    ap.add_argument("--local-root", type=Path, default=Path.home() / "msm_e2e")
    ap.add_argument("--docs", type=Path, default=DOCS)
    ap.add_argument("--model", default="haiku")
    ap.add_argument("--max-iters", type=int, default=60)
    ap.add_argument("--iter-timeout", type=float, default=2700.0)
    ap.add_argument("--max-tokens", type=int, default=60_000_000)
    ap.add_argument("--usd-per-iter", type=float, default=3.0)
    args = ap.parse_args(argv)

    run_dir = args.local_root / f"run-{args.run}"
    remote = f"{REMOTE_ROOT}/run-{args.run}"
    if run_dir.exists():
        raise SystemExit(f"{run_dir} exists; read and delete it before starting run {args.run}")
    if args.goal == "full" and remote_exists(remote):
        raise SystemExit(f"{HOST}:{remote} exists; read and delete it before starting run {args.run}")

    sandbox = run_dir / "sandbox"
    binds, env = build_sandbox(sandbox, args.docs)
    wrapper = run_dir / "jailed-claude"
    write_jailed_claude(sandbox, wrapper, binds, env)
    (run_dir / "harness.pid").write_text(str(os.getpid()))

    # --strict-mcp-config with no --mcp-config drops the account's claude.ai connectors.
    driver = ClaudeDriver(model=args.model, bin=str(wrapper), jail=False, timeout_s=args.iter_timeout,
                          extra_argv=["--strict-mcp-config"])
    result = ralph_loop(
        driver=driver,
        prompt=build_prompt(args.goal, sandbox, remote),
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
    print(json.dumps(summary, indent=2))
    return 0 if result.succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
