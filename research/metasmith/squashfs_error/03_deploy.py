"""Deploy a metasmith agent to the remote test home, then report how that host
decided to read the image's rootfs.

Deploy is what puts the relay binary on the host: it is extracted from inside
the metasmith container by `msm api deploy_from_container`, so this step also
proves the shipped SIF is executable at all. The image pull is skipped because
02_ship_sif.py already pre-placed the SIF where provisioning looks.

Usage:
    python main/squashfs_error/03_deploy.py --host chamois --home /home/tliu/msm_squashfs_test
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from metasmith.agents.agent import Agent
from metasmith.models.remote import Source, SshSource
from metasmith.env.environment import Environment, ContainerDef, Runtime
from metasmith.constants import AgentPaths
from metasmith.logging import Log


def _ssh(host: str, cmd: str) -> str:
    argv = ["bash", "-c", cmd] if host == "local" else ["ssh", host, cmd]
    res = subprocess.run(argv, capture_output=True, text=True)
    return (res.stdout + res.stderr).strip()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="chamois", help='ssh alias, or "local" for this machine')
    ap.add_argument("--home", required=True)
    ap.add_argument("--assertive", action="store_true", help="force re-provision + re-extract")
    args = ap.parse_args(argv)

    local = args.host == "local"
    home = Source.FromLocal(Path(args.home)) if local else SshSource(host=args.host, path=args.home).AsSource()
    smith = Agent(
        home=home,
        runtime=Runtime.APPTAINER,
        setup_commands=[],
    )
    print(f"container: {smith.container}")

    env = Environment(
        image=smith.container,
        runtime=Runtime.APPTAINER,
        container=ContainerDef(cache=Path(args.home) / AgentPaths.CONTAINER_CACHE),
    )
    print("\n== Deploy() ==", flush=True)
    smith.Deploy(assertive=args.assertive)

    print("\n== what landed ==")
    print(_ssh(args.host, f'ls -la "{args.home}" "{args.home}/relay" "{args.home}/{AgentPaths.CONTAINER_CACHE}"'))
    print("\n== run command the wrapper would use ==")
    print(env.MakeRunCommand(local=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
