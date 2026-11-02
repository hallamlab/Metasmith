"""Ship the locally-built SIF to a remote agent home, pre-placing it exactly
where `MakeMaterialiseCommand` looks — so deploy's `[ ! -e {sif} ]` gate skips
the pull and the host runs *our* image rather than whatever is on quay.

The destination name is derived from `Environment`, never hand-spelled: it is
`${APPTAINER_CACHEDIR:-<home>/container_images}/<mangled image uri>.sif` and the
mangling (`://` -> `..`, `:` -> `..`, `/` -> `_`) is one line of code we do not
want a second copy of.

Usage:
    python -m main.squashfs_error.02_ship_sif --host chamois --home ~/msm_squashfs_test
"""

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "src"))

from metasmith.agents.agent import Agent
from metasmith.env.environment import Environment, ContainerDef, Runtime
from metasmith.constants import AgentPaths

REPO = Path(__file__).resolve().parents[3]


def sif_name(image: str) -> str:
    env = Environment(image=image, runtime=Runtime.APPTAINER)
    return f"{env._cached_name()}.sif"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", default="chamois")
    ap.add_argument("--home", required=True, help="remote agent home (may start with ~)")
    ap.add_argument("--sif", default=str(REPO / "metasmith.sif"))
    ap.add_argument("--image", default=None, help="image uri; default = this source tree's")
    args = ap.parse_args(argv)

    image = args.image or Agent.__dataclass_fields__["container"].default
    local = Path(args.sif)
    assert local.exists(), f"no local sif at [{local}] — run 01_build_container.sh"

    cache = f"{args.home}/{AgentPaths.CONTAINER_CACHE}"
    dest = f"{cache}/{sif_name(image)}"
    print(f"image      : {image}")
    print(f"local sif  : {local} ({local.stat().st_size/1e6:.0f} MB)")
    print(f"remote dest: {args.host}:{dest}")

    res = subprocess.run(
        ["ssh", args.host, f'echo "CACHEDIR=[${{APPTAINER_CACHEDIR:-}}]"; mkdir -p "{cache}" && echo mkdir-ok'],
        capture_output=True, text=True, check=True,
    )
    print(res.stdout.strip())
    assert "CACHEDIR=[]" in res.stdout, "remote APPTAINER_CACHEDIR is set; the store root is not the agent home"
    assert "mkdir-ok" in res.stdout, res.stdout + res.stderr

    cmd = ["rsync", "-a", "--info=progress2", str(local), f"{args.host}:{dest}"]
    print(f"$ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, check=True)

    res = subprocess.run(
        ["ssh", args.host, f'ls -la "{dest}"; apptainer sif list "{dest}" 2>&1 | head -12'],
        capture_output=True, text=True, check=True,
    )
    print(res.stdout)
    print(f"OK: {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
