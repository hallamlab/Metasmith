#!/usr/bin/env python3
"""Every environment the standard library declares, with the image behind it.

    inventory.py [--out inventory.json]

Sizes come from the registry MANIFEST, not from `docker images`. The local
listing reports the uncompressed on-disk size, which for these images runs two to
three times the transfer size, and comparing one against the other makes a merge
look cheaper or dearer than it is.

Reads `resources/env/*.env` and the `provides` lists in `data_types/env.yml`, and
walks the transform tree for which transform requires which env.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.request import Request, urlopen

HERE = Path(__file__).resolve()
REPO = HERE.parents[4]
LIB = REPO / "src" / "metasmith_libraries"
ENV_DIR = LIB / "resources" / "env"
ENV_TYPES = LIB / "data_types" / "env.yml"
TRANSFORMS = LIB / "transforms"

ACCEPT = ", ".join([
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
])


def get(url: str, headers: dict) -> dict:
    with urlopen(Request(url, headers=headers), timeout=60) as r:
        return json.loads(r.read())


def registry_of(image: str) -> tuple[str, str, str]:
    """(registry host, repository, reference) for a docker:// URI's body."""
    ref = "latest"
    body = image
    if ":" in body.rsplit("/", 1)[-1]:
        body, ref = body.rsplit(":", 1)
    if "/" not in body or "." not in body.split("/")[0]:
        return "registry-1.docker.io", (body if "/" in body else f"library/{body}"), ref
    host, repo = body.split("/", 1)
    return host, repo, ref


def token_for(host: str, repo: str) -> str | None:
    if host == "registry-1.docker.io":
        url = f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:{repo}:pull"
    elif host == "quay.io":
        url = f"https://quay.io/v2/auth?service=quay.io&scope=repository:{repo}:pull"
    else:
        return None
    try:
        return get(url, {})["token"]
    except Exception:                                              # noqa: BLE001
        return None


def manifest_size(image: str) -> tuple[int | None, str]:
    host, repo, ref = registry_of(image)
    tok = token_for(host, repo)
    headers = {"Accept": ACCEPT}
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    try:
        m = get(f"https://{host}/v2/{repo}/manifests/{ref}", headers)
    except Exception as e:                                         # noqa: BLE001
        return None, f"{type(e).__name__}: {e}"
    if "manifests" in m:
        picked = next(
            (x for x in m["manifests"]
             if x.get("platform", {}).get("architecture") == "amd64"
             and x.get("platform", {}).get("os") == "linux"),
            m["manifests"][0],
        )
        try:
            m = get(f"https://{host}/v2/{repo}/manifests/{picked['digest']}", headers)
        except Exception as e:                                     # noqa: BLE001
            return None, f"{type(e).__name__}: {e}"
    layers = m.get("layers", [])
    if not layers:
        return None, "no layers in manifest"
    return sum(int(l.get("size", 0)) for l in layers), "ok"


def read_env(path: Path) -> dict:
    out = {"container": None, "conda": None}
    for line in path.read_text().splitlines():
        line = line.strip()
        for key in out:
            if line.startswith(f"{key}:"):
                out[key] = line.split(":", 1)[1].strip()
    if out["container"]:
        out["container"] = out["container"].replace("docker://", "")
    return out


def read_provides() -> dict[str, list[str]]:
    provides: dict[str, list[str]] = {}
    current = None
    in_list = False
    for raw in ENV_TYPES.read_text().splitlines():
        m = re.match(r"^  ([A-Za-z0-9_.\-]+\.env):\s*$", raw)
        if m:
            current, in_list = m.group(1), False
            provides[current] = []
            continue
        if current and re.match(r"^      provides:\s*$", raw):
            in_list = True
            continue
        if in_list:
            m = re.match(r"^      - (.+?)\s*$", raw)
            if m:
                provides[current].append(m.group(1))
            elif raw.strip():
                in_list = False
    return provides


def read_requirers() -> dict[str, list[str]]:
    requirers: dict[str, list[str]] = {}
    for py in TRANSFORMS.rglob("*.py"):
        if "_metadata" in py.parts:
            continue
        text = py.read_text(errors="replace")
        for env in set(re.findall(r'lib\.GetType\("env::([^"]+)"\)', text)):
            requirers.setdefault(env, []).append(
                str(py.relative_to(TRANSFORMS)).replace(".py", "")
            )
    return {k: sorted(v) for k, v in requirers.items()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=HERE.parent / "inventory.json")
    args = ap.parse_args()

    provides = read_provides()
    requirers = read_requirers()
    rows = []
    for env_file in sorted(ENV_DIR.glob("*.env")):
        name = env_file.name
        spec = read_env(env_file)
        size, note = (None, "no container declared")
        if spec["container"]:
            size, note = manifest_size(spec["container"])
        rows.append({
            "env": name,
            "image": spec["container"],
            "conda": spec["conda"],
            "compressed_bytes": size,
            "note": note,
            "provides": provides.get(name, []),
            "required_by": requirers.get(name, []),
        })
        mb = "-" if size is None else f"{size / 1e6:.0f} MB"
        print(f"{name:<36} {mb:>10}  {note if note != 'ok' else ''}", flush=True)

    args.out.write_text(json.dumps(rows, indent=2))
    print(f"\n{len(rows)} environments -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
