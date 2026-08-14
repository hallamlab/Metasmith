#!/usr/bin/env python3
"""Generate the generic env resources + conda recipes from container URIs.

Reproducible source for the containers->env migration. For each
`resources/env/<tool>.oci` (legacy bare docker:// URI) or `<tool>.env` (already
migrated) it (re)writes a `<tool>.env` YAML carrying:

    container: <the docker:// URI>
    conda: <tool>            # only when a conda env is feasible

and, for the conda-feasible tools, a conda env recipe at
`envs/tools/<tool>.yml` pinned to the derived bioconda package spec.

Conda feasibility:
  * biocontainers images (quay.io/biocontainers/<pkg>:<ver>--<build>) -> the
    bioconda spec `<pkg>=<ver>` is derived automatically.
  * a curated table maps common bioconda tools shipped from other registries
    (staphb/, old biocontainers/ dockerhub tags, a few hallamlab images).
  * everything else stays container-only (custom / ML / proprietary images).

Idempotent: re-running reads the container URI back out of an existing .env.
Run:  python envs/gen_tool_envs.py            (from the repo root)
"""
from __future__ import annotations
import re, sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ENV_DIR = REPO / "resources" / "env"
RECIPE_DIR = REPO / "envs" / "tools"

# tools on bioconda whose image is NOT a quay.io/biocontainers one -> pin by hand
CURATED = {
    "bbtools": "bbmap=39.49",
    "fastani": "fastani=1.34",
    "fastp": "fastp=1.0.1",
    "filtlong": "filtlong=0.3.1",
    "flye": "flye=2.9.6",
    "samtools": "samtools=1.23",
    "seqkit": "seqkit=2.13.0",
    "skani": "skani=0.2.2",
    "ncbi-datasets": "ncbi-datasets-cli=18.9.0",
    "ppanggolin": "ppanggolin=2.2.5",
    "bedtools": "bedtools=2.27.1",
    "fastqc": "fastqc=0.11.9",
    "megahit": "megahit=1.2.9",
    "minimap2": "minimap2=2.15",
    "nanoplot": "nanoplot=1.42.0",
    "checkm": "checkm2=1.1.0",
    "antismash": "antismash=7.1.0",
    "genomad": "genomad=1.11.0",
}

BIOCONTAINERS = re.compile(r"quay\.io/biocontainers/([^:/]+):([^-\s]+)")


def derive_spec(stem: str, uri: str) -> str | None:
    m = BIOCONTAINERS.search(uri)
    if m:
        pkg, ver = m.group(1), m.group(2)
        return f"{pkg}={ver}"
    return CURATED.get(stem)


def read_uri(p: Path) -> str:
    """URI from a legacy .oci (whole content) or a migrated .env (container:)."""
    text = p.read_text().strip()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("container:"):
            return line.split(":", 1)[1].strip()
    return text  # legacy bare-URI .oci


def main() -> int:
    RECIPE_DIR.mkdir(parents=True, exist_ok=True)
    sources = sorted(list(ENV_DIR.glob("*.oci")) + list(ENV_DIR.glob("*.env")))
    portable, container_only = [], []
    for src in sources:
        stem = src.stem
        uri = read_uri(src)
        spec = derive_spec(stem, uri)
        env_path = ENV_DIR / f"{stem}.env"
        lines = [f"container: {uri}"]
        if spec:
            lines.append(f"conda: {stem}")
            portable.append((stem, spec))
            (RECIPE_DIR / f"{stem}.yml").write_text(
                f"name: {stem}\n"
                "channels:\n  - conda-forge\n  - bioconda\n"
                f"dependencies:\n  - {spec}\n"
            )
        else:
            container_only.append(stem)
        env_path.write_text("\n".join(lines) + "\n")
        if src.suffix == ".oci":
            src.unlink()  # drop the legacy file (git add -A picks up the rename)

    print(f"portable ({len(portable)}): conda env + recipe written")
    for stem, spec in sorted(portable):
        print(f"  {stem:24s} -> {spec}")
    print(f"\ncontainer-only ({len(container_only)}):")
    print("  " + ", ".join(sorted(container_only)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
