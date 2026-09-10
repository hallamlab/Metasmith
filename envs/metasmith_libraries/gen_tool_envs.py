#!/usr/bin/env python3
from __future__ import annotations
import re, sys
from pathlib import Path

# Both directories are addressed from the repo root, not from this file's parent.
# The monorepo split them: the library's content lives under src/ and its conda
# recipes under envs/, where this script sits. The pre-monorepo paths resolved to
# envs/resources/env and envs/envs/tools -- neither exists, so the script found
# zero sources and printed "portable (0)" instead of failing.
HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
ENV_DIR = REPO / "src" / "metasmith_libraries" / "resources" / "env"
RECIPE_DIR = HERE / "tools"

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

HAND_WRITTEN = {"ecspr"}


def derive_spec(stem: str, uri: str) -> str | None:
    m = BIOCONTAINERS.search(uri)
    if m:
        pkg, ver = m.group(1), m.group(2)
        return f"{pkg}={ver}"
    return CURATED.get(stem)


def read_uri(p: Path) -> str:
    text = p.read_text().strip()
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("container:"):
            return line.split(":", 1)[1].strip()
    return text


def read_conda(p: Path) -> str | None:
    """An existing `conda:` line, which this script must not delete.

    `derive_spec` only knows biocontainer URIs and its own CURATED table. Every
    other env -- the hallamlab images, anything hand-built -- returns None, and
    without this the rewrite drops a conda arm somebody wrote by hand. That is not
    a regeneration, it is a deletion, and it is silent.
    """
    if not p.exists():
        return None
    for line in p.read_text().splitlines():
        if line.strip().startswith("conda:"):
            return line.split(":", 1)[1].strip()
    return None


def read_header(p: Path) -> list[str]:
    """The leading `#` block of an env file, which this script must not eat.

    AGENTS.md requires a pin to carry a comment naming the tag, the date and what
    was verified inside the image -- a bare digest is a pin nobody can audit. This
    script rewrites every env file from two computed lines, so without this it
    deletes exactly the comments that rule mandates, on every run, silently.
    Only the leading block is kept: anything after `container:` is regenerated.
    """
    if not p.exists():
        return []
    header: list[str] = []
    for line in p.read_text().splitlines():
        if line.startswith("#") or (not line.strip() and header):
            header.append(line)
            continue
        break
    while header and not header[-1].strip():
        header.pop()
    return header


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
        if stem in HAND_WRITTEN:
            lines.append(f"conda: {stem}")
            portable.append((stem, f"{RECIPE_DIR.name}/{stem}.yml (hand-written)"))
        elif spec:
            lines.append(f"conda: {stem}")
            portable.append((stem, spec))
            (RECIPE_DIR / f"{stem}.yml").write_text(
                f"name: {stem}\n"
                "channels:\n  - conda-forge\n  - bioconda\n"
                f"dependencies:\n  - {spec}\n"
            )
        else:
            existing = read_conda(env_path)
            if existing:
                lines.append(f"conda: {existing}")
                portable.append((stem, f"{existing} (kept; not derivable)"))
            else:
                container_only.append(stem)
        env_path.write_text("\n".join(read_header(env_path) + lines) + "\n")
        if src.suffix == ".oci":
            src.unlink()

    print(f"portable ({len(portable)}): conda env + recipe written")
    for stem, spec in sorted(portable):
        print(f"  {stem:24s} -> {spec}")
    print(f"\ncontainer-only ({len(container_only)}):")
    print("  " + ", ".join(sorted(container_only)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
