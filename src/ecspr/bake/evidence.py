"""Keep what each tool actually said, under `data/processed/<tool>/<version>/`.

THE LAYOUT MIRRORS `originals/`, AND THAT IS THE WHOLE IDEA
-----------------------------------------------------------
`data/originals/` is one folder per SOURCE, each holding `<release>/`, because the
release IS the identity of the bytes: `chem_prop.tsv` on its own says nothing about
which MNXM space it belongs to. The processed tier has exactly the same problem one
layer down. A saved RXNMapper cache says nothing about which RXNMapper produced it, and
`(metabolite, canonical rank)` is an rdkit canonical rank -- so two rdkit versions give
one metabolite two rank systems and two caches that look comparable are not.

So: one folder per TOOL, each holding `<version>/`. Which version produced a table is a
DIRECTORY NAME rather than a line in a metadata file, for the same reason the release is
in `originals/`: a fact you have to open a file to learn is a fact people stop checking.

    data/processed/
      rxnmapper/0.4.3/        mapped.tsv  pairs.parquet  status.tsv  manifest.json
      localmapper/0.1.5/      ...
      indigo/1.45.0/          shards, sidecars, merged cache, pairs, status
      metacyc/26/             the curated member's cache + its refusal report
      curation/<buildlib>/    crosswalk, placeholders, balance verdicts, mapped
      equilibrator/0.7.0/     the member's dG table + the calibration points

WHY KEEP THEM AT ALL
--------------------
Every one of these was being written to the metasmith work directory, which the tier
rule calls transient. Three different things went with it:

  * THE EXPENSIVE ONES. LocalMapper over ~45k reactions is most of a day of CPU. Losing
    its cache means every downstream change -- a fusion rule, a gate threshold, an
    extractor fix -- re-pays that day to answer a question the mapper already answered.
  * THE ANSWER TO "WHY NOT". A reaction that produced no pairs did so for a reason the
    extractor recorded per reaction (`stripped`, `no_pairs`, `unrankable`, `timeout`).
    With that gone, "why did this reaction drop out" is only answerable by re-running
    the thing that dropped it.
  * THE PROVENANCE CLAIM ITSELF. A fused row says `method=consensus,
    source=rxnmapper+indigo`. That is a claim ABOUT the members, and a provenance claim
    that cannot be checked against what the members actually said is not provenance --
    it is an assertion with a schema.

WHAT THIS IS NOT. Not a log. Logs are prose about a run; this is the run's outputs in
the form the next run can read. An evidence directory nobody can navigate is a pile,
which is why `INDEX.tsv` records rows and a digest per file, and why a path that does
not exist is recorded as ABSENT rather than skipped -- "localmapper produced nothing"
and "localmapper never ran" are different facts.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# The packages whose version changes what the tables MEAN, rather than merely how fast
# they were produced. rdkit is first because it is the identity: `(mnxm, canonical rank)`
# is an rdkit canonical rank, so a different rdkit is a different node space -- which is
# why it goes in EVERY manifest, not only the mappers'.
TRACKED = (
    "rdkit", "numpy", "pandas", "pyarrow",
    "rxnmapper", "localmapper", "indigo", "torch", "transformers",
    "equilibrator_api", "equilibrator_cache", "component_contribution",
)

# tool folder -> the package whose version names its release directory. Tools absent
# here are versioned by an explicit `--version` (MetaCyc by its release, our own stages
# by the buildlib git sha), because they have no package to ask.
TOOL_PACKAGE = {
    "rxnmapper": "rxnmapper",
    "localmapper": "localmapper",
    "indigo": "indigo",
    "equilibrator": "component_contribution",
}


# import name -> distribution name, where they differ. Only needed for the ones this
# actually asks about; a missing entry falls back to the import name with `_` -> `-`.
_DIST = {"indigo": "epam-indigo"}


def _version(mod: str):
    """The INSTALLED DISTRIBUTION's version, preferred over the module's `__version__`.

    NOT INTERCHANGEABLE, and localmapper is the live proof: the image pins
    `localmapper==0.1.5`, the distribution metadata says 0.1.5, and
    `localmapper.__version__` says **0.1.1** -- upstream never bumped the attribute.
    Trusting it would file 0.1.5's output under `localmapper/0.1.1/`, and a later run of
    a real 0.1.1 would land in the same directory with nothing to tell the two apart --
    exactly the failure `<source>/<release>/` exists to prevent.

    The distribution metadata is what pip and conda actually resolved. `__version__` is
    whatever the author remembered to update, so it is the fallback, not the source.
    """
    try:
        import importlib.metadata as md
        return md.version(_DIST.get(mod, mod.replace("_", "-")))
    except Exception:
        pass
    try:
        m = __import__(mod)
    except Exception:
        return None
    v = getattr(m, "__version__", None)
    if v:
        return str(v)
    if mod == "indigo":
        # Indigo reports through an instance, and its string carries a build hash and a
        # platform triple. Only the numeric head names the release.
        try:
            return str(m.Indigo().version()).split("-")[0]
        except Exception:
            return "present"
    return "present"


def buildlib_sha():
    """The method's own version. Absent inside a container, where the buildlib arrives
    as staged files with no git history -- an honest "unknown", not a build failure."""
    try:
        r = subprocess.run(["git", "-C", str(Path(__file__).resolve().parent),
                            "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return None


def buildlib_fingerprint(sub: str | None = None):
    """A content hash over the buildlib, for the version the git sha cannot supply.

    `buildlib_sha` is None wherever it matters most: inside the container the buildlib
    arrives as STAGED FILES with no git history, so every stage that is versioned by the
    method rather than by a package -- curation, worklist, rescue -- had no version to
    write under and `tool_version` raised. That is a SystemExit at the evidence copy,
    which runs in the last seconds of a step that may have taken eight hours.

    Hashing the modules themselves answers the question the sha was asked for and answers
    it in the container too: which method produced this. It changes when any module
    changes, which is the intended granularity -- coarser than per-file, and identical
    for two runs of the same tree.

    THE GLOB IS ONE DIRECTORY DEEP, and that is a trap rather than a detail: the default
    covers `bake/*.py` and NOT `bake/<subpackage>/*.py`, so the whole direction ensemble
    is invisible to it. Two runs of materially different direction code would file their
    evidence under one version directory -- exactly what `<tool>/<version>/` exists to
    prevent. `sub` names a subpackage to fingerprint instead; the default stays as it was
    so the version of every artifact already written keeps its meaning.

    AND IT GLOBS BY EXTENSION, which is the same trap one level down. The direction lane's
    substitution tables are curated `.tsv` beside the modules that read them: they are
    method, not data -- swapping one changes every ratio the members produce -- so a
    `.py`-only hash would file two materially different bakes at one version. Adding a
    suffix here is safe for what is already written only while no directory being
    fingerprinted holds a file of that suffix today; check before adding a third.
    """
    d = Path(__file__).resolve().parent
    tag = "lib-"
    if sub:
        d = d / sub
        tag = f"lib-{sub}-"
    h = hashlib.sha256()
    try:
        for p in sorted(q for ext in ("*.py", "*.tsv") for q in d.glob(ext)):
            h.update(p.name.encode())
            h.update(p.read_bytes())
    except OSError:
        return None
    return tag + h.hexdigest()[:12]


def tool_version(tool: str, explicit: str | None = None):
    """The release directory's name for `tool`.

    NEVER 'unknown'. Writing two builds' outputs at one path with nothing to tell them
    apart is precisely the failure `originals/<source>/<release>/` exists to prevent, so
    the fallbacks narrow rather than shrug: an explicit name, then the tool's package,
    then the buildlib's git sha, then a content hash of the buildlib. The last is what
    holds inside a container, where the first three are all unavailable.
    """
    if explicit:
        return explicit
    pkg = TOOL_PACKAGE.get(tool)
    if pkg:
        v = _version(pkg)
        if v and v != "present":
            return v
    sha = buildlib_sha() or buildlib_fingerprint()
    if sha:
        return sha
    raise SystemExit(
        f"[evidence] cannot determine a version for tool '{tool}'. Pass --version. "
        f"Writing under 'unknown/' would put two builds' outputs at one path with "
        f"nothing to tell them apart.")


def manifest(tool: str, version: str, extra: dict | None = None) -> dict:
    out = {
        "tool": tool,
        "version": version,
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "versions": {m: v for m in TRACKED if (v := _version(m)) is not None},
    }
    if (sha := buildlib_sha()):
        out["buildlib_git"] = sha
    if extra:
        out.update(extra)
    return out


def _digest(p: Path, limit=64 << 20):
    """sha256, or of the first `limit` bytes for a large file -- LABELLED as partial.

    It still answers the question actually asked ("did these two runs produce different
    bytes") and keeps indexing a multi-gigabyte cache from costing minutes.
    """
    h = hashlib.sha256()
    n = 0
    with open(p, "rb") as fh:
        while chunk := fh.read(1 << 20):
            h.update(chunk)
            n += len(chunk)
            if n >= limit:
                return f"sha256:{h.hexdigest()}(first{limit >> 20}MB)"
    return f"sha256:{h.hexdigest()}"


def _rows(p: Path):
    """Row count where the format makes one cheap; None where it does not."""
    try:
        if p.suffix == ".parquet":
            import pyarrow.parquet as pq
            return pq.ParquetFile(p).metadata.num_rows
        if p.suffix in (".tsv", ".csv", ".txt", ".attempted", ".json"):
            with open(p, "rb") as fh:
                return max(0, sum(1 for _ in fh) - 1)
    except Exception:
        return None
    return None


def collect(root: Path, tool: str, paths, version: str | None = None,
            extra: dict | None = None):
    """Copy `tool`'s raw outputs to `root/<tool>/<version>/` and index them.

    COPIES rather than moves: the pipeline's next step still reads the original, and an
    evidence collector that relocates its subject is one that can break the run it is
    documenting.
    """
    root = Path(root)
    ver = tool_version(tool, version)
    dest = root / tool / ver
    dest.mkdir(parents=True, exist_ok=True)

    rows = []
    for p in paths:
        p = Path(p)
        if not p.exists() or p.stat().st_size == 0:
            rows.append(dict(file=p.name, status="absent", bytes=0, rows=None,
                             digest=None))
            continue
        tgt = dest / p.name
        shutil.copy2(p, tgt)
        rows.append(dict(file=p.name, status="present", bytes=tgt.stat().st_size,
                         rows=_rows(tgt), digest=_digest(tgt)))

    (dest / "manifest.json").write_text(
        json.dumps(manifest(tool, ver, extra), indent=2, sort_keys=True) + "\n")

    # Rewritten per (tool, version), appended across calls -- stages land one at a time
    # and a later one must not erase the index of an earlier one.
    idx = dest / "INDEX.tsv"
    new = not idx.exists()
    with open(idx, "a") as fh:
        if new:
            fh.write("file\tstatus\tbytes\trows\tdigest\n")
        for r in rows:
            fh.write(f"{r['file']}\t{r['status']}\t{r['bytes']}\t"
                     f"{'' if r['rows'] is None else r['rows']}\t{r['digest'] or ''}\n")

    n_ok = sum(1 for r in rows if r["status"] == "present")
    print(f"[evidence] {tool} {ver}: kept {n_ok}/{len(rows)} raw outputs -> {dest}",
          flush=True)
    return dest


def cmd_collect(args):
    collect(Path(args.root), args.tool, args.file, version=args.version)
    return 0


def cmd_manifest(args):
    v = tool_version(args.tool, args.version)
    print(json.dumps(manifest(args.tool, v), indent=2, sort_keys=True))
    return 0


def cmd_fingerprint(args):
    """Print the content hash a lane can pass back as `--version`.

    A lane whose method lives in a subpackage cannot be versioned by the default
    fingerprint, and the packages it imports are pinned by its image rather than by the
    change it is running -- so the identity has to be computed and handed over.
    """
    fp = buildlib_fingerprint(args.package)
    if fp is None:
        raise SystemExit(f"[evidence] no python modules under buildlib "
                         f"'{args.package or '.'}' to fingerprint")
    print(fp)
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("collect"); p.set_defaults(fn=cmd_collect)
    p.add_argument("--root", required=True, help="data/fabfos/processed/")
    p.add_argument("--tool", required=True, help="the folder name, e.g. rxnmapper")
    p.add_argument("--version", default=None,
                   help="the release directory. Read from the package when it can be; "
                        "REQUIRED for tools with no package to ask (metacyc's release, "
                        "our own stages' buildlib sha)")
    p.add_argument("--file", nargs="+", required=True,
                   help="raw outputs to keep; a missing one is recorded as absent, "
                        "because 'produced nothing' and 'never ran' are different facts")
    p = sub.add_parser("manifest"); p.set_defaults(fn=cmd_manifest)
    p.add_argument("--tool", required=True)
    p.add_argument("--version", default=None)
    p = sub.add_parser("fingerprint"); p.set_defaults(fn=cmd_fingerprint)
    p.add_argument("--package", default=None,
                   help="a buildlib subpackage to hash, e.g. `direction`. Omitted, this "
                        "is the same hash `collect` falls back to")
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
