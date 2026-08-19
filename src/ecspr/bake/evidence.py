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

TRACKED = (
    "rdkit", "numpy", "pandas", "pyarrow",
    "rxnmapper", "localmapper", "indigo", "torch", "transformers",
    "equilibrator_api", "equilibrator_cache", "component_contribution",
)

TOOL_PACKAGE = {
    "rxnmapper": "rxnmapper",
    "localmapper": "localmapper",
    "indigo": "indigo",
    "equilibrator": "component_contribution",
}


_DIST = {"indigo": "epam-indigo"}


def _version(mod: str):
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
        try:
            return str(m.Indigo().version()).split("-")[0]
        except Exception:
            return "present"
    return "present"


def buildlib_sha():
    try:
        r = subprocess.run(["git", "-C", str(Path(__file__).resolve().parent),
                            "rev-parse", "--short", "HEAD"],
                           capture_output=True, text=True, timeout=10)
        if r.returncode == 0 and r.stdout.strip():
            return r.stdout.strip()
    except Exception:
        pass
    return None


def buildlib_fingerprint():
    d = Path(__file__).resolve().parent
    h = hashlib.sha256()
    try:
        for p in sorted(d.glob("*.py")):
            h.update(p.name.encode())
            h.update(p.read_bytes())
    except OSError:
        return None
    return "lib-" + h.hexdigest()[:12]


def tool_version(tool: str, explicit: str | None = None):
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
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
