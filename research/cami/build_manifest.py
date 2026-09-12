"""Enumerate every CAMI object into one manifest: url, exact size, checksum, drop order.

Emits manifest.tsv beside itself. Stdlib only, so it runs on a login node too.
"""

import argparse
import concurrent.futures as cf
import json
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

PUBLISSO = "https://frl.publisso.de/data"
SWIFT = "https://s3.bi.denbi.de/swift/v1"

# Each PUBLISSO root: the frl id, the dataset label, and the checksum file if one exists.
PUBLISSO_ROOTS = [
    ("frl:6425521", "cami2_challenge", "md5sums.txt"),
    ("frl:6425518", "cami2_toy_hmp", "md5sums.tsv"),
    ("frl:6421672", "cami2_toy_mousegut", None),
]

SWIFT_CONTAINERS = [("cami3__human-gut-toy", "cami3_toy_humangut")]

# CAMI I lives on GigaDB, whose site renders its file list client-side. This is the
# JSON its own frontend calls; per_page is the only paging parameter it honours.
GIGADB = ("https://gigadb.org/gigadb/api/dataset/list_dataset_files/"
          "?dataset_id=100344&per_page=500", "cami1")

_HREF = re.compile(r'(?<=href=")([^"]*)(?=")')


def _get(url: str, retries: int = 4) -> bytes:
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=120) as r:
                return r.read()
        except (urllib.error.URLError, TimeoutError):
            if attempt == retries - 1:
                raise
    raise AssertionError("unreachable")


_SIZE_CACHE: dict[str, int] = {}


def load_size_cache(path: Path) -> None:
    """Seed sizes from a previous manifest so a rebuild costs no HEAD requests."""
    if not path.exists():
        return
    for line in path.read_text().splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) >= 4 and parts[3].isdigit():
            _SIZE_CACHE[parts[2]] = int(parts[3])
    sys.stderr.write(f"size cache: {len(_SIZE_CACHE)} entries from {path}\n")


def _size(url: str, retries: int = 4, use_cache: bool = True) -> int:
    """Exact Content-Length. HEAD, because the payload is gigabytes."""
    if use_cache and url in _SIZE_CACHE:
        return _SIZE_CACHE[url]
    req = urllib.request.Request(url, method="HEAD")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                return int(r.headers["Content-Length"])
        except (urllib.error.URLError, TimeoutError, TypeError):
            if attempt == retries - 1:
                return -1
    return -1


def walk_index(base: str, prefix: str = "") -> list[str]:
    """Recursively list an Apache autoindex, returning paths relative to base."""
    out = []
    for name in _HREF.findall(_get(base + prefix).decode("utf-8", "replace")):
        if not name or name.startswith(("?", "/", "http")):
            continue
        if name.endswith("/"):
            out.extend(walk_index(base, prefix + name))
        else:
            out.append(prefix + name)
    return out


def drop_order(relpath: str) -> int:
    """0 never drops. Higher drops sooner if fir runs short: ground truth before inputs."""
    if "/per_bodysite/" in "/" + relpath:
        return 1
    if re.search(r"(^|/)hybrid(_nano|_pacbio)?/", relpath):
        return 2
    return 0


def collect() -> list[dict]:
    rows: list[dict] = []
    for frl, dataset, sumfile in PUBLISSO_ROOTS:
        base = f"{PUBLISSO}/{frl}/"
        sys.stderr.write(f"walking {dataset} ...\n")
        paths = walk_index(base)
        sums: dict[str, str] = {}
        if sumfile:
            for line in _get(base + sumfile).decode().splitlines():
                parts = line.split(None, 1)
                if len(parts) == 2:
                    sums[parts[1].strip()] = parts[0].strip()
        sys.stderr.write(f"  {len(paths)} files, {len(sums)} checksums; sizing ...\n")
        with cf.ThreadPoolExecutor(max_workers=8) as pool:
            sizes = list(pool.map(lambda p: _size(base + p), paths))
        for p, n in zip(paths, sizes):
            rows.append(dict(dataset=dataset, relpath=p, url=base + p,
                             bytes=n, md5=sums.get(p, ""), drop=drop_order(p)))

    for container, dataset in SWIFT_CONTAINERS:
        sys.stderr.write(f"listing {dataset} ...\n")
        for o in json.loads(_get(f"{SWIFT}/{container}/?format=json").decode()):
            if o["name"].endswith("/"):
                continue  # Swift pseudo-directory marker, a zero-byte object
            rows.append(dict(dataset=dataset, relpath=o["name"],
                             url=f"{SWIFT}/{container}/{o['name']}",
                             bytes=int(o["bytes"]), md5=o.get("hash", ""), drop=0))

    url, dataset = GIGADB
    sys.stderr.write(f"listing {dataset} ...\n")
    for o in json.loads(_get(url).decode())["data"]["data"]:
        rows.append(dict(dataset=dataset, relpath=o["file_name"], url=o["url"],
                         bytes=_size(o["url"], use_cache=False) or int(o["file_size"] or 0),
                         md5=(o.get("file_attributes") or {}).get("MD5 checksum", ""),
                         drop=0))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=Path(__file__).parent / "manifest.tsv")
    args = ap.parse_args()

    load_size_cache(args.out)
    rows = collect()
    with args.out.open("w") as f:
        f.write("dataset\trelpath\turl\tbytes\tmd5\tdrop\n")
        for r in sorted(rows, key=lambda r: (r["dataset"], r["relpath"])):
            f.write("{dataset}\t{relpath}\t{url}\t{bytes}\t{md5}\t{drop}\n".format(**r))

    total = sum(r["bytes"] for r in rows if r["bytes"] > 0)
    missing = [r for r in rows if r["bytes"] <= 0]
    sys.stderr.write(f"\n{len(rows)} objects, {total / 1024**4:.2f} TiB -> {args.out}\n")
    if missing:
        sys.stderr.write(f"WARNING: {len(missing)} objects had no Content-Length\n")


if __name__ == "__main__":
    main()
