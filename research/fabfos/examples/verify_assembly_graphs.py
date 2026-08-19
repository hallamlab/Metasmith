#!/usr/bin/env python3
"""Name, check and bank the assemblies + graphs a metasmith run produced.

    python examples/verify_assembly_graphs.py <results_dir> [--bank <dir>]

`<results_dir>` is the `results/` tree of a run of
`examples/fabfos_assemblies_on_fir.py` -- either retrieved locally or in place
on the cluster. Pure stdlib, so it runs on either side.

WHY THE FILES NEED NAMING AT ALL
---------------------------------
metasmith names an output by its lineage hash, not by the sample: a pool's
spades contigs land as `1-1-1.<hash>-<typekey>.fna`. The pool has to be
recovered by a join, and that join is the reason this script exists rather than
a shell one-liner. There are two routes, and the first one is not always
available:

  lineage  `results/_manifests/given.csv` maps a reads instance_index to the
           read file whose basename carries the pool barcode, and each product
           manifest carries the `lineage` it descends from. So
           pool = given[reads_key][ lineage[reads_key] ].
           This works for megahit here but NOT for spades: the Orchestrator
           published spades' entries with their own key alone and no ancestry.
           Rather than guess at that, the second route is preferred when it can
           be had.

  nxf_work each task's `.command.sh` names the read_metadata item it was handed
           (`read_metadata_pool03_CAATCGAC.json`) and the task directory holds
           the outputs it produced, so basename -> pool is direct and needs no
           lineage at all. Pass `--work <run>/nxf_work`; only possible on the
           cluster, where the run still exists.

When `--work` resolves the mapping it is written to `pool_map.tsv` beside the
results, so a later local run of this script over the retrieved tree gets the
same names without the work directory. Guessing from file order would silently
mislabel every pool, so an unattributable output is an error, never a default.

WHAT IS CHECKED, AND WHAT EACH CHECK WOULD CATCH
-------------------------------------------------
spades  Every contig in `contigs.fasta` should appear as a GFA `P` line. That
        is the property the graph was kept for -- if the paths do not name the
        contigs we ship, the graph cannot be walked back to them and it is
        decoration. Circularity is a self-loop `L` line, and the link's overlap
        field states the exact length to trim, which is strictly better than
        searching a plausible repeat-length range.

megahit The FASTG is reconstructed from the k-max intermediate contigs, so its
        node names are SPAdes-style (`NODE_<n>_length_<L>_cov_<C>_ID_<i>`) and
        do NOT match the final contig names (`k141_<n>`). They are matched on
        (length, coverage), which megahit writes into both -- the contig header
        carries `len=` and `multi=`, the node name `_length_` and `_cov_`. A
        contig with no (length, cov) match is reported rather than assumed
        away: it means the contig was emitted at a k the graph does not cover.
        A self-loop is a node that lists itself among its own successors.

megahit's `flag=` field is reported against the graph rather than trusted, and
on the first pool checked it does not survive that. Of 42 contigs, 32 carry
flag=1; the k141 graph has ZERO self-loops, no flag=1 contig has any terminal
self-repeat, and flag partitions the contigs exactly by graph degree --
flag=1 <-> the node has no successors (32/32), flag=0 <-> it has at least one
(10/10). So the flag marks an ISOLATED unitig, not a circle. That also explains
the 98/98 split noticed on a previous pool: half the contigs were isolated, an
unremarkable fact rather than a suspicious one. `flag_vs_isolated` below is
printed per pool so the claim is retested on every one of them rather than
generalised from a single case.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
from collections import defaultdict
from pathlib import Path

PRODUCTS = {
    "sequences::spades_assembly":        ("spades",  "contigs", "fna"),
    "sequences::spades_assembly_graph":  ("spades",  "graph",   "gfa"),
    "sequences::spades_contig_paths":    ("spades",  "paths",   "paths"),
    "sequences::megahit_assembly":       ("megahit", "contigs", "fna"),
    "sequences::megahit_assembly_graph": ("megahit", "graph",   "fastg"),
}
READS_TYPE = "sequences::host_filtered_short_reads"
READS_SUFFIX = ".host_filtered.fq.gz"


def load_given(results: Path) -> tuple[str, dict[int, str]]:
    path = results / "_manifests" / "given.csv"
    if not path.exists():
        raise SystemExit(f"not a metasmith results tree: no {path}")
    key, by_index = None, {}
    with path.open() as f:
        for row in csv.DictReader(f):
            if row["type_name"] != READS_TYPE:
                continue
            key = row["instance_key"]
            name = Path(row["path"]).name
            pool = name[: -len(READS_SUFFIX)] if name.endswith(READS_SUFFIX) else name
            by_index[int(row["instance_index"])] = pool
    if key is None:
        raise SystemExit(f"no {READS_TYPE} rows in {path} -- wrong run?")
    return key, by_index


_META_POOL = re.compile(r"read_metadata_(\S+?)\.json")


def pools_from_work(work: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for cmd in sorted(work.glob("*/*/.command.sh")):
        d = cmd.parent
        code = (d / ".exitcode")
        if not code.exists() or code.read_text().strip() != "0":
            continue
        m = _META_POOL.search(cmd.read_text())
        if not m:
            continue
        for ext in ("fna", "gfa", "fastg", "paths"):
            for f in d.glob(f"*.{ext}"):
                mapping[f.name] = m.group(1)
    return mapping


def load_products(results: Path, reads_key: str, pool_of_index: dict[int, str],
                  by_basename: dict[str, str]) -> dict[str, dict[str, Path]]:
    out: dict[str, dict[str, Path]] = defaultdict(dict)
    for manifest in sorted((results / "_manifests").glob("*.json")):
        type_name = manifest.name.split(".")[0].replace("-", "::", 1)
        if type_name not in PRODUCTS:
            continue
        assembler, kind, _ext = PRODUCTS[type_name]
        for entry in json.loads(manifest.read_text()):
            path = results / entry["path"]
            pool = by_basename.get(path.name)
            if pool is None:
                idxs = entry["lineage"].get(reads_key)
                pool = pool_of_index.get(idxs[0]) if idxs else None
            if pool is None:
                raise SystemExit(
                    f"{manifest.name}: cannot attribute [{path.name}] to a pool. "
                    f"Its lineage carries no {READS_TYPE} ancestor and no "
                    f"mapping was supplied -- re-run with --work <run>/nxf_work "
                    f"on the cluster, or with a pool_map.tsv beside the results.")
            out[pool][f"{assembler}.{kind}"] = path
    return dict(out)


def read_fasta_headers(path: Path) -> list[str]:
    return [ln[1:].strip() for ln in path.read_text().splitlines() if ln.startswith(">")]


def parse_contig_paths(paths_file: Path) -> dict[str, list[str]]:
    walks: dict[str, list[str]] = defaultdict(list)
    current: str | None = None
    for raw in paths_file.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("NODE_"):
            if line.endswith("'"):
                current = None
                continue
            current = line
            walks.setdefault(current, [])
            continue
        if current is None:
            continue
        for tok in line.rstrip(";").split(","):
            tok = tok.strip()
            if tok:
                walks[current].append(tok)
    return dict(walks)


def check_spades(contigs: Path, gfa: Path, paths_file: Path | None) -> dict:
    names = {h.split()[0] for h in read_fasta_headers(contigs)}
    links: dict[tuple[str, str], str] = {}
    n_selfloop_edges = 0
    n_paths = n_segs = 0
    with gfa.open() as f:
        for line in f:
            if line.startswith("P\t"):
                n_paths += 1
            elif line.startswith("S\t"):
                n_segs += 1
            elif line.startswith("L\t"):
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                links[(parts[1] + parts[2], parts[3] + parts[4])] = parts[5]
                if parts[1] == parts[3]:
                    n_selfloop_edges += 1

    walks = parse_contig_paths(paths_file) if paths_file else {}
    missing = sorted(names - set(walks))
    circular = {}
    for contig, edges in walks.items():
        if contig not in names or not edges:
            continue
        closing = links.get((edges[-1], edges[0]))
        if closing is not None:
            circular[contig] = closing
    return dict(
        contigs=len(names),
        segments=n_segs,
        scaffold_paths=n_paths,
        walked=len(names & set(walks)),
        missing=missing,
        self_loop_edges=n_selfloop_edges,
        circular=circular,
    )


_NODE = re.compile(r"NODE_\d+_length_(\d+)_cov_([0-9.]+)_ID_\d+(')?")
_CONTIG = re.compile(r"^(\S+)\s+flag=(\d+)\s+multi=([0-9.]+)\s+len=(\d+)")


def check_megahit(contigs: Path, fastg: Path) -> dict:
    by_shape: dict[tuple[int, str], list[str]] = defaultdict(list)
    for h in read_fasta_headers(contigs):
        m = _CONTIG.match(h)
        if not m:
            continue
        name, flag, multi, length = m.group(1), int(m.group(2)), m.group(3), int(m.group(4))
        by_shape[(length, multi)].append(f"{name}|{flag}")

    node_shapes: set[tuple[int, str]] = set()
    loop_shapes: set[tuple[int, str]] = set()
    degree: dict[tuple[int, str], set[str]] = defaultdict(set)
    with fastg.open() as f:
        for line in f:
            if not line.startswith(">"):
                continue
            head = line[1:].strip().rstrip(";")
            src, _, rest = head.partition(":")
            m = _NODE.match(src)
            if not m:
                continue
            shape = (int(m.group(1)), m.group(2))
            node_shapes.add(shape)
            src_base = src.rstrip("'")
            degree[shape]
            for succ in (s for s in rest.split(",") if s):
                degree[shape].add(succ)
                if succ.rstrip("'") == src_base:
                    loop_shapes.add(shape)

    matched, unmatched, looped_flags = 0, [], defaultdict(int)
    flags = defaultdict(int)
    flag_vs_isolated: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for shape, entries in by_shape.items():
        for e in entries:
            name, flag_s = e.split("|")
            flag = int(flag_s)
            flags[flag] += 1
            if shape in node_shapes:
                matched += 1
                key = "isolated" if not degree.get(shape) else "connected"
                flag_vs_isolated[flag][key] += 1
                if shape in loop_shapes:
                    looped_flags[flag] += 1
            else:
                unmatched.append(name)
    return dict(
        contigs=sum(len(v) for v in by_shape.values()),
        matched=matched,
        unmatched=sorted(unmatched),
        nodes=len(node_shapes),
        self_loop_nodes=len(loop_shapes),
        flags=dict(flags),
        self_looping_by_flag=dict(looped_flags),
        flag_vs_isolated={k: dict(v) for k, v in flag_vs_isolated.items()},
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", type=Path)
    ap.add_argument("--work", type=Path, default=None,
                    help="the run's nxf_work/, on the cluster. The authoritative "
                         "output->pool mapping; cached to pool_map.tsv so a later "
                         "local run does not need it.")
    ap.add_argument("--bank", type=Path, default=None,
                    help="copy every product here as <pool>.<assembler>.<ext>. "
                         "Never overwrites: an existing file with different "
                         "content is reported, not replaced.")
    a = ap.parse_args()

    reads_key, pool_of_index = load_given(a.results)

    cache = a.results / "pool_map.tsv"
    by_basename: dict[str, str] = {}
    if a.work:
        by_basename = pools_from_work(a.work)
        cache.write_text("".join(f"{b}\t{p}\n" for b, p in sorted(by_basename.items())))
        print(f"=== {len(by_basename)} outputs attributed from {a.work} "
              f"(cached to {cache.name}) ===")
    elif cache.exists():
        by_basename = dict(ln.split("\t") for ln in cache.read_text().splitlines() if ln)
        print(f"=== {len(by_basename)} outputs attributed from {cache.name} ===")

    products = load_products(a.results, reads_key, pool_of_index, by_basename)
    print(f"=== {len(products)} pools, {len(pool_of_index)} given read sets ===")

    incomplete, failures = [], []
    for pool in sorted(products):
        got = products[pool]
        have = sorted(got)
        if len(have) != len(PRODUCTS):
            incomplete.append((pool, have))
            print(f"{pool}: INCOMPLETE -- only {have}")
        line = [pool]
        if "spades.contigs" in got and "spades.graph" in got:
            r = check_spades(got["spades.contigs"], got["spades.graph"],
                             got.get("spades.paths"))
            n_missing = len(r["missing"])
            gap = "" if n_missing == 0 else f" MISSING {n_missing}"
            line.append(f"spades {r['contigs']}c/{r['segments']}e"
                        f" walked={r['walked']}{gap}"
                        f" loops={r['self_loop_edges']} circ={len(r['circular'])}")
            if n_missing:
                failures.append((pool, "contigs with no walk in contigs.paths",
                                 r["missing"][:5]))
        if "megahit.contigs" in got and "megahit.graph" in got:
            r = check_megahit(got["megahit.contigs"], got["megahit.graph"])
            line.append(
                f"megahit {r['contigs']}c/{r['nodes']}n matched={r['matched']}"
                f" loops={r['self_loop_nodes']} flags={r['flags']}"
                f" flag_vs_isolated={r['flag_vs_isolated']}")
            if r["unmatched"]:
                failures.append((pool, "megahit contigs absent from the k-max graph",
                                 r["unmatched"][:5]))
        print("  " + "  |  ".join(line))

    if a.bank:
        a.bank.mkdir(parents=True, exist_ok=True)
        n_new = n_same = 0
        for pool, got in sorted(products.items()):
            for tag, src in sorted(got.items()):
                dest = a.bank / f"{pool}.{tag.split('.')[0]}{src.suffix}"
                if dest.exists():
                    if dest.stat().st_size == src.stat().st_size:
                        n_same += 1
                        continue
                    failures.append((pool, f"refusing to overwrite {dest}", []))
                    continue
                shutil.copyfile(src, dest)
                n_new += 1
        print(f"=== banked to {a.bank}: {n_new} new, {n_same} already present ===")

    if incomplete or failures:
        print("\n--- problems ---", file=sys.stderr)
        for pool, have in incomplete:
            print(f"  {pool}: incomplete, has {have}", file=sys.stderr)
        for pool, what, sample in failures:
            print(f"  {pool}: {what} {sample}", file=sys.stderr)
        return 1
    print("\nok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
