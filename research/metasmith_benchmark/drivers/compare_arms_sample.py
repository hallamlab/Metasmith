"""Collect one sample's raw E1-vs-E2 comparison inputs: assembly statistics and contig sharing by
sequence, each arm's MAG inventory, and skani E2-vs-E1 pair tables per binner.

Reads E1 from the e1_close sheet and maps, and E2 from the task cache through the archive manifest.
Never writes the task cache. compare_arms.py turns these files into the comparison tables.
"""
import argparse
import csv
import gzip
import hashlib
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e1_close import BINNERS, bin_stem, read_fasta, read_tsv, sheet_rows  # noqa: E402

COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")
MIN_IDENTITY = 0.99


def canonical(seq):
    s = seq.upper()
    rc = s.translate(COMPLEMENT)[::-1]
    return min(s, rc)


def digest(text):
    return hashlib.sha1(text.encode()).hexdigest()


def file_sha256(path):
    h = hashlib.sha256()
    with (gzip.open if str(path).endswith(".gz") else open)(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def stats(seqs):
    lengths = sorted((len(s) for s in seqs.values()), reverse=True)
    total = sum(lengths)
    acc, n50, l50 = 0, 0, 0
    for i, n in enumerate(lengths, 1):
        acc += n
        if acc * 2 >= total:
            n50, l50 = n, i
            break
    gc = sum(s.upper().count("G") + s.upper().count("C") for s in seqs.values())
    return {"contigs": len(lengths), "bp": total, "n50": n50, "l50": l50,
            "longest": lengths[0] if lengths else 0, "gc_pct": f"{100 * gc / total:.3f}" if total else ""}


def assembly(e1_path, e2_path):
    arms = {"e1": read_fasta(e1_path), "e2": read_fasta(e2_path)}
    row = {}
    exact, canon, names = {}, {}, {}
    for a, seqs in arms.items():
        for k, v in stats(seqs).items():
            row[f"{a}_{k}"] = v
        row[f"{a}_sha256"] = file_sha256(e1_path if a == "e1" else e2_path)
        ordered = sorted(s.upper() for s in seqs.values())
        row[f"{a}_seqset_sha256"] = hashlib.sha256("\n".join(ordered).encode()).hexdigest()
        exact[a] = {digest(s.upper()): len(s) for s in seqs.values()}
        names[a] = {n: digest(canonical(s)) for n, s in seqs.items()}
        canon[a] = {h: len(seqs[n]) for n, h in names[a].items()}
    if row["e1_sha256"] == row["e2_sha256"]:
        row["verdict"] = "byte-identical"
    elif row["e1_seqset_sha256"] == row["e2_seqset_sha256"]:
        row["verdict"] = "sequence-set-identical"
    else:
        row["verdict"] = "different"
    shared_exact = exact["e1"].keys() & exact["e2"].keys()
    shared = canon["e1"].keys() & canon["e2"].keys()
    row["shared_contigs_exact"] = len(shared_exact)
    row["shared_contigs_canonical"] = len(shared)
    for a in ("e1", "e2"):
        bp = sum(canon[a][h] for h in shared)
        row[f"{a}_shared_contig_frac"] = f"{len(shared) / len(canon[a]):.6f}"
        row[f"{a}_shared_bp_frac"] = f"{bp / row[f'{a}_bp']:.6f}"
        row[f"{a}_unshared_contigs"] = len(canon[a]) - len(shared)
        row[f"{a}_unshared_bp"] = row[f"{a}_bp"] - bp
    unshared = {a: {n: arms[a][n] for n, h in names[a].items() if h not in shared} for a in arms}
    return row, unshared


def near_matches(sif, threads, unshared, tmp, binds):
    for a in unshared:
        with open(tmp / f"{a}.unshared.fa", "w") as fh:
            fh.writelines(f">{n}\n{s}\n" for n, s in unshared[a].items())
    for query, target in (("e2", "e1"), ("e1", "e2")):
        best, spans = {}, {}
        if unshared[query] and unshared[target]:
            paf = subprocess.run(
                ["apptainer", "exec", "--no-home", "--cleanenv"] + [x for b in binds for x in ("-B", b)] + [
                 sif, "minimap2", "-x", "asm5", "-c", "--secondary=no", "-t", str(threads),
                 str(tmp / f"{target}.unshared.fa"), str(tmp / f"{query}.unshared.fa")],
                check=True, capture_output=True, text=True).stdout
            for line in paf.splitlines():
                f = line.split("\t")
                q, qs, qe, tname, matches, block = f[0], int(f[2]), int(f[3]), f[5], int(f[9]), int(f[10])
                if q not in best or matches > best[q][1]:
                    best[q] = (tname, matches, block, qe - qs)
                if matches >= MIN_IDENTITY * block:
                    spans.setdefault(q, []).append((qs, qe))
        for n, s in unshared[query].items():
            tname, matches, block, span = best.get(n, ("", 0, 0, 0))
            yield {"pipeline": query.upper(), "contig": n, "length": len(s), "best_target": tname,
                   "identity": f"{matches / block:.6f}" if block else "", "cov_best": f"{span / len(s):.6f}",
                   "cov_id99": f"{union(spans.get(n, [])) / len(s):.6f}"}


def union(intervals):
    covered, end = 0, -1
    for s, e in sorted(intervals):
        if e > end:
            covered += e - max(s, end)
            end = e
    return covered


def write_rows(path, rows, fieldnames=None):
    rows = list(rows)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else fieldnames, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def e1_bins(sample_row, maps, outdir, seqs):
    members = {}
    for r in read_tsv(maps / f"{sample_row['sample']}.tsv"):
        if "unbinned" not in r["bin_id"].lower():
            members.setdefault((r["binner"], bin_stem(r["bin_id"])), []).append(r["contig_id"].split()[0])
    paths = {}
    for (binner, name), contigs in members.items():
        d = outdir / "e1" / binner
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{name}.fa"
        with open(p, "w") as fh:
            for c in contigs:
                fh.write(f">{c}\n{seqs[c]}\n")
        paths.setdefault(binner, {})[name] = p
    return paths


def e2_bins(rows, cache, outdir):
    paths = {}
    for r in rows:
        if r["kind"] != "bin":
            continue
        name = os.path.basename(r["archive_name"])[:-3]
        d = outdir / "e2" / r["binner"]
        d.mkdir(parents=True, exist_ok=True)
        link = d / f"{name}.fa"
        link.symlink_to(Path(cache, r["shard"][:2], r["shard"][2:], r["relpath"]))
        paths.setdefault(r["binner"], {})[name] = link
    return paths


def inventory(pipeline, paths):
    for binner, bins in paths.items():
        for name, p in bins.items():
            seqs = read_fasta(p)
            yield {"pipeline": pipeline, "binner": binner, "bin": name, "contigs": len(seqs),
                   "bp": sum(len(s) for s in seqs.values())}


def skani(sif, threads, e1, e2, tmp, binds):
    by_path = {}
    for pipeline, bins in (("e1", e1), ("e2", e2)):
        lst = tmp / f"{pipeline}.list"
        lst.write_text("".join(f"{p}\n" for p in bins.values()))
        by_path.update({str(p): n for n, p in bins.items()})
    out = tmp / "skani.tsv"
    cmd = ["apptainer", "exec", "--no-home", "--cleanenv"] + [x for b in binds for x in ("-B", b)] + [
        sif, "skani", "dist", "-t", str(threads), "--min-af", "0",
        "--ql", str(tmp / "e2.list"), "--rl", str(tmp / "e1.list"), "-o", str(out)]
    subprocess.run(cmd, check=True)
    for r in read_tsv(out):
        yield {"e2_bin": by_path[r["Query_file"]], "e1_bin": by_path[r["Ref_file"]], "ani": r["ANI"],
               "af_e2": r["Align_fraction_query"], "af_e1": r["Align_fraction_ref"]}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sheet", type=Path, required=True)
    p.add_argument("--maps", type=Path, required=True)
    p.add_argument("--manifest", type=Path, required=True)
    p.add_argument("--cache", type=Path, required=True)
    p.add_argument("--skani-sif", required=True)
    p.add_argument("--minimap2-sif", required=True)
    p.add_argument("--index", type=int, required=True)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--tmp", type=Path, required=True)
    p.add_argument("--outdir", type=Path, required=True)
    a = p.parse_args()

    row = next(r for r in sheet_rows(a.sheet).values() if int(r["idx"]) == a.index)
    s = row["sample"]
    rows = [r for r in read_tsv(a.manifest) if r["sample"] == s]
    asm2, = [r for r in rows if r["kind"] == "assembly"]
    e2_asm = Path(a.cache, asm2["shard"][:2], asm2["shard"][2:], asm2["relpath"])
    out = a.outdir / s
    out.mkdir(parents=True, exist_ok=True)

    asm_row, unshared = assembly(row["assembly"], e2_asm)
    write_rows(out / "assembly.tsv", [{"arm": row["arm"], "sample": s, **asm_row}])
    binds = ["/scratch/phyberos", str(a.tmp)]
    write_rows(out / "contig_matches.tsv", ({"arm": row["arm"], "sample": s, **r} for r in
                                            near_matches(a.minimap2_sif, a.threads, unshared, a.tmp, binds)),
               ["arm", "sample", "pipeline", "contig", "length", "best_target", "identity", "cov_best", "cov_id99"])
    print(f"{s}: assembly done", flush=True)

    e1 = e1_bins(row, a.maps, a.tmp, read_fasta(row["assembly"]))
    e2 = e2_bins(rows, a.cache, a.tmp)
    assert set(e1) == set(e2) == set(BINNERS), (sorted(e1), sorted(e2))
    write_rows(out / "mags.tsv", ({"arm": row["arm"], "sample": s, **r}
                                  for r in [*inventory("E1", e1), *inventory("E2", e2)]))
    pairs = []
    for b in BINNERS:
        tmp = a.tmp / f"skani_{b}"
        tmp.mkdir()
        pairs += [{"arm": row["arm"], "sample": s, "binner": b, **r}
                  for r in skani(a.skani_sif, a.threads, e1[b], e2[b], tmp, binds)]
        print(f"{s}: skani {b}: {len(e1[b])} E1 x {len(e2[b])} E2 bins", flush=True)
    write_rows(out / "skani_pairs.tsv", pairs, ["arm", "sample", "binner", "e2_bin", "e1_bin", "ani", "af_e2", "af_e1"])


if __name__ == "__main__":
    main()
