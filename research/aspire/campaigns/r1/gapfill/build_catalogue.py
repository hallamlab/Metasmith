#!/usr/bin/env python3
"""Unified bin catalogue over every metasmith run plus the gap-filled bins.

Deliberately does NOT reproduce the metasmith aggregator's requirement that all
three binners have produced bins for an assembly before any of them count. That
gate is why `results/*-quality_bin_fasta` holds 45 MAGs while more bins than that
pass the same completeness/contamination thresholds. Here each bin is judged on
its own.

Attribution comes from `nxf_attribution.py`, which reconstructs sample and
transform from the input list each nextflow task recorded in its own
`.command.sh`. That replaces the hand-built `inv/*.tsv` work-dir join this used
to read: the two were cross-checked over all 781 QkqCNJOo bins and agreed on
every one, and the attribution route generalises to any run dir, which the join
did not -- it was written against one run key.

Gap-filled bins carry their sample and binner in the filename by construction.

  python3 build_catalogue.py [run_key ...]      # default: QkqCNJOo + hEYVT7HY
  REFRESH=1 python3 build_catalogue.py ...      # re-walk nxf_work rather than cache
"""
import os, csv, sys, glob, collections, subprocess

MSM  = "/scratch/phyberos/gmcf3495/metasmith/runs"
INV  = "/scratch/phyberos/gmcf3495/inv"
G    = "/scratch/phyberos/gmcf3495/gapfill"
ATTR = "/scratch/phyberos/gmcf3495/pw/nxf_attribution.py"
BINNERS = ("metabat2", "semibin2", "comebin")
MIN_COMP, MAX_CONT = 50.0, 10.0
# The two runs that produced r1 bins: the main DAG, and the scoped run that
# carried S13 and S22 after the library-drop race lost them.
RUNS = sys.argv[1:] or ["QkqCNJOo", "hEYVT7HY"]


def attribution(key):
    """product basename -> (sample, transform), cached per run key.

    Walking nxf_work is thousands of small reads, so it is cached; set REFRESH=1
    when a run has produced tasks since the cache was written.
    """
    cache = f"{INV}/attrib_{key}.tsv"
    if os.environ.get("REFRESH") or not os.path.exists(cache):
        with open(cache, "w") as fh:
            subprocess.run([sys.executable, ATTR, f"{MSM}/{key}"],
                           stdout=fh, check=True)
    out = {}
    with open(cache) as fh:
        next(fh, None)
        for line in fh:
            f = line.rstrip("\n").split("\t")
            if len(f) == 5:
                out[f[0]] = (f[1], f[2])
    return out


def checkm_index(paths):
    """bin stem -> (completeness, contamination) from CheckM2 quality reports."""
    ck = {}
    for f in paths:
        try:
            rows = list(csv.DictReader(open(f), delimiter="\t"))
        except Exception:
            continue
        for row in rows:
            b = row.get("Bin Id") or row.get("Name")
            if not b:
                continue
            try:
                ck[b] = (float(row["Completeness"]), float(row["Contamination"]))
            except Exception:
                pass
    return ck


rows, seen = [], set()
for key in RUNS:
    root = f"{MSM}/{key}"
    if not os.path.isdir(f"{root}/results"):
        print(f"skip {key}: no results tree", file=sys.stderr)
        continue
    att = attribution(key)
    ck = checkm_index(glob.glob(f"{root}/results/*checkm_stats/*"))
    path = {}
    for b in BINNERS:
        for f in glob.glob(f"{root}/results/*{b}_bin_fasta/*"):
            path[os.path.basename(f).rsplit(".", 1)[0]] = f
    n = 0
    for product, (sample, transform) in att.items():
        if transform not in BINNERS or not product.endswith((".fna", ".fa")):
            continue
        stem = product.rsplit(".", 1)[0]
        if stem not in path:      # produced in a work dir but never published
            continue
        m = ck.get(stem)
        rows.append(dict(uid=stem, sample=sample, binner=transform, src="metasmith",
                         path=path[stem], comp=m[0] if m else None,
                         cont=m[1] if m else None, run=key))
        seen.add((sample, transform))
        n += 1
    print(f"{key}: {n} published bin(s)", file=sys.stderr)

# --- gap-filled bins ---
ck_gf = checkm_index(glob.glob(f"{G}/checkm/*/*/checkm2_out/quality_report.tsv"))
dupes = collections.Counter()
for f in sorted(glob.glob(f"{G}/bins/*/*/*.fna")):
    stem = os.path.basename(f)[:-4]
    parts = stem.split("__")
    if len(parts) < 3:
        continue
    s, b = parts[0], parts[1]
    # A gap-fill run over a pair the pipeline already binned would count that
    # sample twice -- which is what the comebin S31 control did before it was
    # deleted. Refuse and say so rather than inflate the MAG count.
    if (s, b) in seen:
        dupes[(s, b)] += 1
        continue
    m = ck_gf.get(stem)
    rows.append(dict(uid=stem, sample=s, binner=b, src="gapfill", path=f,
                     comp=m[0] if m else None, cont=m[1] if m else None, run=""))
for (s, b), n in sorted(dupes.items()):
    print(f"WARNING: skipped {n} gap-fill {b} bin(s) for {s}; the pipeline "
          f"already produced bins for that pair", file=sys.stderr)

with open(f"{G}/catalogue.tsv", "w") as out:
    out.write("uid\tsample\tbinner\tsrc\tcompleteness\tcontamination\tquality\tpath\n")
    for r in rows:
        q = "" if r["comp"] is None else int(r["comp"] >= MIN_COMP and r["cont"] <= MAX_CONT)
        out.write(f"{r['uid']}\t{r['sample']}\t{r['binner']}\t{r['src']}\t"
                  f"{'' if r['comp'] is None else r['comp']}\t"
                  f"{'' if r['cont'] is None else r['cont']}\t{q}\t{r['path']}\n")

# `extra_bins.tsv` is the hook binner_status.py reads to see bins produced by a
# run other than QkqCNJOo, which its work-dir join was built against.
with open(f"{INV}/extra_bins.tsv", "w") as out:
    for r in rows:
        if r["src"] == "metasmith" and r["run"] != "QkqCNJOo":
            out.write(f"{r['sample']}\t{r['binner']}\n")

tot = collections.Counter(); qual = collections.Counter(); nock = 0
for r in rows:
    tot[(r["src"], r["binner"])] += 1
    if r["comp"] is None:
        nock += 1
    elif r["comp"] >= MIN_COMP and r["cont"] <= MAX_CONT:
        qual[r["sample"]] += 1
print(f"bins total {len(rows)}  (awaiting checkm: {nock})")
for k in sorted(tot):
    print(f"  {k[0]:10} {k[1]:10} {tot[k]}")
print(f"\nQUALITY MAGs: {sum(qual.values())}  across {len(qual)} samples")
for s, n in sorted(qual.items(), key=lambda x: -x[1]):
    print(f"  {s:6} {n}")
