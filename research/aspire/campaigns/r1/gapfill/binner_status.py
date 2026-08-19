#!/usr/bin/env python3
import os, re, glob, sys, collections

R    = "/scratch/phyberos/gmcf3495/metasmith/runs/QkqCNJOo"
INV  = "/scratch/phyberos/gmcf3495/inv"
G    = "/scratch/phyberos/gmcf3495/gapfill"
BINNERS = ("metabat2", "semibin2", "comebin")

rd = lambda p: [l.rstrip("\n").split("\t") for l in open(p) if l.strip()]

s2b   = {os.path.basename(w): s for s, w in rd(f"{INV}/sample2bbduk.tsv")}
asm2s = {a: s2b.get(os.path.basename(r.rstrip("/")), "?") for a, r, _ in rd(f"{INV}/megahit.tsv")}
samples = sorted({s for s in s2b.values()},
                 key=lambda s: (s != "NTC", int(re.sub(r"\D", "", s) or 0)))

usable = {s: (int(t), int(u)) for s, t, u in rd(f"{INV}/usable_contigs.tsv")}

ms = collections.Counter()
for stem, b, asm in rd(f"{INV}/bin_registry_raw.tsv"):
    ms[(asm2s.get(asm, "?"), b)] += 1

if os.path.exists(f"{INV}/extra_bins.tsv"):
    for s, b in rd(f"{INV}/extra_bins.tsv"):
        ms[(s, b)] += 1

state = collections.defaultdict(set)
for asm, b, st, _wd in rd(f"{INV}/binners_all.tsv"):
    state[(asm2s.get(asm, "?"), b)].add(st)
cbclass = {}
for asm, cls in rd(f"{INV}/comebin_class.tsv"):
    if cls.startswith("OTHER:") and cbclass.get(asm2s.get(asm, "?")):
        continue
    cbclass[asm2s.get(asm, "?")] = cls

CAUSE_PATTERNS = [
    (r"but all are shorter than (\d+) basepairs",  "ALL_CONTIGS_UNDER_{0}BP"),
    (r"but only (\d+) contain\(s\) at least (\d+) basepairs",
                                                   "ONLY_{0}_CONTIGS_OVER_{1}BP"),
    (r"Number of bins prior to reclustering: 0",   "SEMIBIN_NO_BINS_PRE_RECLUSTER"),
    (r"0 bins \(0 bases in total\) formed",        "METABAT2_NO_BINS_FORMED"),
    (r"Negative coverage depth is not allowed",    "DEPTH_GARBAGE_UNSANITISED"),
    (r"local variable 'logits'",                   "COMEBIN_LOGITS_BATCH_TOO_LARGE"),
    (r"Missing quality table|unitem_profile",      "COMEBIN_NO_MARKER_SEEDS"),
    (r"DUE TO TIME LIMIT|CANCELLED AT",            "TIMEOUT"),
    (r"Illegal option --",                         "WRAPPER_WHICH_FUNCTION_LEAK"),
]


INFLIGHT = set(os.popen("squeue -u phyberos -h -o '%j'").read().split())


def gapfill(sample, binner):
    collected = len(glob.glob(f"{G}/bins/{sample}/{binner}/*.fna"))
    if collected:
        return collected, ""
    dirs = [f"{G}/out/{sample}/{binner}"]
    if binner == "metabat2":
        dirs += [f"{G}/out/{sample}/metabat2fix", f"{G}/out/{sample}/metabat2sub"]
    best = None
    for d in dirs:
        if not os.path.isdir(d):
            continue
        n = len(glob.glob(f"{d}/bins/*.fa") + glob.glob(f"{d}/output_bins/*.fa.gz")
                + glob.glob(f"{d}/comebin_res/comebin_res_bins/*.fa"))
        if os.path.exists(f"{d}/skipped.txt"):
            return 0, open(f"{d}/skipped.txt").read().split("\t")[2].strip()
        if best is None or n > best[0]:
            best = (n, os.path.basename(d))
    if best is None:
        return None, None
    if best[0] > 0:
        return best[0], ""
    variant = best[1]
    for v in {variant, binner}:
        if f"gf_{v}_{sample}" in INFLIGHT:
            return 0, "IN_FLIGHT"
    logs = sorted(glob.glob(f"{G}/logs/{variant}_{sample}.*.err")
                  + glob.glob(f"{G}/logs/{variant}_{sample}.*.out"),
                  key=os.path.getmtime, reverse=True)
    blob = "".join(open(p, errors="replace").read() for p in logs[:2])
    for pat, cause in CAUSE_PATTERNS:
        m = re.search(pat, blob)
        if m:
            return 0, cause.format(*m.groups()) if m.groups() else cause
    return 0, "UNKNOWN_SEE_LOGS"


PIPE_CAUSE = {
    "LOGITS":         "COMEBIN_LOGITS_BATCH_TOO_LARGE",
    "UNITEM_NO_BINS": "COMEBIN_NO_MARKER_SEEDS",
    "EMPTY":          "TIMEOUT_8H_WALL",
}

out = [("sample", "total_contigs", "usable_contigs", "binner",
        "n_bins", "source", "status", "cause")]
for s in samples:
    tot, use = usable.get(s, ("", ""))
    for b in BINNERS:
        n_ms = ms.get((s, b), 0)
        n_gf, cause_gf = gapfill(s, b)
        if n_ms > 0 and n_ms >= (n_gf or 0):
            out.append((s, tot, use, b, n_ms, "metasmith", "BINS", ""))
        elif n_gf:
            out.append((s, tot, use, b, n_gf, "gapfill", "BINS", ""))
        elif n_gf == 0:
            out.append((s, tot, use, b, 0, "gapfill", "ZERO", cause_gf))
        else:
            st = state.get((s, b), set())
            if not st:
                out.append((s, tot, use, b, "", "-", "NEVER_ATTEMPTED", ""))
            else:
                c = PIPE_CAUSE.get(cbclass.get(s, ""), "") if b == "comebin" else ""
                out.append((s, tot, use, b, 0, "metasmith", "ZERO",
                            c or ("DEPTH_GARBAGE_UNSANITISED" if b == "metabat2"
                                  else "PIPELINE_FAILED_SEE_TRACE")))

with open(f"{G}/binner_status.tsv", "w") as fh:
    for r in out:
        fh.write("\t".join(str(x) for x in r) + "\n")

w = collections.Counter(r[6] for r in out[1:])
print(f"{len(out)-1} (sample,binner) cells: " +
      "  ".join(f"{k}={v}" for k, v in sorted(w.items())))
print()
for r in out:
    print("\t".join(str(x) for x in r))
