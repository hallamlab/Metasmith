import sys, os
sys.path.insert(0, "/scratch/phyberos/bench/e5_mags")
from e5_assemblies import HOME, read_index, sample_of
from e5_mags import bin_of
RUN = "Zze0nvLA"
KINDS = {"modelling-carveme_model": "carveme_sbml", "sequences-bin_orfs": "mag_orfs",
         "modelling-memote_score": "memote", "bench-checkm2_quality": "checkm2"}
files = {}
with open("/scratch/phyberos/bench/e5_mags/evict_files.txt") as f:
    for line in f:
        p, s = line.rsplit(" ", 1)
        if "/out/" in p:
            files[os.path.basename(p)] = (p, int(s))
parents = read_index(HOME / "runs" / RUN / "results" / "_metadata" / "index.yml")
print("study\tsample\tbin\tkind\tfile\tcache_path\tsize")
miss = 0
for key, ps in parents.items():
    d = key.split("/", 1)[0]
    todo = []
    if d in KINDS:
        todo.append((KINDS[d], key.split("/", 1)[1]))
        if d == "modelling-carveme_model":
            todo += [("bigg_hits", p.split("/", 1)[1]) for p in ps if p.startswith("out/") and p.endswith(".tsv")]
    elif d == "sequences-das_tool_bin_fasta":
        todo.append(("mag_fasta", key.split("/", 1)[1]))
    if not todo:
        continue
    b = bin_of(key, parents)
    study, sample = sample_of(b, parents)
    if not study.startswith("toy_hmp"):
        continue
    for kind, fn in todo:
        if kind == "mag_fasta":
            p = HOME / "runs" / RUN / "results" / key
            hit = (str(p.resolve()), p.stat().st_size) if p.exists() else None
        else:
            hit = files.get(fn)
        if hit is None:
            miss += 1; hit = ("MISSING", 0)
        print(study, sample, b.split("/", 1)[1], kind, fn, *hit, sep="\t")
print("missing", miss, file=sys.stderr)
