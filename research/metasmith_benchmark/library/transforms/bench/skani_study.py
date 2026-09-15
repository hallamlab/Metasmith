# skani ANI clusters over every bin in a study from the three binners, one of four interchangeable
# dereplicators. Clustering follows the standard skani_dedup.py: single linkage at 95 and 99 ANI,
# medoid by mean intra-cluster ANI.
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::skani.env"))
study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pair})
sets    = {label: model.AddRequirement(lib.GetType(f"sequences::{label}_bin_fasta"), parents={asm})
           for label in ("metabat2", "semibin2", "comebin")}
out     = model.AddProduct(lib.GetType("bench::skani_study_clusters"))

THRESHOLDS = (95.0, 99.0)
HEADER = "bin_id\tcluster_95\tis_centroid_95\tcluster_99\tis_centroid_99\n"


def clusters(names, ani, threshold):
    parent = {n: n for n in names}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for (a, b), v in ani.items():
        if v >= threshold:
            parent[find(a)] = find(b)
    groups = {}
    for n in names:
        groups.setdefault(find(n), []).append(n)
    result = {}
    for i, members in enumerate(sorted(groups.values()), start=1):
        mean = {m: sum(ani.get((m, o), 0.0) for o in members if o != m) / max(len(members) - 1, 1)
                for m in members}
        medoid = max(sorted(members), key=mean.get)
        for m in members:
            result[m] = (f"c{int(threshold)}_{i:05d}", int(m == medoid))
    return result


def protocol(context: ExecutionContext):
    Path("bins").mkdir()
    names = []
    for label, dep in sets.items():
        for k, ibin in enumerate(context.InputGroup(dep)):
            name = f"{label}__{k}_{ibin.local.stem}"
            shutil.copy(ibin.local, Path("bins") / f"{name}.fa")
            names.append(name)
    ounit = context.Output(out)
    if not names:
        Log.Warn("no bins; writing an empty table")
        ounit.local.write_text(HEADER)
        return ExecutionResult(manifest=[{out: ounit.local}], success=True)

    Path("bins.list").write_text("".join(f"bins/{n}.fa\n" for n in names))
    cpus = context.params.get("cpus") or 1
    context.ExecWithEnv(env=image, cmd=f"skani triangle -l bins.list --sparse -o ani.tsv -t {cpus}")

    ani = {}
    with open("ani.tsv") as f:
        header = f.readline().rstrip("\n").split("\t")
        ref, qry, col = header.index("Ref_file"), header.index("Query_file"), header.index("ANI")
        for line in f:
            row = line.rstrip("\n").split("\t")
            a, b = Path(row[ref]).stem, Path(row[qry]).stem
            if a != b:
                ani[(a, b)] = ani[(b, a)] = float(row[col])
    by_threshold = [clusters(names, ani, t) for t in THRESHOLDS]
    with open(ounit.local, "w") as f:
        f.write(HEADER)
        for n in sorted(names):
            f.write("\t".join([n, *(f"{c[n][0]}\t{c[n][1]}" for c in by_threshold)]) + "\n")

    return ExecutionResult(manifest=[{out: ounit.local}], success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=8)),
)
