"""Pure-ANI clustering of pre-filtered quality bins.

Input: `quality_bin_fasta` (emitted by aggregator). Runs skani triangle, builds
clusters at 95% (species) and 99% (strain) ANI, picks medoid per cluster as
centroid (the bin with the highest mean ANI to other members — most central).

No checkm/gtdbtk consulted here; quality decisions live in the aggregator.
"""
import shutil
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::skani.env"))
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
bin   = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
table = model.AddProduct(lib.GetType("binning_local::cluster_table"))

ANI_THRESHOLDS = [95.0, 99.0]


class _UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[ra] = rb


def protocol(context: ExecutionContext):
    bin_paths = list(context.InputGroup(bin))
    stems = [p.local.stem for p in bin_paths]

    itable = context.Output(table)

    if not bin_paths:
        Log.Warn("no quality bins; emitting empty cluster_table")
        with open(itable.local, "w") as f:
            f.write("bin_id\tcluster_95\tis_centroid_95\tcluster_99\tis_centroid_99\tmean_intra_ani_95\n")
        return ExecutionResult(manifest=[{table: itable.local}], success=True)

    # Stage bins for skani
    staged = Path("staged_bins")
    staged.mkdir()
    bins_list = Path("bins.list")
    with open(bins_list, "w") as lf:
        for p in bin_paths:
            dest = staged / f"{p.local.stem}.fna"
            shutil.copy(p.local, dest, follow_symlinks=True)
            lf.write(f"{dest.resolve()}\n")

    threads = context.params.get("cpus", 8)
    ani_tsv = "skani_ani.tsv"
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"skani triangle -l {bins_list} --sparse -o {ani_tsv} -t {threads}",
    )

    # Parse skani edges; build symmetric ANI map for medoid scoring.
    ani_map: dict[tuple[str, str], float] = {}
    with open(ani_tsv) as f:
        header = f.readline().rstrip("\n").split("\t")
        try:
            ani_idx = header.index("ANI")
            ref_idx = header.index("Ref_file")
            qry_idx = header.index("Query_file")
        except ValueError:
            ani_idx, ref_idx, qry_idx = 2, 0, 1
        for line in f:
            toks = line.rstrip("\n").split("\t")
            if len(toks) <= max(ani_idx, ref_idx, qry_idx):
                continue
            try:
                ani = float(toks[ani_idx])
            except ValueError:
                continue
            a = Path(toks[ref_idx]).stem
            b = Path(toks[qry_idx]).stem
            if a == b:
                continue
            ani_map[(a, b)] = ani
            ani_map[(b, a)] = ani

    cluster_id: dict[str, dict[float, str]] = {s: {} for s in stems}
    is_centroid: dict[str, dict[float, bool]] = {s: {} for s in stems}
    mean_ani_95: dict[str, float] = {s: 0.0 for s in stems}

    for t in ANI_THRESHOLDS:
        uf = _UnionFind(stems)
        for (a, b), ani in ani_map.items():
            if ani >= t:
                uf.union(a, b)
        clusters: dict[str, list[str]] = {}
        for s in stems:
            clusters.setdefault(uf.find(s), []).append(s)

        for ci, (root, members) in enumerate(sorted(clusters.items()), start=1):
            cid = f"c{int(t)}_{ci:05d}"
            # Medoid: member with highest mean ANI to other cluster members.
            # Singletons trivially pass.
            if len(members) == 1:
                medoid = members[0]
                medoid_score = 100.0
            else:
                scores = {}
                for m in members:
                    vals = [ani_map.get((m, o), 0.0) for o in members if o != m]
                    scores[m] = sum(vals) / len(vals) if vals else 0.0
                medoid = max(scores, key=lambda x: scores[x])
                medoid_score = scores[medoid]

            for m in members:
                cluster_id[m][t] = cid
                is_centroid[m][t] = (m == medoid)
            if t == 95.0:
                mean_ani_95[medoid] = medoid_score
                for m in members:
                    if m != medoid:
                        # Mean ANI of this member to other members of its cluster
                        if len(members) > 1:
                            vals = [ani_map.get((m, o), 0.0) for o in members if o != m]
                            mean_ani_95[m] = sum(vals) / len(vals)

    with open(itable.local, "w") as f:
        f.write("bin_id\tcluster_95\tis_centroid_95\tcluster_99\tis_centroid_99\tmean_intra_ani_95\n")
        for s in sorted(stems):
            f.write("\t".join([
                s,
                cluster_id[s][95.0],
                "1" if is_centroid[s][95.0] else "0",
                cluster_id[s][99.0],
                "1" if is_centroid[s][99.0] else "0",
                f"{mean_ani_95[s]:.3f}",
            ]) + "\n")

    return ExecutionResult(
        manifest=[{table: itable.local}],
        success=itable.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)
