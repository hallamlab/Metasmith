from metasmith.python_api import *
import csv
import gzip
from collections import defaultdict

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
orfs      = model.AddRequirement(lib.GetType("sequences::bin_orfs"))
diamond   = model.AddRequirement(lib.GetType("bench::bigg_hits_diamond"), parents={orfs})
kofam     = model.AddRequirement(lib.GetType("annotation::kofamscan_results_chunk"), parents={orfs})
reference = model.AddRequirement(lib.GetType("bench::bigg_gene_kos"))
gprs      = model.AddRequirement(lib.GetType("e4abl::carveme166_bigg_gprs"))
out       = model.AddProduct(lib.GetType("bench::bigg_hits_diluted_top1"))


def best_kos(path):
    best = {}
    with open(path) as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            score = float(r["score"])
            top, kos = best.get(r["bigg_gene"], (float("-inf"), set()))
            if score > top:
                best[r["bigg_gene"]] = (score, {r["KO"]})
            elif score == top:
                kos.add(r["KO"])
    genes_of = defaultdict(set)
    for gene, (_, kos) in best.items():
        for ko in kos:
            genes_of[ko].add(gene)
    return genes_of


# carve names a GPR gene "<model>.<gene id without its G_ prefix>", the form DIAMOND's subjects take.
def reactions_of_genes(path):
    reactions = defaultdict(set)
    with gzip.open(path, "rt") as fh:
        for r in csv.DictReader(fh):
            reactions[f"{r['model']}.{r['gene'][2:]}"].add(r["reaction"])
    return reactions


# Each lane splits one unit of belief per ORF over its nominations by score, and each nomination splits
# its share evenly over the distinct reactions it names (ECSPr's dilution). A nomination naming no
# reaction keeps its share, so an ORF that looks like something CarveMe cannot use stays weak.
def dilute(nominations, reactions_of):
    total = sum(score for _, score in nominations)
    belief = defaultdict(float)
    for key, score in nominations:
        named = reactions_of(key)
        for r in named:
            belief[r] += score / total / len(named)
    return belief


def protocol(context: ExecutionContext):
    idiamond   = context.Input(diamond)
    ikofam     = context.Input(kofam)
    ireference = context.Input(reference)
    igprs      = context.Input(gprs)
    iout       = context.Output(out)

    gene_reactions = reactions_of_genes(igprs.local)
    genes_of = best_kos(ireference.local)
    ko_reactions = {ko: set().union(*(gene_reactions.get(g, set()) for g in genes)) for ko, genes in genes_of.items()}

    hits = defaultdict(dict)
    with open(idiamond.local) as fh:
        for line in fh:
            fields = line.rstrip("\n").split("\t")
            hits[fields[0]][fields[1]] = max(hits[fields[0]].get(fields[1], 0.0), float(fields[11]))
    calls = defaultdict(dict)
    with open(ikofam.local) as fh:
        for row in csv.DictReader(fh):
            try:
                score, above = float(row["score"]), float(row["score"]) >= float(row["thrshld"])
            except ValueError:
                continue
            if above:
                calls[row["gene_name"]][row["KO"]] = max(calls[row["gene_name"]].get(row["KO"], 0.0), score)

    won, rows = defaultdict(int), 0
    with open(iout.local, "w") as fout:
        for orf in sorted(set(hits) | set(calls)):
            b_d = dilute(hits[orf].items(), lambda g: gene_reactions.get(g, ()))
            b_k = dilute(calls[orf].items(), lambda ko: ko_reactions.get(ko, ()))
            top_d, top_k = max(b_d.values(), default=0.0), max(b_k.values(), default=0.0)
            if top_d == top_k == 0.0:
                continue
            # KOfam must beat DIAMOND outright to take the ORF.
            if top_k > top_d:
                lane, top = "kofam", top_k
                best = {r for r, b in b_k.items() if b == top}
                genes = {g for ko in calls[orf] for g in genes_of.get(ko, ()) if gene_reactions.get(g, set()) & best}
            else:
                lane, top = "diamond", top_d
                best = {r for r, b in b_d.items() if b == top}
                genes = {g for g in hits[orf] if gene_reactions.get(g, set()) & best}
            won[lane] += 1
            for g in sorted(genes):
                fout.write(f"{orf}\t{g}\t0\t0\t0\t0\t0\t0\t0\t0\t0\t{top:.6g}\n")
                rows += 1
    print(f"[hits_diluted_top1] {len(set(hits) | set(calls)):,} ORFs, DIAMOND wins {won['diamond']:,}, "
          f"KOfam wins {won['kofam']:,}, {rows:,} rows", flush=True)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)
