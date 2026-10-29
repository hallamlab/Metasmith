from metasmith.python_api import *
import csv
from collections import defaultdict

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
results   = model.AddRequirement(lib.GetType("annotation::kofamscan_results_chunk"))
reference = model.AddRequirement(lib.GetType("bench::bigg_gene_kos"))
out       = model.AddProduct(lib.GetType("bench::bigg_hits_kofam"))


# DIAMOND's 12-column table, which carve reads by position and keeps only query, subject and score of.
# The other columns carry placeholders.
def protocol(context: ExecutionContext):
    iresults   = context.Input(results)
    ireference = context.Input(reference)
    iout       = context.Output(out)

    genes_of = defaultdict(set)
    with open(ireference.local) as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            genes_of[row["KO"]].add(row["bigg_gene"])

    # kofamscan.py also keeps a hit whose KO has no threshold. Only a hit above a threshold counts here,
    # as on the reference side.
    orf_kos, rows, expansions = set(), 0, []
    with open(iresults.local) as fh, open(iout.local, "w") as fout:
        for row in csv.DictReader(fh):
            try:
                above = float(row["score"]) >= float(row["thrshld"])
            except ValueError:
                above = False
            if not above or (row["gene_name"], row["KO"]) in orf_kos:
                continue
            orf_kos.add((row["gene_name"], row["KO"]))
            targets = sorted(genes_of.get(row["KO"], ()))
            expansions.append(len(targets))
            for bigg_gene in targets:
                fout.write(f"{row['gene_name']}\t{bigg_gene}\t0\t0\t0\t0\t0\t0\t0\t0\t{row['E-value']}\t{row['score']}\n")
                rows += 1
    print(f"[hits_from_kofam] {len(orf_kos):,} ORF-KO pairs above threshold, "
          f"{sum(1 for n in expansions if n):,} with a BiGG gene, {rows:,} rows", flush=True)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=results,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)
