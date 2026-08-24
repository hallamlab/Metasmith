from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
ecs   = model.AddRequirement(lib.GetType("annotation::deepec_predictions"))
kos   = model.AddRequirement(lib.GetType("annotation::kofamscan_results"))
hits  = model.AddRequirement(lib.GetType("annotation::diamond_uniref50_results"))
out   = model.AddProduct(lib.GetType("annotation::ptools_annotation_table"))


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iecs  = context.Input(ecs)
    ikos  = context.Input(kos)
    ihits = context.Input(hits)
    iout  = context.Output(out)

    script = f"""
import csv
import pandas as pd
import polars as pl

orf_ids = []
with open("{iorfs.container}") as fh:
    for line in fh:
        if line.startswith(">"):
            orf_ids.append(line[1:].split()[0])

rows = []

# DeepEC: TSV; first column = gene id, second column = predicted EC.
# Skip header / rows whose second field is not a dotted EC.
with open("{iecs.container}") as fh:
    reader = csv.reader(fh, delimiter="\\t")
    for r in reader:
        if len(r) < 2:
            continue
        gene, ec = r[0].strip(), r[1].strip()
        if ec.count(".") < 1 or ec.lower().startswith("predicted"):
            continue
        rows.append({{"orf_id": gene, "kind": "EC", "value": ec,
                      "score": None, "confidence": None}})

# KofamScan: CSV header gene_name,KO,thrshld,score,E-value,best -- keep best=="*" only.
ko_df = pd.read_csv("{ikos.container}")
ko_df = ko_df[ko_df["best"] == "*"]
for _, r in ko_df.iterrows():
    try:
        score = float(r["score"])
    except (TypeError, ValueError):
        score = None
    rows.append({{"orf_id": str(r["gene_name"]), "kind": "KEGG", "value": str(r["KO"]),
                  "score": score, "confidence": "best"}})

# DIAMOND UniRef50: headered BLAST6 + bsr, written by diamond_uniref50.
# That transform sets --max-target-seqs 1 so this is already best-hit-per-query;
# still dedup defensively in case the upstream relaxes that.
hit_df = pd.read_csv("{ihits.container}", sep="\\t", dtype=str)
hit_df["evalue"]   = pd.to_numeric(hit_df["evalue"],   errors="coerce")
hit_df["bitscore"] = pd.to_numeric(hit_df["bitscore"], errors="coerce")
hit_df = (hit_df.sort_values(["qseqid","evalue","bitscore"], ascending=[True,True,False])
                 .drop_duplicates(subset=["qseqid"], keep="first"))
for _, r in hit_df.iterrows():
    acc = str(r["sseqid"])
    if acc.startswith("UniRef50_"):
        acc = acc[len("UniRef50_"):]
    rows.append({{"orf_id": str(r["qseqid"]), "kind": "UNIPROT", "value": acc,
                  "score": float(r["bitscore"]) if pd.notna(r["bitscore"]) else None,
                  "confidence": "best"}})

# polars writes the parquet: pandas' own writer is a pyarrow front end and this
# image carries no pyarrow. Built from `rows` rather than handed over from pandas,
# because `pl.from_pandas` is itself implemented over arrow.
keep = set(orf_ids)
rows = [r for r in rows if r["orf_id"] in keep]
pl.DataFrame(rows, schema={{"orf_id": pl.String, "kind": pl.String,
                            "value": pl.String, "score": pl.Float64,
                            "confidence": pl.String}}).write_parquet("{iout.container}")
"""

    context.LocalShell("cat > _gather.py << 'PYEOF'\n" + script + "\nPYEOF\n")
    context.ExecWithEnv().ifContainerDo(env=image, cmd="python3 _gather.py")

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(minutes=30),
    ),
)
