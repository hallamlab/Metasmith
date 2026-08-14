"""merge_dram_annotate_genes — gather per-chunk DRAM annotations.tsv into a
per-sample annotations.tsv.

DRAM 1.5.0 emits annotations.tsv with a single TSV header line. Some columns
may differ between chunks if DRAM dynamically drops empty columns (rare for
the full reference set, but worth guarding): we reindex to the union of
columns observed across chunks, filling missing cells with ''.
"""
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

parent_orfs = model.AddRequirement(lib.GetType("sequences::orfs"))
chunk_out   = model.AddRequirement(lib.GetType("annotation::dram_annotations_chunk"), parents={parent_orfs})
merged      = model.AddProduct(lib.GetType("annotation::dram_annotations"))


def protocol(context: ExecutionContext):
    import os, csv
    chunks = sorted(context.InputGroup(chunk_out), key=lambda p: str(p.local))
    iout = context.Output(merged)

    all_cols: list[str] = []
    seen = set()
    rows_per_chunk = []
    for cf in chunks:
        with open(cf.local, newline='') as fin:
            reader = csv.DictReader(fin, delimiter='\t')
            cols = reader.fieldnames or []
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    all_cols.append(c)
            rows_per_chunk.append(list(reader))

    with open(iout.local, "w", newline='') as fout:
        writer = csv.DictWriter(fout, fieldnames=all_cols, delimiter='\t', extrasaction='ignore')
        writer.writeheader()
        for rows in rows_per_chunk:
            for r in rows:
                for c in all_cols:
                    r.setdefault(c, "")
                writer.writerow(r)

    for cf in chunks:
        try:
            os.unlink(cf.local)
        except OSError:
            pass

    return ExecutionResult(
        manifest=[{merged: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=parent_orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
