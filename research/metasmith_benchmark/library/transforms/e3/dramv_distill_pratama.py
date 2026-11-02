# Pratama's AMG calls, stage 5 of 5: join the per-annotator tables and run `DRAM-v.py distill`.
#
# The two products are unchanged from the monolith this replaces, so the E3 driver's targets bind as before.
#
# CAUTION both annotate runs call their own ORFs over the same prepped contigs, so their tables are
# expected to share a gene index. The join asserts that overlap rather than assuming it: a silent index
# mismatch would yield a half-empty AMG distillate that still looks valid.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_dram = model.AddRequirement(lib.GetType("env::dram.env"))
dram_db  = model.AddRequirement(lib.GetType("annotation::dram_db"))
combined = model.AddRequirement(lib.GetType("e3::dramv_checkv_combined"))
kofam    = model.AddRequirement(lib.GetType("e3::dramv_kofam_annotations"), parents={combined})
pfam     = model.AddRequirement(lib.GetType("e3::dramv_pfam_annotations"), parents={combined})

out_annot   = model.AddProduct(lib.GetType("annotation::dramv_annotations"))
out_distill = model.AddProduct(lib.GetType("annotation::dramv_distill"))

MERGE = """import sys, pandas as pd
a = pd.read_csv(sys.argv[1], sep="\\t", index_col=0)
b = pd.read_csv(sys.argv[2], sep="\\t", index_col=0)
shared = a.index.intersection(b.index)
print(f"kofam rows {len(a)}, pfam rows {len(b)}, shared gene ids {len(shared)}")
assert len(shared) > 0, "the two annotate runs share no gene id: their ORF calls diverged"
extra = [c for c in b.columns if c not in a.columns]
print("columns taken from pfam:", extra)
m = a.join(b[extra], how="outer")
m.to_csv(sys.argv[3], sep="\\t")
print(f"merged rows {len(m)}, columns {len(m.columns)}")
"""


def protocol(context: ExecutionContext):
    idram = context.Input(dram_db)
    ikofam = context.Input(kofam)
    ipfam = context.Input(pfam)
    iannot = context.Output(out_annot)
    idistill = context.Output(out_distill)

    Path("merge.py").write_text(MERGE)
    context.ExecWithEnv(env=img_dram, binds=[(idram.external, "/db")], cmd=f"""
        export HOME=/tmp
        python3 merge.py {ikofam.container} {ipfam.container} annotations.tsv
        test -s annotations.tsv
        DRAM-v.py distill -i annotations.tsv -o dramv_distill --config_loc /db/DRAM.config
    """)
    context.LocalShell(f"cp annotations.tsv {iannot.local}")
    context.LocalShell(f"cp -r dramv_distill {idistill.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local, out_distill: idistill.local}],
                           success=iannot.local.exists() and idistill.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=combined,
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=4)),
)
