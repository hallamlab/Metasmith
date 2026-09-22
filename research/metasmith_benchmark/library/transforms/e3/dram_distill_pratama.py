# Pratama's MAG annotation (reproduction_map A14), the distill stage. It joins the per-annotator tables
# into the one `annotations.tsv` DRAM's distill expects, then runs `DRAM.py distill`.
#
# The two products are unchanged from the monolith this replaces, so the E3 driver's targets and every
# downstream consumer bind exactly as before.
#
# CAUTION both annotate runs call their own ORFs with Prodigal over the same MAGs, so their tables are
# expected to share a gene index. The join is an OUTER join on that index and asserts the overlap rather
# than assuming it: a silent index mismatch would produce a half-empty distillate that still looks valid.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddRequirement(lib.GetType("annotation::dram_db"))
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"))
kofam = model.AddRequirement(lib.GetType("e3::mag_dram_kofam_annotations"), parents={asm})
pfam  = model.AddRequirement(lib.GetType("e3::mag_dram_pfam_annotations"), parents={asm})

out_annot   = model.AddProduct(lib.GetType("e3::mag_dram_annotations"))
out_distill = model.AddProduct(lib.GetType("e3::mag_dram_distill"))

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
    idb = context.Input(db)
    ikofam = context.Input(kofam)
    ipfam = context.Input(pfam)
    iannot = context.Output(out_annot)
    idistill = context.Output(out_distill)

    Path("merge.py").write_text(MERGE)
    context.ExecWithEnv(env=image, binds=[(idb.external, "/db")], cmd=f"""
        export HOME=/tmp
        python3 merge.py {ikofam.container} {ipfam.container} annotations.tsv
        test -s annotations.tsv
        DRAM.py distill -i annotations.tsv -o dram_distill --config_loc /db/DRAM.config
    """)
    context.LocalShell(f"cp annotations.tsv {iannot.local}")
    context.LocalShell(f"cp -r dram_distill {idistill.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local, out_distill: idistill.local}],
                           success=iannot.local.exists() and idistill.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # Distill is a table join and a set of lookups against DRAM's sheets: light, and nothing like the
    # searches that precede it.
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=4)),
)
