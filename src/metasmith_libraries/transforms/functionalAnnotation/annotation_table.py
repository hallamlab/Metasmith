from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
ev_lib  = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
gpr_lib = model.AddRequirement(lib.GetType("lib::fabfos_gpr"))

# Every lane, so this is the one table a user selects to have them all. Each consuming tool
# requires only the lane tables it reads instead.
ORF_LANES = ("kofam", "clean", "uniref50", "uniref90", "swissprot", "metacyc", "pbert", "deepec",
             "eggnog", "interproscan", "mobileog", "megares", "bacmet", "vfdb", "bigg", "dbcan",
             "merops", "vogdb", "refseq_viral", "rgi", "amrfinderplus", "pathofact_hmm",
             "pathofact_ml", "deeparg")
CONTIG_LANES = ("bakta_nc", "antismash")
tables = (
    [model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={orfs})
     for k in ORF_LANES]
    + [model.AddRequirement(lib.GetType(f"annotation::{k}_lane_table"), parents={asm})
       for k in CONTIG_LANES])
out = model.AddProduct(lib.GetType("annotation::annotation_table"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    context.ExecWithEnv(env=image, cmd=f"""
        python3 {context.Input(gpr_lib).container}/annotation_table.py \
            --ev-lib {context.Input(ev_lib).container} \
            --table {" ".join(str(context.Input(t).container) for t in tables)} \
            --out {iout.container}
    """)
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists() and iout.local.stat().st_size > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    # The assembly, not the ORFs: the contig lanes descend from it and not from the ORF
    # set, and a slot parented to anything but the key collects every candidate.
    group_by=asm,
    resources=Resources(cpus=1, memory=Size.GB(16), duration=Duration(hours=1)),
)
