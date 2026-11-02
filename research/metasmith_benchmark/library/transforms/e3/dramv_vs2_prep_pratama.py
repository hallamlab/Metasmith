# Pratama's AMG calls, stage 2 of 5: VirSorter2 --prep-for-dramv over CheckV's combined set.
#
# It emits the two files DRAM-v needs together, the contigs and the affi table, so both are products of
# this one transform and the annotators pair them through their shared parent.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_vs2  = model.AddRequirement(lib.GetType("env::virsorter2.env"))
combined = model.AddRequirement(lib.GetType("e3::dramv_checkv_combined"))
vs2_db   = model.AddRequirement(lib.GetType("annotation::virsorter2_db"))

out_contigs = model.AddProduct(lib.GetType("e3::dramv_prep_contigs"))
out_affi    = model.AddProduct(lib.GetType("e3::dramv_prep_affi"))


def protocol(context: ExecutionContext):
    icombined = context.Input(combined)
    ivs2 = context.Input(vs2_db)
    ocontigs = context.Output(out_contigs)
    oaffi = context.Output(out_affi)
    threads = context.params.get("cpus", 8)

    context.ExecWithEnv(env=img_vs2, binds=[(ivs2.external, "/db")], cmd=f"""
        export HOME="$PWD"
        virsorter run --seqname-suffix-off --viral-gene-enrich-off --provirus-off --prep-for-dramv \
            --seqfile {icombined.container} --db-dir /db --working-dir vs2_out \
            --include-groups dsDNAphage,ssDNA --min-length 5000 --min-score 0.5 --jobs {threads} all
        test -s vs2_out/for-dramv/final-viral-combined-for-dramv.fa
        test -s vs2_out/for-dramv/viral-affi-contigs-for-dramv.tab
    """)
    context.LocalShell(f"cp vs2_out/for-dramv/final-viral-combined-for-dramv.fa {ocontigs.local}")
    context.LocalShell(f"cp vs2_out/for-dramv/viral-affi-contigs-for-dramv.tab {oaffi.local}")
    return ExecutionResult(manifest=[{out_contigs: ocontigs.local, out_affi: oaffi.local}],
                           success=ocontigs.local.exists() and oaffi.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=combined,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=20)),
)
