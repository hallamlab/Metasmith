# Pratama's AMG calls (reproduction_map B10): VirSorter2 --prep-for-dramv on the >=10 kb vOTUs, then
# `DRAM-v.py annotate --min_contig_size 1000` and `distill`.
#
# The map gives no flags for this second VirSorter2 pass, so it takes the per-sample pass's (B4).
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_vs2  = model.AddRequirement(lib.GetType("env::virsorter2.env"))
img_dram = model.AddRequirement(lib.GetType("env::dram.env"))
reps     = model.AddRequirement(lib.GetType("e3::votu_representatives_10kb"))
vs2_db   = model.AddRequirement(lib.GetType("annotation::virsorter2_db"))
dram_db  = model.AddRequirement(lib.GetType("annotation::dram_db"))
out_annot   = model.AddProduct(lib.GetType("annotation::dramv_annotations"))
out_distill = model.AddProduct(lib.GetType("annotation::dramv_distill"))


def protocol(context: ExecutionContext):
    ireps = context.Input(reps)
    ivs2 = context.Input(vs2_db)
    idram = context.Input(dram_db)
    iannot = context.Output(out_annot)
    idistill = context.Output(out_distill)
    threads = context.params.get("cpus", 8)

    context.ExecWithEnv(env=img_vs2, binds=[(ivs2.external, "/db")], cmd=f"""
        export HOME="$PWD"
        virsorter run --seqfile {ireps.container} --db-dir /db --working-dir vs2_out --jobs {threads} \
            --include-groups dsDNAphage,ssDNA --keep-original-seq --min-score 0.5 --min-length 5000 \
            --prep-for-dramv all
        test -s vs2_out/for-dramv/final-viral-combined-for-dramv.fa
    """)
    context.ExecWithEnv(env=img_dram, binds=[(idram.external, "/db")], cmd=f"""
        export HOME=/tmp
        DRAM-v.py annotate -i vs2_out/for-dramv/final-viral-combined-for-dramv.fa \
            -v vs2_out/for-dramv/viral-affi-contigs-for-dramv.tab \
            -o dramv_annot --threads {threads} --min_contig_size 1000 --config_loc /db/DRAM.config
        DRAM-v.py distill -i dramv_annot/annotations.tsv -o dramv_distill --config_loc /db/DRAM.config
    """)
    context.LocalShell(f"cp dramv_annot/annotations.tsv {iannot.local}")
    context.LocalShell(f"cp -r dramv_distill {idistill.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local, out_distill: idistill.local}],
                           success=iannot.local.exists() and idistill.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reps,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)
