# Pratama's AMG calls (Workflows/Virus_bioinformatics.md), stage 1 of 5: CheckV on the >=10 kb vOTUs.
#
# Split out of the DRAM-v monolith, which ran CheckV, VirSorter2, DRAM-v annotate and distill in one 24 h
# task. That task hit its wall at 23:59:32 on attempt 1 and had reached attempt 3 of 4 when E3 was stopped,
# so one more failure would have exhausted its ladder -- and its rung 4 asked an illegal 192 h.
#
# The combined set is an e3 sibling type, so this CheckV never competes with the standard `checkv` that
# scores the whole frozen set.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_checkv = model.AddRequirement(lib.GetType("env::checkv.env"))
reps       = model.AddRequirement(lib.GetType("e3::votu_representatives_10kb"))
checkv_db  = model.AddRequirement(lib.GetType("ref::checkv_db"))

out_combined = model.AddProduct(lib.GetType("e3::dramv_checkv_combined"))


def protocol(context: ExecutionContext):
    ireps = context.Input(reps)
    icheckv = context.Input(checkv_db)
    ocombined = context.Output(out_combined)
    threads = context.params.get("cpus", 8)

    # CheckV trims host regions: proviruses.fna holds the trimmed proviruses, viruses.fna the rest.
    context.ExecWithEnv(env=img_checkv, cmd=f"""
        checkv end_to_end {ireps.container} checkv -t {threads} -d {icheckv.container}
        cat checkv/proviruses.fna checkv/viruses.fna > combined.fna
        test -s combined.fna
        echo "combined contigs: $(grep -c '^>' combined.fna)"
    """)
    context.LocalShell(f"cp combined.fna {ocombined.local}")
    return ExecutionResult(manifest=[{out_combined: ocombined.local}], success=ocombined.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reps,
    # CAUTION duration must satisfy base x 2^(tries-1) <= 168 h, because fir refuses any job over 7.0 days
    # at submit time and an ignored submission failure wedges the run.
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=20)),
)
