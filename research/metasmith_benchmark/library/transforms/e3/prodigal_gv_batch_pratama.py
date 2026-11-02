# prodigal-gv over ONE slice of the frozen viral set.
#
# Split out of prodigal_gv_pratama.py, which called genes over all 4,597,542 frozen contigs in a single
# task. That task missed its 8 h rung (FAILED 140:0 at 07:59:01) and was retried at 16 h; the first
# attempt had covered ~2.42 M contigs in 3:59, and the rate FALLS as it goes, so extrapolating from the
# first half underestimated it. Each attempt also restarts from zero, because prodigal-gv has no
# checkpoint. Chunking is the fix the wave-3 diagnosis already named.
#
# Gene calling is per contig, so a batch's calls are identical to the same contigs' calls inside the whole
# set. This is a scheduling change and carries no deviation row.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# checkv.env ships prodigal-gv 2.11.0-gv.
image = model.AddRequirement(lib.GetType("env::checkv.env"))
batch = model.AddRequirement(lib.GetType("e3::viral_contig_batch"))
cds   = model.AddProduct(lib.GetType("e3::viral_orfs_batch"))
gff   = model.AddProduct(lib.GetType("e3::viral_gff_batch"))


def protocol(context: ExecutionContext):
    ibatch = context.Input(batch)
    outs = {p: context.Output(p) for p in (cds, gff)}
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        prodigal-gv -p meta -i {ibatch.container} -a viral_orfs.faa -f gff -o viral_orfs.gff
        test -s viral_orfs.gff
    """)
    context.LocalShell(f"cp viral_orfs.faa {outs[cds].local}")
    context.LocalShell(f"cp viral_orfs.gff {outs[gff].local}")
    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=batch,
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=8)),
)
