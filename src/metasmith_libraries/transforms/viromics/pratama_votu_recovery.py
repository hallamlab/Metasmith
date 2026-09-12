# DESIGNED, NOT YET RUN AGAINST REAL DATA: verified only that the `skani dist
# --qi --ri --small-genomes` command shape and column names
# (Ref_name/Query_name give the per-contig identity, not just the file) are
# what a real skani 0.2.2 binary emits, against a synthetic 3 kb pair
# (staphb/skani:0.2.2) -- not against the real 3 GB published vOTU catalogue
# or a real viromics::dereplicated_candidate_virus from the Pratama
# reproduction, which has not itself run yet (.awm/context.md).
#
# --qi/--ri score each contig in a multi-fasta as its own genome, unlike
# pratama_mag_recovery.py's plain per-file comparison -- both the published
# catalogue and our own frozen candidate set are one pooled multi-fasta, one
# record per vOTU, so there is no re-splitting step to write here.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::skani.env"))
published = model.AddRequirement(lib.GetType("pratama::published_votus"))
ours      = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out       = model.AddProduct(lib.GetType("pratama::votu_recovery_table"))


def protocol(context: ExecutionContext):
    ipub = context.Input(published)
    iours = context.Input(ours)
    iout = context.Output(out)

    threads = context.params.get("cpus", 8)
    _cmd = f"""
            skani dist --qi -q {ipub.container} --ri -r {iours.container} \
                --small-genomes --min-af 15 -o {iout.container} -t {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=ours,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)
