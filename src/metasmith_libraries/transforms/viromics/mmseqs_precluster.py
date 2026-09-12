# Antonio's step 6: MMseqs2 easy-cluster at 95% identity over 80% of the shorter
# sequence, coverage mode 0. A sibling file rather than a second instance of one
# transform, because the two clusterings differ only in a flag and a transform in
# this library is one invocation with its parameters fixed -- two files keep the
# flag visible in the DAG and let the two runs schedule in parallel.
#
# It emits membership only. The representative FASTA MMseqs2 also writes is
# deliberately not a product: consuming it would restart the filter chain, and it
# carries two traps besides (a trailing space appended to every header, and 19
# sidecar files beside it).
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::mmseqs2.env"))

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::precluster_table"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    o = context.Output(out)

    threads = context.params.get("cpus", 16)

    # easy-cluster reaches the same place as the createdb -> cluster -> createtsv
    # chain in one call, and without the 19 database sidecars that chain leaves
    # behind. Of the three files it writes, only the membership TSV is taken:
    # <prefix>_rep_seq.fasta appends a trailing space to every header, and nothing
    # downstream consumes a representative FASTA anyway.
    _cmd = f"""
        mkdir -p mmseqs_tmp
        mmseqs easy-cluster {ifrozen.container} cl mmseqs_tmp \\
            --min-seq-id 0.95 -c 0.80 --cov-mode 0 --threads {threads}
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    context.LocalShell(f"cp cl_cluster.tsv {o.local}")

    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "precluster_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
