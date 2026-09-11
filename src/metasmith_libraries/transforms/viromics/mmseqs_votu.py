# Antonio's step 10: the same 95/80 clustering at coverage mode 1. This table IS
# the vOTU definition -- there is no representative FASTA downstream of it, and a
# vOTU-level abundance is this membership joined to per-sample contig coverage
# through viromics::candidate_call_provenance.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::mmseqs2.env"))

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::votu_cluster_table"))


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
            --min-seq-id 0.95 -c 0.80 --cov-mode 1 --threads {threads}
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    context.LocalShell(f"cp cl_cluster.tsv {o.local}")

    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "votu_membership.tsv"},
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=6)),
)
