# Antonio's step 5 cut the pooled FASTA at 1 kb and steps 11 and 13 cut it again
# at 5 kb and 10 kb, so each later tool saw a different set. Here the same numbers
# are one column of one table: seqkit stats per frozen contig, and every size cut
# is a predicate over it.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::seqkit.env"))

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
out    = model.AddProduct(lib.GetType("viromics::contig_length_table"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    o = context.Output(out)

    # -n prints only the name (no sequence), -l adds length, -g adds GC percent.
    # seqkit writes no header, so one is prepended here rather than left for every
    # consumer to guess at.
    _cmd = f"seqkit fx2tab -n -l -g {ifrozen.container} > lengths.raw.tsv"
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    with open("lengths.raw.tsv") as src, open(o.local, "w") as dst:
        dst.write("contig_id\tlength_bp\tgc_percent\n")
        for line in src:
            if line.strip():
                dst.write(line)

    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={out: "contig_lengths.tsv"},
    resources=Resources(cpus=2, memory=Size.GB(4), duration=Duration(hours=1)),
)
