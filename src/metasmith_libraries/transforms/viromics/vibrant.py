# MOCK -- model only. The requirements, products and grouping are real; the
# protocol touches its outputs and runs no tool. See transforms/viromics/README
# for what filling this in involves.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# contig_fasta rather than assembly, so VIBRANT sits on the same input as the
# other two callers per sample AND runs again on the frozen set, where its
# lifestyle and AMG tables are what Antonio's step 15 wanted.
contigs = model.AddRequirement(lib.GetType("sequences::contig_batch"))

out_quality   = model.AddProduct(lib.GetType("viromics::vibrant_genome_quality"))
out_lifestyle = model.AddProduct(lib.GetType("viromics::vibrant_lifestyle_table"))
out_amgs      = model.AddProduct(lib.GetType("viromics::vibrant_amgs"))
out_calls     = model.AddProduct(lib.GetType("viromics::vibrant_candidate_virus"))


def protocol(context: ExecutionContext):
    context.Input(contigs)
    outs = {p: context.Output(p) for p in (out_quality, out_lifestyle, out_amgs, out_calls)}
    for o in outs.values():
        context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=contigs,
    output_signature={
        out_quality: "genome_quality.tsv",
        out_lifestyle: "lifestyle.tsv",
        out_amgs: "amgs.tsv",
        out_calls: "vibrant_calls.tsv",
    },
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
