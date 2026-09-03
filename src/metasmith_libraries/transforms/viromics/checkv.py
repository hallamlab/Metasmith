# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# ONE run, answering Antonio's steps 9 and 14 together. `checkv end_to_end` is
# contamination -> completeness -> complete_genomes -> quality_summary over a
# single tmp/, so one prodigal-gv gene call and one DIAMOND search are shared by
# all four; running `contamination` as its own transform and `end_to_end` as
# another repeats that pass for nothing.
#
# The trim stays a suggestion. contamination.tsv carries region_coords_bp and
# region_coords_genes, so a provirus boundary is recorded as coordinates and
# nothing is cut out of the frozen set. Note also that CheckV drops nothing: a
# contig with no detected boundary is written whole to viruses.fna, so that file
# is "contigs with no host region", not "the viral contigs".
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))

out_contamination = model.AddProduct(lib.GetType("viromics::checkv_contamination"))
out_quality       = model.AddProduct(lib.GetType("viromics::checkv_quality_summary"))
out_completeness  = model.AddProduct(lib.GetType("viromics::checkv_completeness"))
out_complete      = model.AddProduct(lib.GetType("viromics::checkv_complete_genomes"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    outs = {p: context.Output(p) for p in
            (out_contamination, out_quality, out_completeness, out_complete)}
    for o in outs.values():
        context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={
        out_contamination: "contamination.tsv",
        out_quality: "quality_summary.tsv",
        out_completeness: "completeness.tsv",
        out_complete: "complete_genomes.tsv",
    },
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=8)),
)
