# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 21. It requires the augmented database as a product rather than
# the shipped one, which is what orders add_to_db before this by data dependency
# instead of by hoping the scheduler agrees.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
db     = model.AddRequirement(lib.GetType("viromics::iphop_augmented_db"))

out_genus  = model.AddProduct(lib.GetType("viromics::host_prediction_genus"))
out_genome = model.AddProduct(lib.GetType("viromics::host_prediction_genome"))
out_detail = model.AddProduct(lib.GetType("viromics::host_prediction_detail"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    context.Input(db)
    outs = {p: context.Output(p) for p in (out_genus, out_genome, out_detail)}
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
        out_genus: "Host_prediction_to_genus.csv",
        out_genome: "Host_prediction_to_genome.csv",
        out_detail: "Detailed_output_by_tool.csv",
    },
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=24)),
)
