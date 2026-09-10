# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 23, and the second place the two lanes meet: every CRISPR spacer
# the survey's own MAGs carry, searched against the frozen viral set. One task,
# because a spacer database is only useful pooled.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
spacers = model.AddRequirement(lib.GetType("viromics::crispr_spacers"), parents={study})
frozen  = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"), parents={study})

out = model.AddProduct(lib.GetType("viromics::spacer_host_links"))


def protocol(context: ExecutionContext):
    context.InputGroup(spacers)
    context.Input(frozen)
    o = context.Output(out)
    context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={out: "spacer_hits.tsv"},
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
