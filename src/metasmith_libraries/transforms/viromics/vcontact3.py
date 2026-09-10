# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 20, run ONCE on the frozen set. vConTACT3 has no path that
# reuses protein clusters against a subset of genomes -- `-p/-g` skips gene
# calling but not clustering, and the parquet intermediates are skipped only on
# an exact re-run -- so a filtered rerun costs the whole thing. Run it on
# everything and treat the cluster assignment as a per-contig annotation.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))

out_assignments = model.AddProduct(lib.GetType("viromics::vcontact3_assignments"))
out_network     = model.AddProduct(lib.GetType("viromics::vcontact3_network"))
out_ani         = model.AddProduct(lib.GetType("viromics::vcontact3_ani"))


def protocol(context: ExecutionContext):
    context.Input(frozen)
    outs = {p: context.Output(p) for p in (out_assignments, out_network, out_ani)}
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
        out_assignments: "final_assignments.csv",
        out_network: "network",
        out_ani: "ani_summary.tsv",
    },
    resources=Resources(cpus=32, memory=Size.GB(128), duration=Duration(hours=24)),
)
