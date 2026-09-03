# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 22, on the MAG lane: CRISPR arrays and cas operons per quality
# bin. Its third product, the spacers, is the input to the within-survey half of
# host prediction.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

bin = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"))

out_arrays  = model.AddProduct(lib.GetType("viromics::crispr_arrays"))
out_operons = model.AddProduct(lib.GetType("viromics::cas_operons"))
out_spacers = model.AddProduct(lib.GetType("viromics::crispr_spacers"))


def protocol(context: ExecutionContext):
    results = []
    for item in context.AsBatch():
        item.Input(bin)
        outs = {p: item.Output(p) for p in (out_arrays, out_operons, out_spacers)}
        for o in outs.values():
            context.external_shell.Exec(f"touch {o.external}")
        results.append(ExecutionResult(
            manifest=[{p: o.local for p, o in outs.items()}],
            success=all(o.local.exists() for o in outs.values()),
        ))
    return results


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin,
    batch_size=50,
    output_signature={
        out_arrays: "crisprs_all.tab",
        out_operons: "cas_operons.tab",
        out_spacers: "spacers.fna",
    },
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
