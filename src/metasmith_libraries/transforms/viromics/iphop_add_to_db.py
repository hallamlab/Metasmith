# MOCK -- model only. Requirements, products and grouping are real; the protocol
# touches its outputs and runs no tool.
#
# Antonio's step 21b: the survey's own MAGs are added to iPHoP's host database
# before any host is predicted. This is one of the two transforms that make the
# viral and MAG lanes one template rather than two.
#
# The three collected slots are joined on bin id inside the task, not by lineage:
# GTDB-Tk runs on each binner's bins while the quality pool is the aggregator's
# output, so no single quality bin has one gtdbtk ancestor to recover. gtdbtk's
# `user_genome` column names the bin file, which is the join every consumer of
# these two files already uses.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
bin     = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={study})
cluster = model.AddRequirement(lib.GetType("binning_local::cluster_table"), parents={study})
# gtdbtk_raw, not gtdbtk: add_to_db wants the DECORATED TREES, which only
# de_novo_wf writes. The classification TSV that `taxonomy::gtdbtk` carries is a
# different artifact and add_to_db never looks at it.
tax     = model.AddRequirement(lib.GetType("taxonomy::gtdbtk_raw"), parents={study})

out_db  = model.AddProduct(lib.GetType("viromics::iphop_augmented_db"))


def protocol(context: ExecutionContext):
    for slot in (bin, cluster, tax):
        context.InputGroup(slot)
    o = context.Output(out_db)
    context.external_shell.Exec(f"touch {o.external}")
    return ExecutionResult(manifest=[{out_db: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={out_db: "iphop_db"},
    resources=Resources(cpus=16, memory=Size.GB(128), duration=Duration(hours=24)),
)
