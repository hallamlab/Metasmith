# CheckV over ONE slice of the frozen viral set.
#
# A pinned copy of the standard viromics/checkv.py, per batch instead of over the whole set: the standard
# transform is never edited in place, because that would retire its entries for every other experiment.
# The E3 driver masks the standard checkv.py so these two never both answer viromics::checkv_*.
#
# Chunking is safe here for the same reason it is for prodigal-gv: every table CheckV writes is one row per
# contig, scored against its own database, so a batch's rows are identical to those contigs' rows inside the
# whole set. A scheduling change, no deviation row.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::checkv.env"))
db    = model.AddRequirement(lib.GetType("ref::checkv_db"))
batch = model.AddRequirement(lib.GetType("e3::viral_contig_batch"))

out_contamination = model.AddProduct(lib.GetType("e3::checkv_contamination_batch"))
out_quality       = model.AddProduct(lib.GetType("e3::checkv_quality_summary_batch"))
out_completeness  = model.AddProduct(lib.GetType("e3::checkv_completeness_batch"))
out_complete      = model.AddProduct(lib.GetType("e3::checkv_complete_genomes_batch"))

WANTED = {
    out_contamination: "contamination.tsv",
    out_quality: "quality_summary.tsv",
    out_completeness: "completeness.tsv",
    out_complete: "complete_genomes.tsv",
}


def protocol(context: ExecutionContext):
    ibatch = context.Input(batch)
    idb = context.Input(db)
    threads = context.params.get("cpus", 16)
    work = "checkv_out"

    # The output directory is POSITIONAL. checkv has no -o, and passing one is a parse error.
    context.ExecWithEnv(env=image, cmd=f"""
        checkv end_to_end {ibatch.container} {work} -d {idb.container} -t {threads}
    """)

    outs = {}
    for prod, name in WANTED.items():
        src = Path(work) / name
        o = context.Output(prod)
        assert src.exists(), (
            f"checkv wrote no {name}. end_to_end produces all four together, so a missing one means the "
            "run stopped part way rather than that this table was not applicable.")
        o.local.write_bytes(src.read_bytes())
        outs[prod] = o

    return ExecutionResult(manifest=[{p: o.local for p, o in outs.items()}],
                           success=all(o.local.exists() for o in outs.values()))


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=batch,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=8)),
)
