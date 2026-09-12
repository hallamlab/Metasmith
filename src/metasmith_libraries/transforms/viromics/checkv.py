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
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::checkv.env"))
db     = model.AddRequirement(lib.GetType("ref::checkv_db"))
frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))

out_contamination = model.AddProduct(lib.GetType("viromics::checkv_contamination"))
out_quality       = model.AddProduct(lib.GetType("viromics::checkv_quality_summary"))
out_completeness  = model.AddProduct(lib.GetType("viromics::checkv_completeness"))
out_complete      = model.AddProduct(lib.GetType("viromics::checkv_complete_genomes"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    idb = context.Input(db)
    threads = context.params.get("cpus", 16)
    work = "checkv_out"

    # The output directory is POSITIONAL. checkv has no -o, and passing one is a
    # parse error rather than a warning.
    _cmd = f"""
        checkv end_to_end {ifrozen.container} {work} -d {idb.container} -t {threads}
    """
    context.ExecWithEnv(env=image, cmd=_cmd)

    wanted = {
        out_contamination: "contamination.tsv",
        out_quality: "quality_summary.tsv",
        out_completeness: "completeness.tsv",
        out_complete: "complete_genomes.tsv",
    }
    outs = {}
    for prod, name in wanted.items():
        src = Path(work)/name
        o = context.Output(prod)
        assert src.exists(), (
            f"checkv wrote no {name}. end_to_end produces all four together, so a "
            "missing one means the run stopped part way rather than that this "
            "table was not applicable."
        )
        o.local.write_bytes(src.read_bytes())
        outs[prod] = o

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
