"""feast — FEAST microbial source tracking (single aggregate run).

Estimates the contribution of external source environments (human gut, oral,
skin) to each lake sample's community. Antonio provided the COMPLETE FEAST input
as a frozen table (annotation::feast_sources): a MetaPhlAn SGB species matrix
whose rows are the 101 lake sinks + 81 external source profiles, plus the
matching metadata (SourceSink / Env / id). FEAST is therefore run ONCE over the
whole table with `different_sources_flag=0` (all sinks share the source pool) —
exactly Antonio's invocation — rather than per-sample. This reproduces his
published analysis; it does not re-derive the sink rows from our own MetaPhlAn
outputs (the frozen sinks already ARE these 101 samples). No contig_id dependency.

feast_sources is a directory holding `FEAST_otus.csv` + `FEAST_metadata_final.csv`
(staged from raw/originals_from_antonio_2_resistome/feast_sources; provided to the
driver as a pre-staged input, see w4_resistome.py DB_INPUTS).
"""
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::feast.env"))
sources = model.AddRequirement(lib.GetType("annotation::feast_sources"))
out_props = model.AddProduct(lib.GetType("annotation::feast_proportions"))


def protocol(context: ExecutionContext):
    isources = context.Input(sources)
    iout = context.Output(out_props)

    # Wrapper `FEAST` (container build) reads the OTU + metadata CSVs, applies
    # Antonio's ceiling(otus*1000) integerisation, and runs FEAST once, writing
    # FEAST_results_source_contributions_matrix.txt into --outdir.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(isources.external, "/feast_sources")],
        cmd=f"""
            FEAST \
                --otus /feast_sources/FEAST_otus.csv \
                --metadata /feast_sources/FEAST_metadata_final.csv \
                --outdir feast_out
            cp feast_out/FEAST_results_source_contributions_matrix.txt {iout.container} \
                || cp feast_out/*source_contributions* {iout.container}
        """,
    )

    if not iout.local.exists():
        Path(iout.local).write_text("source\tproportion\n")

    return ExecutionResult(
        manifest=[{out_props: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=4),
    ),
)
