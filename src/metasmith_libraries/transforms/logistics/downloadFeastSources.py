from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))  # shell only
ref   = model.AddProduct(lib.GetType("annotation::feast_sources"))

# FEAST source data is NOT internet-downloadable — Antonio provided the complete
# frozen FEAST input directly (via Slack): a MetaPhlAn SGB species matrix whose
# rows are the 101 lake sinks + 81 external source profiles (human_gut / oral /
# skin), plus the matching metadata (SourceSink / Env / id). Provenance +
# invocation: raw/originals_from_antonio_2_resistome/feast_sources/PROVENANCE.md.
#
# Those CSVs are staged to the fir lib as the durable source of truth (see
# w4_resistome.py / W0 staging). In the normal run the driver provides
# feast_sources as a PRE-STAGED input (DB_INPUTS), so this downloader is not on
# the run path; it exists as the standard-mechanism fallback that assembles the
# product dir from the lib backup.
LIB_FEAST_SOURCES = "/home/phyberos/project-rpp/lib/feast_sources"
REQUIRED = ["FEAST_otus.csv", "FEAST_metadata_final.csv"]


def protocol(context: ExecutionContext):
    iref = context.Output(ref)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p {iref.container}
            cp {LIB_FEAST_SOURCES}/FEAST_otus.csv {iref.container}/
            cp {LIB_FEAST_SOURCES}/FEAST_metadata_final.csv {iref.container}/
        """,
    )

    ok = all((iref.local / f).exists() for f in REQUIRED)
    return ExecutionResult(
        manifest=[{ref: iref.local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(2),
        duration=Duration(minutes=10),
    ),
)
