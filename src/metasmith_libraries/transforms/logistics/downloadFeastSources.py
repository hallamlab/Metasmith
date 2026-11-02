from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
ref   = model.AddProduct(lib.GetType("annotation::feast_sources"))

# The source profiles are a curated local compilation with no public download, so
# the run names where they are: `feast_sources_dir`, a directory on the host that
# runs this local step. Registering an `annotation::feast_sources` input skips the step.
REQUIRED = ["FEAST_otus.csv", "FEAST_metadata_final.csv"]


def protocol(context: ExecutionContext):
    iref = context.Output(ref)
    src = context.params.get("feast_sources_dir")
    if not src:
        Log.Error("no `feast_sources_dir` run param: name the directory holding "
                  f"{' and '.join(REQUIRED)}, or give annotation::feast_sources as an input")
        return ExecutionResult(manifest=[], success=False)

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iref.local}",
        *(f"cp '{src}/{f}' {iref.local}/" for f in REQUIRED),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    ok = all((iref.local/f).is_file() and (iref.local/f).stat().st_size > 0 for f in REQUIRED)
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
