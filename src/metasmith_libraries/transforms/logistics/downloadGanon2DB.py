# REVIEW: Uses `ganon build` against RefSeq archaea+bacteria (top genome
# per species), which downloads taxonomy + sequences and builds the HIBF in
# one step. Index files share the "ganon_db" prefix inside the output dir
# per the ref::ganon2_db dtype declaration. Adjust --source / --top /
# --taxonomy for a different scope.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::ganon.env"))
out   = model.AddProduct(lib.GetType("ref::ganon2_db"))


def protocol(context: ExecutionContext):
    # ref::ganon2_db is a PREFIX: ganon writes <prefix>.hibf, <prefix>.tax
    # and the classifier reads via `--db-prefix <prefix>`.
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"--threads {threads}"

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p $(dirname {iout.container})
            ganon build --db-prefix {iout.container} \
                --source refseq --organism-group archaea bacteria \
                --top 1 {threads_arg}
        """,
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)
