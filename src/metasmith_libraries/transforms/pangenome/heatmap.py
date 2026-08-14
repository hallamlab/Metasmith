from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
clust   = model.AddRequirement(lib.GetType("lib::hierarchical_clustering.py"))
clust   = model.AddRequirement(lib.GetType("lib::local"))
script  = model.AddRequirement(lib.GetType("lib::pangenome_heatmap.py"))
matrix  = model.AddRequirement(lib.GetType("pangenome::ppanggolin_matrix"))
out     = model.AddProduct(lib.GetType("pangenome::heatmap"))

def protocol(context: ExecutionContext):
    imatrix=context.Input(matrix)
    iscript=context.Input(script)
    iout=context.Output(out)
    # these are so the browser works correctly (with --no-home), which is used by kaleido, which is used by plotly
    context.LocalShell("""
        mkdir -p ./fake_home/.cache
        mkdir -p ./fake_home/.local
        mkdir -p ./fake_home/.config
        mkdir -p ./fake_home/.pki
    """)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            ("$(pwd -P)/fake_home/.cache",  "$HOME/.cache"),
            ("$(pwd -P)/fake_home/.local",  "$HOME/.local"),
            ("$(pwd -P)/fake_home/.config", "$HOME/.config"),
            ("$(pwd -P)/fake_home/.pki",    "$HOME/.pki"),
        ],
        cmd=f"""\
            export NUMBA_CACHE_DIR=$TMPDIR
            python {iscript.container} {imatrix.container} {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=matrix,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    )
)
