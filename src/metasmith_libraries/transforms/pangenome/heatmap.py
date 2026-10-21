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
    context.LocalShell("""
        mkdir -p ./fake_home/.cache
        mkdir -p ./fake_home/.local
        mkdir -p ./fake_home/.config
        mkdir -p ./fake_home/.pki
    """)
    fake_home = [
        context.Mount(f"$(pwd -P)/fake_home/{d}", f"$HOME/{d}")
        for d in (".cache", ".local", ".config", ".pki")
    ]
    context.ExecWithEnv(
        env=image,
        binds=fake_home,
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
