"""Render the cross-genome BSR-distance histogram from a pooled all-vs-all blast."""
from pathlib import Path
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
script = model.AddRequirement(lib.GetType("lib::bsr_histogram.py"))
blast  = model.AddRequirement(lib.GetType("pangenome::all_vs_all_blast"))
out    = model.AddProduct(lib.GetType("pangenome::bsr_histogram"))

def protocol(context: ExecutionContext):
    iblast  = context.Input(blast)
    iscript = context.Input(script)
    iout    = context.Output(out)
    # fake home dirs so kaleido's headless browser works under --no-home
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
            python {iscript.container} {iblast.container} {iout.container}
        """,
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=blast,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
