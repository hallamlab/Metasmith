# network_modules -- NETWORK_MODULES (asv_pipeline.nf:5522) at upstream's config defaults:
# Leiden and Louvain at three resolutions, 25 repetitions each, consensus at 0.8.

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::aspire.env"))
scripts = model.AddRequirement(lib.GetType("lib::aspire"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
policy  = model.AddRequirement(lib.GetType("aspire::network_modules_on"), parents={survey})
all     = model.AddRequirement(lib.GetType("aspire::network_graph_all"), parents={survey})
thr     = model.AddRequirement(lib.GetType("aspire::network_graph_thr"), parents={survey})
sub     = model.AddProduct(lib.GetType("aspire::network_modules_sub"))
mall    = model.AddProduct(lib.GetType("aspire::network_modules_all"))
summary = model.AddProduct(lib.GetType("aspire::network_modules_summary"))
runs    = model.AddProduct(lib.GetType("aspire::network_modules_runs"))


def protocol(context: ExecutionContext):
    outs = {p: context.Output(p) for p in (sub, mall, summary, runs)}
    s = context.Input(scripts).container

    context.ExecWithEnv(env=image, cmd=f"""\
        set -euo pipefail
        Rscript {s}/network_modules.R --graph-sub {context.Input(thr).container} \
            --graph-all {context.Input(all).container} --outdir mods --prefix spieceasi \
            --methods leiden,louvain --primary-method leiden --reps 25 \
            --resolutions 0.5,1.0,1.5 --consensus-threshold 0.8 --seed 42
        cp mods/spieceasi_modules_sub.tsv {outs[sub].container}
        cp mods/spieceasi_modules_all.tsv {outs[mall].container}
        cp mods/spieceasi_module_summary.tsv {outs[summary].container}
        cp mods/spieceasi_module_runs.tsv {outs[runs].container}
        rm -r mods
    """)

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=not [o for o in outs.values() if not o.local.exists()],
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=6),
    ),
)
