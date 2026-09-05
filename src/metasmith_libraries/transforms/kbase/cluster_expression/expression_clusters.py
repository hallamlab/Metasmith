from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
clust   = model.AddRequirement(lib.GetType("lib::hierarchical_clustering.py"))
local   = model.AddRequirement(lib.GetType("lib::local"))
script  = model.AddRequirement(lib.GetType("lib::expression_clusters.py"))
counts  = model.AddRequirement(lib.GetType("transcriptomics::gene_count_table"))
# KBase ships four apps here -- hierarchical, k-means, WGCNA and an estimate of k.
# That is one transform with a method knob, not four, so the knob is an input.
params  = model.AddRequirement(lib.GetType("transcriptomics::clustering_params"))
out     = model.AddProduct(lib.GetType("transcriptomics::expression_clusters"))

def protocol(context: ExecutionContext):
    icounts=context.Input(counts)
    iparams=context.Input(params)
    iscript=context.Input(script)
    iout=context.Output(out)

    # Genes that move together are the operon and the regulon, which per-gene
    # testing reports as unrelated hits. The clustering itself is the resource the
    # pangenome heatmap already ships.
    _cmd = f"""\
            export NUMBA_CACHE_DIR=$TMPDIR
            python {iscript.container} {icounts.container} {iparams.container} {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=counts,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)
