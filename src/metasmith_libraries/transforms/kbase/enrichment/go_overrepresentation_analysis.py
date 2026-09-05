from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
genes   = model.AddRequirement(lib.GetType("annotation::gene_set"))
ipr     = model.AddRequirement(lib.GetType("annotation::interproscan_results"))
script  = model.AddRequirement(lib.GetType("lib::go_overrepresentation.py"))
out     = model.AddProduct(lib.GetType("annotation::go_overrepresentation"))

def protocol(context: ExecutionContext):
    igenes=context.Input(genes)
    iipr=context.Input(ipr)
    iscript=context.Input(script)
    iout=context.Output(out)

    # The background is the annotated genome, which is what {iipr} is -- taking it
    # from the gene set instead returns nothing significant and reads as a real
    # negative. The gene set is whatever selected the genes -- a DE result, a bin,
    # a blast hit list -- and is deliberately NOT typed to one of those.
    _cmd = f"""\
            python {iscript.container} {igenes.container} {iipr.container} {iout.container}
        """
    context.ExecWithEnv().ifContainerDo(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genes,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
