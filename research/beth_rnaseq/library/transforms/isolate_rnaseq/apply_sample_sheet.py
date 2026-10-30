from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
raw     = model.AddRequirement(lib.GetType("isolate_rnaseq::raw_count_table"), parents={exp})
sheet   = model.AddRequirement(lib.GetType("isolate_rnaseq::sample_sheet"), parents={exp})
counts  = model.AddProduct(lib.GetType("transcriptomics::gene_count_table"))


def protocol(context: ExecutionContext):
    icounts = context.Output(counts)
    context.ExecWithEnv(env=image, cmd=f"""\
        python {context.Input(scripts).container}/apply_sample_sheet.py {context.Input(raw).container} \\
            {context.Input(sheet).container} {icounts.container}
    """)
    return ExecutionResult(manifest=[{counts: icounts.local}], success=icounts.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(minutes=30)),
)
