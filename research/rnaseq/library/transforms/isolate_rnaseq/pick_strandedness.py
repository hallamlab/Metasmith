from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
scripts = model.AddRequirement(lib.GetType("lib::isolate_rnaseq"))
exp     = model.AddRequirement(lib.GetType("transcriptomics::experiment"))
sweep   = model.AddRequirement(lib.GetType("isolate_rnaseq::featurecounts_sweep"), parents={exp})
gff     = model.AddRequirement(lib.GetType("sequences::gff"), parents={exp})
asm     = model.AddRequirement(lib.GetType("sequences::isolate_assembly"), parents={exp})
counts  = model.AddProduct(lib.GetType("isolate_rnaseq::raw_count_table"))
qc      = model.AddProduct(lib.GetType("isolate_rnaseq::count_qc"))


def protocol(context: ExecutionContext):
    icounts, iqc = context.Output(counts), context.Output(qc)
    context.ExecWithEnv(env=image, cmd=f"""\
        python {context.Input(scripts).container}/counts.py {context.Input(sweep).container} \\
            {context.Input(gff).container} {context.Input(asm).container} {icounts.container} {iqc.container}
    """)
    return ExecutionResult(
        manifest=[{counts: icounts.local, qc: iqc.local}],
        success=icounts.local.exists() and iqc.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=exp,
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=1)),
)
