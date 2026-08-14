from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::fastp.env"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))
out             = model.AddProduct(lib.GetType("sequences::clean_short_reads"))
report_json     = model.AddProduct(lib.GetType("sequences::fastp_json_report"))
report_html     = model.AddProduct(lib.GetType("sequences::fastp_html_report"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    iout=context.Output(out)
    ijson=context.Output(report_json)
    ihtml=context.Output(report_html)

    # todo: custom container with pigz
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--thread {threads}"
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            fastp --interleaved_in {threads} \
            -h {ihtml.container} -j {ijson.container} \
            -i {ireads.container} --stdout | gzip > {iout.container}
        """,
    )
    
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                report_json: ijson.local,
                report_html: ihtml.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    )
)
