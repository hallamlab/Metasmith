from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::fastp.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::trimmed_short_reads"))
rjson   = model.AddProduct(lib.GetType("e2::fastp_json"))
rhtml   = model.AddProduct(lib.GetType("e2::fastp_html"))

# nf-core/mag 5.5.0 FASTP: `-q 15 --cut_front --cut_tail --cut_mean_quality 15 --length_required 15`
# from its params, and --detect_adapter_for_pe from the module's paired branch.
ARGS = "-q 15 --cut_front --cut_tail --cut_mean_quality 15 --length_required 15"


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iout = context.Output(out)
    ijson = context.Output(rjson)
    ihtml = context.Output(rhtml)
    cpus = context.params.get("cpus") or 1

    context.ExecWithEnv(env=image, cmd=f"""
        fastp --interleaved_in --in1 {ireads.container} --stdout \
            --json {ijson.container} --html {ihtml.container} \
            --thread {cpus} --detect_adapter_for_pe {ARGS} \
            2> fastp.log | gzip -c > {iout.container}
        cat fastp.log
    """)

    return ExecutionResult(
        manifest=[{out: iout.local, rjson: ijson.local, rhtml: ihtml.local}],
        success=iout.local.exists() and ijson.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(cpus=6, memory=Size.GB(16), duration=Duration(hours=6)),
)
