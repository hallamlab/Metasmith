# Reference-free QUAST over a sample's four assemblies, the score Pratama gets and CAMI gets beside metaQUAST.
from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("e5::quast.env"))
meta     = model.AddRequirement(lib.GetType("sequences::read_metadata"))
megahit  = model.AddRequirement(lib.GetType("sequences::megahit_assembly"), parents={meta})
opera    = model.AddRequirement(lib.GetType("e5::opera_ms_assembly"), parents={meta})
flye     = model.AddRequirement(lib.GetType("e5::flye_assembly"), parents={meta})
polca    = model.AddRequirement(lib.GetType("e5::polca_assembly"), parents={meta})
out      = model.AddProduct(lib.GetType("e5::quast_report"))

LABELS = {"megahit": megahit, "opera_ms": opera, "flye": flye, "flye_polca": polca}


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    threads = context.params.get("cpus") or 4
    fastas = " ".join(str(context.Input(r).container) for r in LABELS.values())

    context.ExecWithEnv(env=image, cmd=f"""
        quast.py {fastas} -l {",".join(LABELS)} -o q -t {threads} --no-icarus --no-plots --no-html
        cd q && tar czf {iout.container} report.tsv transposed_report.tsv quast.log
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=4, memory=Size.GB(16), duration=Duration(hours=4)),
)
