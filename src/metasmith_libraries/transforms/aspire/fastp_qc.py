from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
run   = model.AddRequirement(lib.GetType("aspire::run"))
sid   = model.AddRequirement(lib.GetType("aspire::sample_id"), parents={run})
pair  = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={sid})
r1    = model.AddRequirement(lib.GetType("sequences::zipped_forward_short_reads"), parents={pair})
r2    = model.AddRequirement(lib.GetType("sequences::zipped_reverse_short_reads"), parents={pair})
fwd   = model.AddProduct(lib.GetType("aspire::qc_reads_fwd"))
rev   = model.AddProduct(lib.GetType("aspire::qc_reads_rev"))
rjson = model.AddProduct(lib.GetType("aspire::fastp_report_json"))
rhtml = model.AddProduct(lib.GetType("aspire::fastp_report_html"))

def protocol(context: ExecutionContext):
    made = {
        fwd: context.Output(fwd),
        rev: context.Output(rev),
        rjson: context.Output(rjson),
        rhtml: context.Output(rhtml),
    }
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = set()

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sid,
    resources=Resources(
        cpus=4,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
