# The final vOTU representative set's own recovery score against Pratama's published catalogue,
# same skani parameters as pratama_votu_recovery.py (the standard library's frozen-set,
# pool-level score, which stays unchanged and answers a separate target).
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::skani.env"))
published = model.AddRequirement(lib.GetType("pratama::published_votus"))
final     = model.AddRequirement(lib.GetType("e3::final_votu_representatives"))
out       = model.AddProduct(lib.GetType("e3::final_votu_recovery_table"))


def protocol(context: ExecutionContext):
    ipub = context.Input(published)
    ifinal = context.Input(final)
    iout = context.Output(out)

    threads = context.params.get("cpus", 8)
    _cmd = f"""
            skani dist --qi -q {ipub.container} --ri -r {ifinal.container} \
                --small-genomes --min-af 15 -o {iout.container} -t {threads}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=final,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=4),
    ),
)
