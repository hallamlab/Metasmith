from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::instrain.env"))
magref  = model.AddRequirement(lib.GetType("binning::derep_mag_ref"))
profiles = model.AddRequirement(lib.GetType("annotation::instrain_profile"),
                                parents={magref})
out_cmp = model.AddProduct(lib.GetType("annotation::instrain_compare"))


def protocol(context: ExecutionContext):
    imagref = context.Input(magref)
    iprofiles = sorted(context.InputGroup(profiles), key=lambda p: str(p.container))
    iout = context.Output(out_cmp)

    threads = context.params.get("cpus", 16)
    dirs = " ".join(str(p.container) for p in iprofiles)

    context.ExecWithEnv(
        env=image,
        binds=[(imagref.external, "/magref")],
        cmd=f"""
            inStrain compare \
                -i {dirs} \
                -s /magref/mag_ref.stb \
                -o compare_out \
                -p {threads}
        """,
    )

    Path("compare_out").rename(iout.local)

    return ExecutionResult(
        manifest=[{out_cmp: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=magref,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
