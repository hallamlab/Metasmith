from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly = model.AddRequirement(lib.GetType("std::assembly"))
image    = model.AddRequirement(lib.GetType("std::oci_image_abricate"))
out      = model.AddProduct(lib.GetType("std::abricate_report"))

def protocol(context: ExecutionContext):
    assembly_path = context.Input(assembly)
    out_path      = context.Output(out)

    cpus = context.params.get("cpus")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"--threads {cpus}"

    context.ExecWithContainer(
        image = image,
        cmd = f"""
            abricate \
                {cpus_string} \
                --db resfinder \
                {assembly_path.container} > {out_path.container}
        """
    )
    return ExecutionResult(
        manifest=[{
            out: out_path.local,
        }],
        success=out_path.local.exists(),
    )

TransformInstance(
    protocol = protocol,
    group_by=assembly,
    model = model,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
