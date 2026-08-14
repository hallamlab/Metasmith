from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
w_base   = model.AddProduct(lib.GetType("ref::ankh_base_weights"))
w_large  = model.AddProduct(lib.GetType("ref::ankh_large_weights"))

HF_BASE  = "ElnaggarLab/ankh-base"
HF_LARGE = "ElnaggarLab/ankh-large"

def protocol(context: ExecutionContext):
    ibase  = context.Output(w_base)
    ilarge = context.Output(w_large)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            pip install --quiet --no-cache-dir huggingface_hub
            huggingface-cli download {HF_BASE}  --local-dir ankh_base  --local-dir-use-symlinks False
            tar -czf {ibase.container}  -C ankh_base .
            huggingface-cli download {HF_LARGE} --local-dir ankh_large --local-dir-use-symlinks False
            tar -czf {ilarge.container} -C ankh_large .
        """,
    )

    return ExecutionResult(
        manifest=[{w_base: ibase.local, w_large: ilarge.local}],
        success=ibase.local.exists() and ilarge.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
