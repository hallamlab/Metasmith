from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
weights  = model.AddProduct(lib.GetType("ref::prott5_xl_uniref50_weights"))

HF_REPO  = "Rostlab/prot_t5_xl_uniref50"

def protocol(context: ExecutionContext):
    iweights = context.Output(weights)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            pip install --quiet --no-cache-dir huggingface_hub
            huggingface-cli download {HF_REPO} \
                --local-dir model \
                --local-dir-use-symlinks False
            tar -czf {iweights.container} -C model .
        """,
    )

    return ExecutionResult(
        manifest=[{weights: iweights.local}],
        success=iweights.local.exists(),
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
