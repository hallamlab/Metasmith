from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
w_300m    = model.AddProduct(lib.GetType("ref::esm_c_300m_weights"))
w_600m    = model.AddProduct(lib.GetType("ref::esm_c_600m_weights"))

HF_300M   = "EvolutionaryScale/esmc-300m-2024-12"
HF_600M   = "EvolutionaryScale/esmc-600m-2024-12"

def protocol(context: ExecutionContext):
    i300 = context.Output(w_300m)
    i600 = context.Output(w_600m)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            pip install --quiet --no-cache-dir huggingface_hub
            huggingface-cli download {HF_300M} --local-dir esmc_300m --local-dir-use-symlinks False
            tar -czf {i300.container} -C esmc_300m .
            huggingface-cli download {HF_600M} --local-dir esmc_600m --local-dir-use-symlinks False
            tar -czf {i600.container} -C esmc_600m .
        """,
    )

    return ExecutionResult(
        manifest=[{w_300m: i300.local, w_600m: i600.local}],
        success=i300.local.exists() and i600.local.exists(),
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
