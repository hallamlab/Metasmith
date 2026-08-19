from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::clean.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
wrapper_lib = model.AddRequirement(lib.GetType("lib::clean_wrapper.py"))
pred  = model.AddProduct(lib.GetType("annotation::clean_predictions"))

def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iwrap = context.Input(wrapper_lib)
    opred = context.Output(pred)

    context.LocalShell(
        'if [ -n "${SLURM_TMPDIR:-}" ] && [ -d "${SLURM_TMPDIR}" ]; then '
        '  rm -rf clean_ws; mkdir -p "${SLURM_TMPDIR}/clean_ws"; '
        '  ln -sfn "${SLURM_TMPDIR}/clean_ws" clean_ws; '
        'else mkdir -p clean_ws; fi'
    )

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd / "clean_ws", "/clean_ws"),
        ],
        args=["--env", "TORCH_HOME=/opt/torch_cache"],
        cmd=f"""
            python {iwrap.container} \
                --fasta {iorfs.container} \
                --out {opred.container} \
                --app /app \
                --workdir /clean_ws
        """,
    )
    n_rows = sum(1 for _ in open(opred.local)) - 1 if opred.local.exists() else 0
    print(f"[clean] {n_rows:,} EC calls", flush=True)
    return ExecutionResult(
        manifest=[{pred: opred.local}],
        success=n_rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=4),
                        gpus=Gpus.REQUIRED, gpu_memory=Size.GB(16)),
)
