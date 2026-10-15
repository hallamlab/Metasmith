# Declared `Gpus.OPTIONAL` on purpose: this must succeed on a CPU-only host too,
# which is what makes the fallback path honest rather than decorative. The probe
# asks from inside the tool container with the tool's own `nvidia-smi`, because a
# `--nv` flag on the command line proves nothing about what arrived.
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("containers::metasmith.env"))
name = model.AddRequirement(lib.GetType("examples::name"))
out = model.AddProduct(lib.GetType("examples::gpu_report"))


def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    declared, requested = context.DeclaredGpus()
    detected = context.DetectGpus()

    header = [
        f"declared={declared.value}",
        f"requested_gb={'' if requested is None else requested.value_gb}",
        f"detected_devices={len(detected)}",
        f"detected_gb={','.join(f'{d.value_gb:.1f}' for d in detected)}",
    ]
    # The load-bearing line: ask the tool environment itself. `|| echo` so a
    # CPU-only host produces a report rather than a failed step.
    cmd = (
        f'{{ '
        f'echo "{" ".join(header)}"; '
        f'echo "CUDA_VISIBLE_DEVICES=${{CUDA_VISIBLE_DEVICES:-unset}}"; '
        f'nvidia-smi -L 2>&1 || echo "no gpu visible in tool environment"; '
        f'}} > {out_path.container} 2>&1'
    )
    context.ExecWithEnv(env=image, cmd=cmd)
    return ExecutionResult(
        manifest=[{out: out_path.local}],
        success=out_path.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=name,
    resources=Resources(
        cpus=1, memory=Size.GB(1), duration=Duration(minutes=10),
        gpus=Gpus.OPTIONAL, gpu_memory=Size.GB(4),
    ),
)
