from metasmith.python_api import *
from pathlib import Path

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
genomes = model.AddRequirement(lib.GetType("fabfos_data::genomes"))
out     = model.AddProduct(lib.GetType("sequences::orfs"))


def protocol(context: ExecutionContext):
    src = Path(str(context.Input(genomes).local))
    hosts = sorted(p for p in src.glob("*") if (p / "genome").is_dir())
    if not hosts:
        raise SystemExit(f"no host/genome/ directories under {src}")

    manifest = []
    for i, host in enumerate(hosts):
        faa = sorted((host / "genome").glob("*.faa"))
        if len(faa) != 1:
            raise SystemExit(
                f"{host.name}: expected exactly one proteome under genome/, found "
                f"{[p.name for p in faa]}. One proteome per host is what makes the "
                f"attribution in host_gpr_denovo unambiguous.")
        iout = context.Output(out, i)
        dest = Path(str(iout.local))
        dest.write_bytes(faa[0].read_bytes())
        n = sum(1 for line in dest.open() if line.startswith(">"))
        Log.Info(f"{host.name}: {faa[0].name} -> {dest.name}  {n:,} proteins")
        manifest.append({out: dest})

    return ExecutionResult(
        manifest=manifest,
        success=len(manifest) == len(hosts),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=genomes,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=15)),
)
