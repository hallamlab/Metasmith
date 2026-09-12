from math import ceil
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::pprodigal.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
cds         = model.AddProduct(lib.GetType("sequences::orfs"))
gff         = model.AddProduct(lib.GetType("sequences::gff"))

def _balanced_shard_sizes(total: int, cap: int) -> list[int]:
    if cap <= 0 or total <= cap:
        return [total]
    n = ceil(total / cap)
    base, extra = divmod(total, n)
    return [base + (1 if i < extra else 0) for i in range(n)]


def _iter_fasta(path):
    header, body = None, []
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if header is not None:
                    yield header, body
                header, body = line, []
            elif header is not None:
                body.append(line)
    if header is not None:
        yield header, body


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    icds = context.Output(cds)
    igff = context.Output(gff)

    cpus_string = ""
    cpus = context.params.get("cpus")
    if cpus is not None:
        cpus_string = f"-T {cpus}"

    _cmd = f"""\
            pprodigal \
                {cpus_string} \
                -C 100 \
                -p meta \
                -i {iasm.container} \
                -a {icds.container} \
                -f gff \
                -o {igff.container}
            """
    context.ExecWithEnv(env=image, cmd=_cmd)
    
    if not (icds.local.exists() and igff.local.exists()):
        return ExecutionResult(manifest=[{cds: icds.local, gff: igff.local}], success=False)

    cap = context.params.get("orf_shard_size", 0)
    total = sum(1 for _ in _iter_fasta(icds.local))
    sizes = _balanced_shard_sizes(total, cap)

    if len(sizes) == 1:
        return ExecutionResult(
            manifest=[{cds: icds.local, gff: igff.local}],
            success=True,
        )

    Log.Info(f"sharding {total} ORFs into {len(sizes)} shards of {sizes} (cap {cap})")
    source = icds.local.parent / f"_all_{icds.local.name}"
    icds.local.rename(source)
    shard_paths = [icds.local] + [context.Output(cds, i).local for i in range(1, len(sizes))]

    handles = [open(p, "w") for p in shard_paths]
    try:
        i, left = 0, sizes[0]
        for header, body in _iter_fasta(source):
            while left == 0:
                i += 1
                left = sizes[i]
            handles[i].write(header)
            handles[i].writelines(body)
            left -= 1
    finally:
        for h in handles:
            h.close()
    source.unlink()

    manifest = [
        {cds: p, gff: igff.local} if i == 0 else {cds: p}
        for i, p in enumerate(shard_paths)
    ]
    return ExecutionResult(
        manifest=manifest,
        success=all(m[cds].exists() for m in manifest),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    )
)
