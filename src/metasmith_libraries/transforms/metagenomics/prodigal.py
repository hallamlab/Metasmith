"""ORF prediction, with the ORF output sharded to cap downstream compute.

pprodigal itself is cheap; what it feeds is not. A single ORF FASTA makes every
downstream annotator (kofamscan, CLEAN, DIAMOND, the embedding lanes) one
unbounded job whose wall time scales with the whole assembly. So this emits the
ORFs as N shards instead of one file: the contract is unchanged -- still one
`sequences::orfs` product -- but the protocol registers one manifest branch per
shard, and metasmith's output channel turns N files on one product into N
downstream items. Each annotator then runs per shard, bounded by `orf_shard_size`.

Shards are **balanced, not greedily filled**. `n = ceil(total / orf_shard_size)`
then the ORFs are spread evenly over those n shards, so 11 ORFs at a cap of 10
give 6 + 5, never 10 + 1 -- a straggler shard costs a whole extra scheduling
round for a job that does almost no work.

The GFF stays a single file. It is the coordinate record for the assembly as a
whole and nothing consumes it per shard; splitting it would only invent a
partition that has to be reversed to be read.

Set `orf_shard_size` to 0 (or leave the assembly under one shard's worth) to get
the old single-file behaviour.
"""
from math import ceil
from metasmith.python_api import *

lib         = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model       = Transform()
image       = model.AddRequirement(lib.GetType("env::pprodigal.env"))
asm         = model.AddRequirement(lib.GetType("sequences::assembly"))
cds         = model.AddProduct(lib.GetType("sequences::orfs"))
gff         = model.AddProduct(lib.GetType("sequences::gff"))

def _balanced_shard_sizes(total: int, cap: int) -> list[int]:
    """Split `total` records into the fewest shards of at most `cap`, evenly.

    `ceil(total/cap)` fixes the shard count; the remainder is then spread one
    record per shard rather than dumped on the last one. 11 at a cap of 10 is
    [6, 5], not [10, 1].
    """
    if cap <= 0 or total <= cap:
        return [total]
    n = ceil(total / cap)
    base, extra = divmod(total, n)
    return [base + (1 if i < extra else 0) for i in range(n)]


def _iter_fasta(path):
    """Yield (header_line, [sequence_lines]) without loading the file at once."""
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

    # Same command either way: this tool is a plain CLI in both worlds.
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
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)
    
    if not (icds.local.exists() and igff.local.exists()):
        return ExecutionResult(manifest=[{cds: icds.local, gff: igff.local}], success=False)

    # ------------------------------------------------------------------
    # Shard the ORFs so each downstream annotation job is bounded. Branch 0
    # keeps the path pprodigal already wrote to (and carries the GFF); any
    # further shards get their own output path via `Output(cds, i)`.
    cap = context.params.get("orf_shard_size", 0)
    total = sum(1 for _ in _iter_fasta(icds.local))
    sizes = _balanced_shard_sizes(total, cap)

    if len(sizes) == 1:
        return ExecutionResult(
            manifest=[{cds: icds.local, gff: igff.local}],
            success=True,
        )

    Log.Info(f"sharding {total} ORFs into {len(sizes)} shards of {sizes} (cap {cap})")
    # Shard 0 reuses the path pprodigal wrote to, so move the full file aside
    # first -- otherwise opening shard 0 for writing truncates the source that is
    # still being read. One streaming pass, switching output file at each bound.
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
