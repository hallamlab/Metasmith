import math
from pathlib import Path

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
batch = model.AddProduct(lib.GetType("sequences::orf_batch"))

ORF_BATCH_SIZE = 1_800_000
OPEN_GROUP = 64


def _iter_records(path):
    idt, rest, buf = None, "", []
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if idt is not None:
                    yield idt, rest, "".join(buf)
                head = line[1:].rstrip("\n")
                sp = head.find(" ")
                if sp < 0:
                    idt, rest = head, ""
                else:
                    idt, rest = head[:sp], head[sp:]
                buf = []
            else:
                buf.append(line.strip())
        if idt is not None:
            yield idt, rest, "".join(buf)


def _write_record(fh, sample, idt, rest, seq):
    fh.write(f">{sample}~{idt}{rest}\n")
    for i in range(0, len(seq), 60):
        fh.write(seq[i:i + 60] + "\n")


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    sample = Path(iorfs.local).stem
    size = int(context.params.get("amr_orf_batch_size", ORF_BATCH_SIZE))

    try:
        import resource
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 8192), hard))
    except Exception:
        pass

    lens = [len(seq) for _, _, seq in _iter_records(iorfs.local)]
    n = len(lens)
    n_bins = max(1, math.ceil(n / size))
    order = sorted(range(n), key=lambda i: -lens[i])
    bin_of = [0] * n
    for rank, i in enumerate(order):
        bin_of[i] = rank % n_bins

    out_paths = [context.Output(batch, i=b).local for b in range(n_bins)]
    for p in out_paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)

    for start in range(0, n_bins, OPEN_GROUP):
        end = min(start + OPEN_GROUP, n_bins)
        fhs = {b: open(out_paths[b], "w") for b in range(start, end)}
        for idx, (idt, rest, seq) in enumerate(_iter_records(iorfs.local)):
            b = bin_of[idx]
            if start <= b < end:
                _write_record(fhs[b], sample, idt, rest, seq)
        for fh in fhs.values():
            fh.close()

    print(f"[splitOrfsForAmr] {sample}: {n} ORFs -> {n_bins} orf_batch "
          f"(size~{size})", flush=True)
    manifest = [{batch: p} for p in out_paths]
    return ExecutionResult(
        manifest=manifest,
        success=len(out_paths) > 0 and all(Path(p).exists() for p in out_paths),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
