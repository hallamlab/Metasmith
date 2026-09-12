from pathlib import Path

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
batch = model.AddProduct(lib.GetType("sequences::contig_batch"))

CONTIG_BATCH_BP = 240_000_000


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
    iasm = context.Input(asm)
    sample = Path(iasm.local).stem
    batch_bp = int(context.params.get("amr_contig_batch_bp", CONTIG_BATCH_BP))

    out_paths = []
    b = 0
    cur_bp = 0
    cur_n = 0
    fh = None

    def _open(bi):
        p = context.Output(batch, i=bi).local
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        out_paths.append(p)
        return open(p, "w")

    for idt, rest, seq in _iter_records(iasm.local):
        if fh is None:
            fh = _open(b)
        if cur_n > 0 and cur_bp + len(seq) > batch_bp:
            fh.close()
            b += 1
            cur_bp = 0
            cur_n = 0
            fh = _open(b)
        _write_record(fh, sample, idt, rest, seq)
        cur_bp += len(seq)
        cur_n += 1
    if fh is not None:
        fh.close()

    print(f"[splitContigsForAmr] {sample}: {len(out_paths)} contig_batch "
          f"(bp~{batch_bp})", flush=True)
    manifest = [{batch: p} for p in out_paths]
    return ExecutionResult(
        manifest=manifest,
        success=len(out_paths) > 0 and all(Path(p).exists() for p in out_paths),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
