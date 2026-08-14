"""splitOrfsForAmr — in-DAG fan-out of a whole per-sample ORF FASTA into
AMR-sized ORF batches (sequences::orf_batch) for the resistome ORF tools
(rgi, amrfinderplus, megares, bacmet, vfdb).

Replaces the external `w4_rebatch.py` pre-materialization: the split now runs as
a workflow task consuming the whole published `sequences::orfs` and emitting N
`sequences::orf_batch` products, so no pre-staged scratch batch pool is needed and
the sacred-ID prefixing happens inside the DAG.

Batch sizing targets ~3 hr per downstream tool job (governed by the SLOWEST ORF
tool — RGI), i.e. ~30x larger batches / ~30x fewer jobs than the old ~6-min-job
pool (60k ORFs/batch). Default ORF_BATCH_SIZE lands ~85 batches across the 101
lake samples; a 4-sample river run yields a handful. Override per run via
`params["amr_orf_batch_size"]` once the first sample's real per-tool timings are
in (see the W4 SOP calibration step).

Sacred-ID contract (unchanged from w4_rebatch.py): every header id token is
reversibly sample-prefixed `>SG<id>~k141_<N>_<k> <prodigal desc>`; the sample is
the input FASTA's filename stem (metasmith stages the given item under its
original basename `<sid>.faa`, as genomad already relies on). `~` is safe — k141
ids use only [k0-9_], sample ids [A-Z0-9]. The original prodigal description is
kept verbatim. w4_recompile.py splits on the FIRST `~` to recover (sample,
original id) and regroup per sample; it reads identity from the data, never the
filename — so the batch filenames metasmith assigns are irrelevant.

Length-balanced round-robin (rank by descending length, assign rank % n_bins) so
each batch carries a comparable amino-acid load. Batches are written in groups of
<=64 open files at a time to avoid the Lustre EIO seen opening hundreds at once
(memory w4_rebatch_scratch_gotchas); at the ~3hr batch size n_bins is small so
this is a safety net.
"""
import math
from pathlib import Path

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"))
batch = model.AddProduct(lib.GetType("sequences::orf_batch"))

# ORFs per batch. 60k -> ~6-min jobs on the last run; ~1.8M targets ~3 hr on the
# slowest ORF tool (RGI) and ~85 batches across 101 samples. Override with
# params["amr_orf_batch_size"].
ORF_BATCH_SIZE = 1_800_000
OPEN_GROUP = 64


def _iter_records(path):
    """Yield (id_token, header_rest, seq_str) streaming, one record at a time.
    header_rest keeps the leading space + original description verbatim."""
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

    # raise the fd soft limit toward the hard cap (defensive; n_bins is small at
    # the ~3hr batch size, but a tiny override could re-create the many-file case)
    try:
        import resource
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(hard, 8192), hard))
    except Exception:
        pass

    # pass 1: per-record lengths only (bounded memory)
    lens = [len(seq) for _, _, seq in _iter_records(iorfs.local)]
    n = len(lens)
    n_bins = max(1, math.ceil(n / size))
    # length-balanced round-robin: rank by descending length, assign rank % n_bins
    order = sorted(range(n), key=lambda i: -lens[i])
    bin_of = [0] * n
    for rank, i in enumerate(order):
        bin_of[i] = rank % n_bins

    out_paths = [context.Output(batch, i=b).local for b in range(n_bins)]
    for p in out_paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)

    # pass 2: stream the source once per group of <=OPEN_GROUP bins
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
