"""splitContigsForAmr — in-DAG fan-out of a whole per-sample assembly FASTA into
AMR-sized contig batches (sequences::contig_batch) for the contig-level tools
(integron_finder, genomad, virsorter2, mobileelementfinder, pathofact).

Replaces the external `w4_rebatch.py` pre-materialization: the split now runs as
a workflow task consuming the whole published `sequences::assembly` and emitting N
`sequences::contig_batch` products, so no pre-staged scratch batch pool is needed.

Batch sizing targets ~3 hr per downstream tool job. Contig tools span a wide
speed range and PathoFact is the slowest — its transform is already calibrated to
~2.9 hr at ~240 Mbp/batch — so CONTIG_BATCH_BP defaults to 240 Mbp (the faster
tools, genomad/virsorter2/integron_finder, finish well under 3 hr at that size but
still amortize SLURM startup, vs the ~6-min jobs of the old 30-Mbp pool). ~240 Mbp
lands ~85 batches across the 101 lake samples; a 4-sample river run yields a
handful. If the first river sample's timings show one tool dominating, switch to a
per-family batch size (a distinct coarse contig_batch sibling) rather than one
global value. Override per run via `params["amr_contig_batch_bp"]`.

Sacred-ID contract (unchanged from w4_rebatch.py): every header id token is
reversibly sample-prefixed `>SG<id>~k141_<N> <megahit flags>`; the sample is the
input FASTA's filename stem (metasmith stages the given item under its original
basename `<sid>.fna`). `~` is safe — k141 ids use only [k0-9_], sample ids
[A-Z0-9]. The original megahit description is kept verbatim. w4_recompile.py splits
on the FIRST `~` to recover (sample, original id) and regroup per sample; it reads
identity from the data, never the filename.

Greedy fill by bp with one open file at a time (contigs are emitted in file order;
a batch rolls over once adding the next contig would exceed the bp cap and the
current batch is non-empty). Contig ids are byte-identical to the published
assembly so the k141_* IDs survive the split unchanged.
"""
from pathlib import Path

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
batch = model.AddProduct(lib.GetType("sequences::contig_batch"))

# bp per contig batch. 30 Mbp -> ~6-min jobs last run; 240 Mbp matches pathofact's
# calibrated ~2.9 hr/batch (the slowest contig tool) and ~85 batches / 101 samples.
CONTIG_BATCH_BP = 240_000_000


def _iter_records(path):
    """Yield (id_token, header_rest, seq_str) streaming, one record at a time."""
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
        # roll over if adding this contig would exceed the cap and batch nonempty
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
