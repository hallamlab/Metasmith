"""DIAMOND blastp of the ORFs vs UniRef50 -> diamond_uniref50_results.

Emits BLAST6 + stitle + an appended BSR column (hit_bitscore / self_bitscore) --
the 14-col shape the GPR mapper's uniref reader consumes. The self bitscore is the
BLOSUM62 query self-diagonal converted to bits via the DIAMOND Karlin-Altschul
params (lambda=0.267, K=0.041) -- computed analytically, with NO query-vs-query
alignment. BSR is the mapper's per-hit confidence (raw_score) for this lane.
The _self_bitscores helper is ported from cyanoverse functionalAnnotation/uniref_lane.py.
"""
import math
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::diamond.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db = model.AddRequirement(lib.GetType("ref::uniref50_diamond_db"))
out_results = model.AddProduct(lib.GetType("annotation::diamond_uniref50_results_chunk"))

# BLOSUM62 self-diagonal (score of each residue aligned to itself). Summed over a
# query gives its raw self-score; the KA transform below turns that into bits.
_BLOSUM62_DIAG = {
    "A": 4, "R": 5, "N": 6, "D": 6, "C": 9, "Q": 5, "E": 5, "G": 6, "H": 8, "I": 4,
    "L": 4, "K": 5, "M": 5, "F": 6, "P": 7, "S": 4, "T": 5, "W": 11, "Y": 7, "V": 4,
    "B": 4, "Z": 4, "J": 3, "X": 0, "*": 0, "U": 0, "O": 0,
}
_LAMBDA, _K, _LN2 = 0.267, 0.041, math.log(2)
_LN_K = math.log(_K)
_RAW = "diamond_raw.tsv"


def _self_bitscores(fasta_path):
    """Per-query self-bitscore: BLOSUM62 diagonal sum -> bits (KA), no alignment."""
    scores, seq_id, raw = {}, None, 0

    def finalize(sid, r):
        scores[sid] = max(0.0, (_LAMBDA * r - _LN_K) / _LN2)

    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                if seq_id is not None:
                    finalize(seq_id, raw)
                seq_id, raw = line[1:].split()[0], 0
            elif seq_id is not None:
                for ch in line.strip().upper():
                    raw += _BLOSUM62_DIAG.get(ch, 0)
        if seq_id is not None:
            finalize(seq_id, raw)
    return scores


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb = context.Input(db)
    iout = context.Output(out_results)

    threads = context.params.get("cpus", 8)
    mem = context.params.get("memory")
    block_size = 2.0  # default
    if mem:
        mem_gb = int(float(mem))
        # DIAMOND uses ~6GB per block, adjust based on available memory
        block_size = max(1.0, min(12.0, (mem_gb - 4) / 6))

    # Run DIAMOND blastp against UniRef50; BLAST6 + stitle written to _RAW.
    context.ExecWithEnv().ifContainerDo(
        binds=[(idb.external.parent, "/db")],
        env=image,
        cmd=f"""
            diamond blastp \
                --query {iorfs.container} \
                --db /db/{idb.external.name} \
                --out {_RAW} \
                --threads {threads} \
                --block-size {block_size} \
                --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle \
                --max-target-seqs 1 \
                --evalue 1e-5 \
                --sensitive
        """,
    )

    # Append the analytic BSR column (hit_bitscore / self_bitscore). stitle is the
    # last emitted field, so BSR becomes column 14. No second alignment.
    self_bs = _self_bitscores(iorfs.local)
    n = 0
    with open(_RAW) as fin, open(iout.local, "w") as fout:
        for line in fin:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            try:
                bits = float(fields[11])
                sb = self_bs.get(fields[0], 0.0)
                bsr = bits / sb if sb > 0 else 0.0
            except (ValueError, IndexError):
                bsr = 0.0
            fout.write(line.rstrip("\n") + f"\t{bsr:.4f}\n")
            n += 1
    print(f"[diamond_uniref50] {n:,} best hits", flush=True)

    # `iout.local` is created by the open() above, so its existence says nothing.
    # An empty table means the database staged wrong or the ORF ids are not what
    # the mapper joins on -- either way the uniref50 channel would be silently
    # absent from the GPR table.
    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
            },
        ],
        success=n > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)
