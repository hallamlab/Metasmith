import math
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::diamond.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db = model.AddRequirement(lib.GetType("ref::uniref50_diamond_db"))
out_results = model.AddProduct(lib.GetType("annotation::diamond_uniref50_results_chunk"))
out_descriptions = model.AddProduct(lib.GetType("annotation::diamond_uniref50_descriptions_chunk"))

_BLOSUM62_DIAG = {
    "A": 4, "R": 5, "N": 6, "D": 6, "C": 9, "Q": 5, "E": 5, "G": 6, "H": 8, "I": 4,
    "L": 4, "K": 5, "M": 5, "F": 6, "P": 7, "S": 4, "T": 5, "W": 11, "Y": 7, "V": 4,
    "B": 4, "Z": 4, "J": 3, "X": 0, "*": 0, "U": 0, "O": 0,
}
_LAMBDA, _K, _LN2 = 0.267, 0.041, math.log(2)
_LN_K = math.log(_K)
_RAW = "diamond_raw.tsv"

# `stitle` is the last field diamond asks for, so it is split off by position and
# not by count: a subject title carrying a tab would otherwise become extra
# columns. It leaves the hit table entirely and is the description half instead.
_HITS_HEADER = "\t".join([
    "qseqid", "sseqid", "pident", "length", "mismatch", "gapopen",
    "qstart", "qend", "sstart", "send", "evalue", "bitscore", "bsr",
]) + "\n"
_STITLE_AT = 12
_DESCRIPTIONS_HEADER = "sseqid\tdescription\n"


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _self_bitscores(fasta_path):
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
    idesc = context.Output(out_descriptions)

    threads = context.params.get("cpus", 8)
    mem = context.params.get("memory")
    block_size = 2.0
    if mem:
        mem_gb = int(float(mem))
        block_size = max(1.0, min(12.0, (mem_gb - 4) / 6))

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

    self_bs = _self_bitscores(iorfs.local)
    n = 0
    titles: dict[str, str] = {}
    with open(_RAW) as fin, open(iout.local, "w") as fout:
        fout.write(_HITS_HEADER)
        for line in fin:
            if not line.strip():
                continue
            fields = line.rstrip("\n").split("\t", _STITLE_AT)
            try:
                bits = float(fields[11])
                sb = self_bs.get(fields[0], 0.0)
                bsr = bits / sb if sb > 0 else 0.0
            except (ValueError, IndexError):
                bsr = 0.0
            if len(fields) > _STITLE_AT:
                titles.setdefault(fields[1], _one_line(fields[_STITLE_AT]))
            fout.write("\t".join(fields[:_STITLE_AT]) + f"\t{bsr:.4f}\n")
            n += 1

    with open(idesc.local, "w") as fout:
        fout.write(_DESCRIPTIONS_HEADER)
        for sseqid in sorted(titles):
            fout.write(f"{sseqid}\t{titles[sseqid]}\n")

    print(f"[diamond_uniref50] {n:,} best hits over {len(titles):,} clusters",
          flush=True)

    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
                out_descriptions: idesc.local,
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
