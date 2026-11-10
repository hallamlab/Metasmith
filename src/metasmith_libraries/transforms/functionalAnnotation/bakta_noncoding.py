from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::bakta.env"))
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
db       = model.AddRequirement(lib.GetType("annotation::bakta_db"))
to_gff3  = model.AddRequirement(lib.GetType("lib::piler_cr_to_gff3.py"))
out_gff  = model.AddProduct(lib.GetType("annotation::bakta_gff"))
out_tsv  = model.AddProduct(lib.GetType("annotation::bakta_tsv"))
# bakta writes twelve files under the flags above and this transform declared two of them.
# Three of the other ten are worth having and cost nothing to declare: the .gbff is the
# annotated genome record and the interchange format everything downstream of `annotate`
# reads, the .ffn carries the sequence of every feature called, and the .txt is the
# per-genome summary. The remaining seven are empty or duplicate the input under --skip-cds.
out_gbk  = model.AddProduct(lib.GetType("sequences::gbk"))
out_ffn  = model.AddProduct(lib.GetType("annotation::bakta_feature_nt"))
out_txt  = model.AddProduct(lib.GetType("annotation::bakta_summary"))


# Bakta's covariance-model searches scale with the bases searched, and a ~1 Gbp parent assembly ran past
# 23 h in one process at 8 cores. Past SHARD_BP the contigs are dealt into shards run side by side.
SHARD_BP = 60_000_000
THREADS_PER_SHARD = 4
OUTPUTS = ("gff3", "tsv", "gbff", "ffn", "txt")


def _iter_fasta(path: Path):
    head, buf = None, []
    with open(path) as f:
        for line in f:
            if line.startswith(">"):
                if head is not None:
                    yield head, buf
                head, buf = line, []
            else:
                buf.append(line)
    if head is not None:
        yield head, buf


def _shard_fasta(path: Path, n: int, outdir: Path) -> list:
    outdir.mkdir(parents=True, exist_ok=True)
    shards = [outdir / f"shard_{i}.fasta" for i in range(n)]
    fhs = [open(s, "w") for s in shards]
    filled = [0] * n
    for head, buf in _iter_fasta(path):
        i = filled.index(min(filled))
        fhs[i].write(head)
        fhs[i].writelines(buf)
        filled[i] += sum(len(l.strip()) for l in buf)
    for fh in fhs:
        fh.close()
    return [s for s, bp in zip(shards, filled) if bp > 0]


def _merge(parts: list, out: Path, ext: str):
    """Header comes from the first shard; a gff3 keeps one ##FASTA section after every shard's features."""
    with open(out, "w") as fo:
        if ext in ("gbff", "ffn", "txt"):
            for p in parts:
                fo.write(p.read_text())
            return
        fasta = []
        for k, p in enumerate(parts):
            body, _, seqs = p.read_text().partition("##FASTA\n")
            fasta.append(seqs)
            for line in body.splitlines(keepends=True):
                if line.startswith("#") and k > 0 and not line.startswith("##sequence-region"):
                    continue
                fo.write(line)
        if ext == "gff3" and any(fasta):
            fo.write("##FASTA\n" + "".join(fasta))


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb  = context.Input(db)
    igff3 = context.Input(to_gff3)
    outs = dict(zip(OUTPUTS, (context.Output(t) for t in (out_gff, out_tsv, out_gbk, out_ffn, out_txt))))

    cpus = context.params.get("cpus") or THREADS_PER_SHARD
    total_bp = sum(sum(len(l.strip()) for l in buf) for _, buf in _iter_fasta(iasm.local))
    n = max(1, min(-(-total_bp // SHARD_BP), cpus // THREADS_PER_SHARD))
    inputs = [Path(iasm.container)] if n == 1 else _shard_fasta(Path(iasm.local), n, Path("bakta_shards"))
    per = max(1, cpus // len(inputs))
    outdirs = [Path(f"bakta_out_{i}") for i in range(len(inputs))]
    runs = " ".join(
        f"( bakta --db /db/db-light --threads {per}"
        f" --skip-cds --skip-sorf --skip-pseudo --skip-plot --skip-crispr --keep-contig-headers --force"
        f" --prefix bakta_noncoding --output {o} {i} > {o}.log 2>&1 || {{ tail -50 {o}.log; exit 1; }} ) &"
        for i, o in zip(inputs, outdirs))
    print(f"bakta noncoding on {total_bp} bp: {len(inputs)} shard(s) at {per} threads each", flush=True)

    context.ExecWithEnv(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            rc=0
            {runs}
            for p in $(jobs -p); do wait $p || rc=1; done
            exit $rc
        """,
    )

    Path("bakta_out").mkdir(exist_ok=True)
    for ext in OUTPUTS:
        _merge([o / f"bakta_noncoding.{ext}" for o in outdirs], Path(f"bakta_out/bakta_noncoding.{ext}"), ext)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            pilercr -in {iasm.container} -out crispr_raw.txt -noinfo -quiet || true

            if [ -s crispr_raw.txt ]; then
                python3 {igff3.container} crispr_raw.txt bakta_out/bakta_noncoding.gff3
            fi
        """,
    )

    for ext, o in outs.items():
        context.LocalShell(f"cp bakta_out/bakta_noncoding.{ext} {o.local}")

    return ExecutionResult(
        manifest=[{t: outs[ext].local for t, ext in zip((out_gff, out_tsv, out_gbk, out_ffn, out_txt), OUTPUTS)}],
        success=all(o.local.exists() for o in outs.values()),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
