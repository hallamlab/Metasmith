# Pratama's DeepVirFinder (reproduction_map B1): `dvf.py -l 1000 -c`. The paper's Methods give the
# cut Pratama applied to fold DVF into the pooled caller set: score >= 0.9 and p-value <= 0.05.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("bench::deepvirfinder.env"))
contigs = model.AddRequirement(lib.GetType("sequences::contig_batch"))
out_calls = model.AddProduct(lib.GetType("e3::deepvirfinder_candidate_virus"))

# dvf.py's own -l floor; a batch with nothing this long writes no dvfpred table at all.
MIN_LENGTH   = 1000
SCORE_CUT    = 0.9
PVALUE_CUT   = 0.05
CALLS_HEADER = "contig_id\tstart\tend\tcaller\tscore\n"
DVFPRED      = Path("dvf/contigs.fa_gt1000bp_dvfpred.txt")


def _longest_contig(fasta: Path) -> int:
    longest = current = 0
    with open(fasta) as f:
        for line in f:
            if line.startswith(">"):
                longest, current = max(longest, current), 0
            else:
                current += len(line.strip())
    return max(longest, current)


def _read_dvfpred(path: Path):
    """DeepVirFinder's own table: name, len, score, pvalue -- one row per scored contig."""
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        col = {n: i for i, n in enumerate(header)}
        missing = [c for c in ("name", "len", "score", "pvalue") if c not in col]
        assert not missing, f"{path.name} has no {missing}; header was {header}"
        rows = []
        for line in f:
            if line.strip():
                r = line.rstrip("\n").split("\t")
                rows.append((r[col["name"]], int(r[col["len"]]), float(r[col["score"]]), float(r[col["pvalue"]])))
    return rows


def _passes_cut(score: float, pvalue: float) -> bool:
    return score >= SCORE_CUT and pvalue <= PVALUE_CUT


def _write_calls(dvfpred_tsv: Path, out: Path):
    with open(out, "w") as o:
        o.write(CALLS_HEADER)
        for name, length, score, pvalue in _read_dvfpred(dvfpred_tsv):
            if _passes_cut(score, pvalue):
                o.write(f"{name.split()[0]}\t1\t{length}\tdeepvirfinder\t{score}\n")


def protocol(context: ExecutionContext):
    ictg = context.Input(contigs)
    ocalls = context.Output(out_calls)
    cpus = context.params.get("cpus") or 1

    # The batch splitter sorts contigs by length (sequences::contig_batch), so a later batch of a
    # metaSPAdes assembly can hold only contigs under dvf.py's own 1 kb floor -- the same edge case
    # vibrant.py and virsorter2.py guard against for their own floors.
    longest = _longest_contig(ictg.local)
    if longest < MIN_LENGTH:
        Log.Info(f"longest contig {longest} bp, under {MIN_LENGTH}: no DeepVirFinder calls in this batch")
        ocalls.local.write_text(CALLS_HEADER)
        return ExecutionResult(manifest=[{out_calls: ocalls.local}], success=True)

    # Theano compiles its kernels on first use and needs a writable cache. CAUTION dvf.py predicts in a
    # multiprocessing pool whose workers share that cache and race for its lock, and one task died with
    # `FileExistsError: ... compiledir_.../lock_dir`. The cure is to compile BEFORE the pool exists: a
    # one-contig run warms the cache serially, so every worker then finds it built and takes no lock.
    # `compiledir_format=%(process_id)s` is NOT the cure -- Theano 1.0 has no such key and raises
    # `KeyError: 'process_id'` at import, which failed every task in 16 s.
    # CAUTION dvf.py has an upstream crash we must dodge by pre-filtering. Its encode loop flushes every
    # 100 ACCEPTED contigs and clears its buffers (`if len(seqname) % 100 == 0: ... code = []`), and the
    # tail block afterwards calls `zip(*pool.map(pred, range(0, len(code))))` UNCONDITIONALLY while the
    # append above it stays guarded. So when the accepted count is an exact multiple of 100 AND the LAST
    # record in the file is rejected, `code` is empty and dvf.py dies with
    # `ValueError: not enough values to unpack (expected 3, got 0)` -- after a full run's work.
    # Handing dvf.py only records it accepts makes the final record always append, so the tail always
    # has work. See bench/deepvirfinder.py for the measurements that pinned this down.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        export THEANO_FLAGS="base_compiledir=$PWD/theano,floatX=float32" OMP_NUM_THREADS={cpus}
        zcat -f {ictg.container} | awk '
            function flush() {{
                if (h != "") {{
                    L = length(s)
                    if (L >= 1000) {{ t = s; nN = gsub(/[Nn]/, "", t); if (nN / L <= 0.3) print h"\\n"s }}
                }}
            }}
            /^>/ {{ flush(); h = $0; s = ""; next }}
            {{ s = s $0 }}
            END {{ flush() }}
        ' > contigs.fa
        echo "contigs accepted by dvf.py's own filter: $(grep -c '^>' contigs.fa)"
        head -n 2 contigs.fa > warm.fa
        python /DeepVirFinder/dvf.py -i warm.fa -o dvf_warm -l 1 -c 1 || true
        python /DeepVirFinder/dvf.py -i contigs.fa -o dvf -l 1000 -c {cpus}
    """)
    assert DVFPRED.exists(), f"DeepVirFinder wrote no {DVFPRED}"
    _write_calls(DVFPRED, ocalls.local)
    return ExecutionResult(manifest=[{out_calls: ocalls.local}], success=ocalls.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=contigs,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=12)),
)
