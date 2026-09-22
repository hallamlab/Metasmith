# DeepVirFinder over one assembly, as Pratama ran it: `dvf.py -l 1000`. The paper gives no score or
# p-value cut, so the per-contig table is the product and no call joins the frozen viral set.
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::deepvirfinder.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
out     = model.AddProduct(lib.GetType("bench::deepvirfinder_scores"))


def protocol(context: ExecutionContext):
    iasm, oscores = context.Input(asm), context.Output(out)
    cpus = context.params.get("cpus") or 1
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
    # Measured: a metagem sample with exactly 69,600 accepted contigs failed identically at 32/64/128 GB
    # (55:23 / 58:12 / 58:34), while 70,116 (%100=16) and 145,469 (%100=69) passed. Handing dvf.py only
    # records it accepts makes the final record always append, so the tail always has work.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        export THEANO_FLAGS="base_compiledir=$PWD/theano,floatX=float32" OMP_NUM_THREADS={cpus}
        zcat -f {iasm.container} | awk '
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
        mv dvf/contigs.fa_gt1000bp_dvfpred.txt {oscores.container}
    """)
    return ExecutionResult(manifest=[{out: oscores.local}], success=oscores.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    # CAUTION 20 h, not 24. fir refuses any job over 7.0 days at SUBMIT time, and the retry ladder
    # doubles duration as well as memory, so a 24 h base makes rung 4 (192 h) unsubmittable -- and an
    # ignored SUBMISSION failure decrements nextflow's running counter with no matching increment until
    # the whole run wedges (metagem ended runningCount -7, loadCpus -112). Keep base x 2^3 <= 168 h.
    # Measured walls: cami 9-15 min, metagem ~50 min, so 20 h is ample.
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=20)),
)
