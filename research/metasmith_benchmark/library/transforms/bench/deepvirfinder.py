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
    # Theano compiles its kernels on first use and needs a writable cache.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        export THEANO_FLAGS="base_compiledir=$PWD/theano,floatX=float32" OMP_NUM_THREADS={cpus}
        zcat -f {iasm.container} > contigs.fa
        python /DeepVirFinder/dvf.py -i contigs.fa -o dvf -l 1000 -c {cpus}
        mv dvf/contigs.fa_gt1000bp_dvfpred.txt {oscores.container}
    """)
    return ExecutionResult(manifest=[{out: oscores.local}], success=oscores.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=24)),
)
