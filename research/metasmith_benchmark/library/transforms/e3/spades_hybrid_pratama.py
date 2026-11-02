# Pratama's hybrid assembly: `spades.py --meta -1 R1 -2 R2 -t N -m 380 --nanopore DIR`, one per 0.2 um 2022
# Illumina replicate with its well's MinION run (17 assemblies). The call names no -k, so SPAdes picks its
# own k. The short reads are the bbduk clean reads, interleaved, so `--12` stands in for `-1 -2`.
from metasmith.python_api import *

lib    = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model  = Transform()
image  = model.AddRequirement(lib.GetType("env::spades.env"))
meta   = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads  = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
pair   = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={meta})
nano   = model.AddRequirement(lib.GetType("e3::nanopore_reads"), parents={pair})
out    = model.AddProduct(lib.GetType("e3::hybrid_spades_assembly"))

PRATAMA_MEM_GB = 380


def protocol(context: ExecutionContext):
    ireads, inano, iout = context.Input(reads), context.Input(nano), context.Output(out)
    threads = context.params.get("cpus", 1)
    # -m is a hard setrlimit inside SPAdes: Pratama's 380 GB, capped below the job's own limit.
    mem_gb = context.params.get("memory")
    mem = min(PRATAMA_MEM_GB, int(mem_gb * 0.95)) if mem_gb else PRATAMA_MEM_GB

    # CAUTION every metasmith runtime pins OMP_NUM_THREADS=1, and SPAdes then honours 1 thread, not -t.
    context.ExecWithEnv(env=image, cmd=f"""
        export OMP_NUM_THREADS={threads}
        spades.py --meta -t {threads} -m {mem} --12 {ireads.container} --nanopore {inano.container} -o spades_ws
        [[ $(head -c 1 spades_ws/contigs.fasta) == ">" ]] && cp spades_ws/contigs.fasta {iout.container}
    """)
    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    # CAUTION 20 h, not 48. fir refuses ANY job over 7.0 days at submit time (reproduced with
    # sbatch --test-only; no partition exempts it), and the retry ladder doubles duration as well as
    # memory, so a 48 h base makes rungs 3 and 4 (192 h, 384 h) unsubmittable. An ignored SUBMISSION
    # failure then decrements nextflow's running counter with no matching increment and the whole run
    # wedges -- measured on wave-5 SMETANA (runningCount -6, loadCpus -96). Keep base x 2^3 <= 168 h.
    # This transform has never run, so retuning it retires no cache.
    resources=Resources(cpus=48, memory=Size.GB(384), duration=Duration(hours=20)),
)
