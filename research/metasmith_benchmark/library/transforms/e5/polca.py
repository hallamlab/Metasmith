# POLCA rather than pypolca: pypolca runs one freebayes over the whole BAM, and POLCA runs freebayes on 5 Mbp
# batches across threads, which on a several-hundred-Mb metagenome assembly is the difference that matters.
# POLCA has no careful mode: it calls with freebayes -p 1 -F 0.2 --min-coverage 3 and applies the calls whose
# alternative allele outweighs the reference.
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e5::masurca.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e5::flye_assembly"), parents={meta})
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("e5::polca_assembly"))


def protocol(context: ExecutionContext):
    iasm, ireads, iout = context.Input(asm), context.Input(reads), context.Output(out)
    threads = context.params.get("cpus") or 4
    mem_gb = context.params.get("memory")
    # -m is samtools sort memory per thread.
    sort_mem = f"-m {max(1, int(mem_gb * 0.5 / threads))}G" if mem_gb else ""

    # POLCA maps with bwa mem -SP, single-ended, so the interleaved reads go in as they are.
    # It names its outputs for the assembly it was given, beside the working directory.
    context.ExecWithEnv(env=image, cmd=f"""
        cp {iasm.container} flye.fa
        polca.sh -a flye.fa -r {ireads.container} -t {threads} {sort_mem}
        cat flye.fa.report
        cp flye.fa.PolcaCorrected.fa {iout.container}
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists() and iout.local.stat().st_size > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=12)),
)
