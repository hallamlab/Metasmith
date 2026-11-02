from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::bowtie2.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
reads   = model.AddRequirement(lib.GetType("e2::trimmed_short_reads"), parents={meta})
asm     = model.AddRequirement(lib.GetType("e2::megahit_assembly"), parents={meta})
out     = model.AddProduct(lib.GetType("e2::binning_bam"))


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    iasm = context.Input(asm)
    iout = context.Output(out)
    cpus = context.params.get("cpus") or 1

    # BOWTIE2_ASSEMBLY_BUILD and BOWTIE2_ASSEMBLY_ALIGN, with binning_map_mode 'own' and bowtie2_mode unset.
    context.ExecWithEnv(env=image, cmd=f"""
        bowtie2-build --threads {cpus} {iasm.container} bt2_index_base
        bowtie2 -p {cpus} -x bt2_index_base --interleaved {ireads.container} 2> bowtie2.log \
            | samtools view -@ {cpus} -bS \
            | samtools sort -@ {cpus} -o {iout.container}
        cat bowtie2.log
    """)

    return ExecutionResult(manifest=[{out: iout.local}], success=iout.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(32), duration=Duration(hours=12)),
)
