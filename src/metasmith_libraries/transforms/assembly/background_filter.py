from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
img_sam = model.AddRequirement(lib.GetType("env::samtools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
host    = model.AddRequirement(lib.GetType("sequences::background_genome"))
out     = model.AddProduct(lib.GetType("sequences::host_filtered_short_reads"))

def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    imeta  = context.Input(meta)
    ihost  = context.Input(host)
    iout   = context.Output(out)

    with open(imeta.local) as j:
        read_meta = json.load(j)
    parity = read_meta["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    sr_params = "-x sr" if parity == "paired" else ""

    threads = context.params.get("cpus")
    threads_mm2 = "" if threads is None else f"-t {threads}"
    threads_sam = "" if threads is None else f"-@ {threads}"

    _cmd = f"""\
            minimap2 -a {sr_params} {threads_mm2} --secondary=no \
                {ihost.container} {ireads.container} > temp.sam
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_mm2, cmd=_cmd) \
        .ifVirtualEnvDo(env=img_mm2, cmd=_cmd)
    if parity == "paired":
        filter_cmd = (
            f"samtools view -u -e 'flag.unmap || flag.munmap' {threads_sam} temp.sam"
            f" | samtools collate -u -O {threads_sam} -"
            f" | samtools fastq -N {threads_sam} -"
        )
    else:
        filter_cmd = (
            f"samtools view -u -f 4 {threads_sam} temp.sam"
            f" | samtools fastq -N {threads_sam} -"
        )
    _cmd = f"""\
            {filter_cmd} \
            | gzip > {iout.container}
            rm -f temp.sam
        """
    context.ExecWithEnv() \
        .ifContainerDo(env=img_sam, cmd=_cmd) \
        .ifVirtualEnvDo(env=img_sam, cmd=_cmd)

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    )
)
