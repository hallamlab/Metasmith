from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
img   = model.AddRequirement(lib.GetType("env::bam2fastx.env"))
rmeta = model.AddRequirement(lib.GetType("sequences::read_metadata"))
bam   = model.AddRequirement(lib.GetType("sequences::pacbio_hifi_bam"), parents={rmeta})
out   = model.AddProduct(lib.GetType("sequences::long_reads"))

def protocol(context: ExecutionContext):
    ibam = context.Input(bam)
    iout = context.Output(out)

    temp_prefix = "converted"
    context.ExecWithEnv().ifContainerDo(
        env=img,
        cmd=f"""
        ln -sf {ibam.container} /ws/input.bam
        pbindex /ws/input.bam
        bam2fastq -o {temp_prefix} /ws/input.bam
        """
    )

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    context.LocalShell(f"""
        if [ -f {temp_prefix}.fastq.gz ]; then
            zcat {temp_prefix}.fastq.gz | pigz {threads} -7 -c >{iout.local}
        elif [ -f {temp_prefix}.fastq ]; then
            pigz {threads} -7 -c {temp_prefix}.fastq >{iout.local}
        else
            echo "ERROR: no output from bam2fastq" >&2
            exit 1
        fi
    """)

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
    group_by=rmeta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    )
)
