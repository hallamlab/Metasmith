from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1      = model.AddRequirement(lib.GetType("sequences::forward_ora_reads"), parents={pair})
r2      = model.AddRequirement(lib.GetType("sequences::reverse_ora_reads"), parents={pair})
orad    = model.AddRequirement(lib.GetType("env::orad.env"))
bbtools = model.AddRequirement(lib.GetType("env::bbtools.env"))
out     = model.AddProduct(lib.GetType("sequences::short_reads_pe"))

def protocol(context: ExecutionContext):
    ir1 = context.Input(r1)
    ir2 = context.Input(r2)
    iout = context.Output(out)
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"

    # Decompress ORA files to gzipped FASTQ using orad (one file at a time)
    # Note: on Apptainer (HPC), /app/oradata needs a bind mount from outside.
    # The runner script or Nextflow config should add:
    #   containerOptions = '--bind /path/to/oradata:/app/oradata'
    context.ExecWithEnv().ifContainerDo(
        env=orad,
        cmd=f'''
        orad -q -c "{ir1.container}" > r1.fastq.gz
        orad -q -c "{ir2.container}" > r2.fastq.gz
        '''
    )

    # Interleave R1+R2 with reformat.sh and compress with pigz
    context.ExecWithEnv().ifContainerDo(
        env=bbtools,
        cmd=f'''
        reformat.sh \
            in1=r1.fastq.gz \
            in2=r2.fastq.gz \
            out=stdout.fq \
        | pigz {threads} > {iout.container}
        '''
    )

    return ExecutionResult(
        manifest=[{
            out: iout.local
        }],
        success=iout.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pair,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    )
)
