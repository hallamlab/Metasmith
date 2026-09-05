from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"))
r1      = model.AddRequirement(lib.GetType("sequences::zipped_forward_short_reads"), parents={pair})
r2      = model.AddRequirement(lib.GetType("sequences::zipped_reverse_short_reads"), parents={pair})
image   = model.AddRequirement(lib.GetType("env::bbtools.env"))
out     = model.AddProduct(lib.GetType("sequences::short_reads"))

def protocol(context: ExecutionContext):
    ir1=context.Input(r1)
    ir2=context.Input(r2)
    iout=context.Output(out)
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    

    # unbgzip=f is load-bearing, not tidying. By default bbmap sends EVERY .gz
    # through its BGZF reader (ReadWrite.getGZipInputStream), which then splits
    # the stream on block boundaries that plain gzip does not have. Above a few
    # hundred MB the split lands mid-deflate and the reader thread dies with
    # "Not a gzip file" while the process keeps waiting -- so the task hangs
    # instead of failing, and SLURM, nextflow and any log-watching monitor all
    # report it as RUNNING. unbgzip=f falls through to external pigz, which is
    # both correct on multi-member gzip and faster. Do not remove it without
    # re-testing on a >1 GB library, and check READ COUNTS, not exit codes:
    # multithreadedbgzf=f, the other obvious knob, exits 0 having silently
    # dropped 90% of the reads.
    #
    # The same latent bug is in interleave_short_reads, ora2fastq, filtlong,
    # sylph, ganon2 and phyloflash. They are untouched because the run that
    # found this did not exercise them, and an untested edit is not a fix.

    _cmd = f'''
        reformat.sh \
            unbgzip=f \
            in1="{ir1.container}" \
            in2="{ir2.container}" \
            out=stdout.fq \
        | pigz {threads} > {iout.container}
        '''
    context.ExecWithEnv(env=image, cmd=_cmd)
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
        cpus=16,
        memory=Size.GB(8),
        duration=Duration(hours=3),
    )
)
