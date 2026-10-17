import re

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::phyloflash.env"))
sources = model.AddRequirement(lib.GetType("ref::phyloflash_sources"))
out     = model.AddProduct(lib.GetType("ref::phyloflash_db"))


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    isrc = context.Input(sources)
    threads = context.params.get('cpus')
    threads_arg = "" if threads is None else f"-CPUs {threads}"

    silva = sorted(p.name for p in isrc.local.glob("SILVA_*"))
    assert len(silva) == 1, f"expected one SILVA_* file in [{isrc.local}], found {silva}"
    silva_file = silva[0]
    # makedb reads the release from the file name and builds into ./<release>/.
    release = re.match(r"SILVA_([^_]+)_", silva_file).group(1)

    # phyloFlash.pl -dbhome must name the release directory itself, and consumers
    # pass this product as -dbhome, so the release directory becomes the product.
    context.ExecWithEnv(env=image, cmd=f"""
        set -e
        phyloFlash_makedb.pl \
            --silva_file {isrc.container}/{silva_file} \
            --univec_file {isrc.container}/UniVec \
            {threads_arg}
        mkdir -p $(dirname {iout.container})
        if [ -d {iout.container} ]; then rmdir {iout.container}; fi
        mv {release} {iout.container}
    """)
    # The files phyloFlash.pl 3.4.2's check_dbhome requires of a -dbhome.
    whole = all((iout.local/name).is_file() and (iout.local/name).stat().st_size > 0 for name in (
        "ref/genome/1/summary.txt",
        "SILVA_SSU.noLSU.masked.trimmed.NR96.fixed.fasta",
        "SILVA_SSU.noLSU.masked.trimmed.fasta",
    ))
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["noscratch"],
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    ),
)
