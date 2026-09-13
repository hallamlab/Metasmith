import glob
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("e2::comebin.env"))
meta    = model.AddRequirement(lib.GetType("e2::read_metadata"))
asm     = model.AddRequirement(lib.GetType("e2::assembly"), parents={meta})
bam     = model.AddRequirement(lib.GetType("e2::binning_bam"), parents={asm})
bins    = model.AddProduct(lib.GetType("e2::comebin_bin"))
table   = model.AddProduct(lib.GetType("e2::comebin_contig_to_bin"))


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    ibam = context.Input(bam)
    cpus = context.params.get("cpus") or 1

    # COMEBIN_RUNCOMEBIN passes no arguments beyond threads, assembly and the BAM directory.
    # COMEBin writes temporary files beside the assembly, so it gets a local copy.
    context.ExecWithEnv(env=image, cmd=f"""
        cat {iasm.container} > local_assembly.fasta
        mkdir -p bam && cp -L {ibam.container} bam/
        run_comebin.sh -t {cpus} -a local_assembly.fasta -p bam/ -o .
        rm local_assembly.fasta
    """)

    otable = context.Output(table)
    manifest = []
    with open(otable.local, "w") as t:
        for i, path in enumerate(sorted(glob.glob("comebin_res/comebin_res_bins/*.fa"))):
            obin = context.Output(bins, i=i)
            text = Path(path).read_text()
            obin.local.write_text(text)
            for line in text.splitlines():
                if line.startswith(">"):
                    t.write(f"{line[1:].split()[0]}\t{Path(path).stem}\n")
            manifest.append({bins: obin.local})

    return ExecutionResult(manifest=manifest + [{table: otable.local}], success=len(manifest) > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=48)),
)
