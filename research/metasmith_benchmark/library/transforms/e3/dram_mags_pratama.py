# Pratama's MAG annotation (reproduction_map A14): `DRAM.py annotate --min_contig_size 1000`, then `distill`.
# Pratama annotates the dereplicated MAGs. dRep is a gapfill, so this annotates each sample's refined bins.
# CAUTION the staged DRAM database lacks dbCAN (BLOCKERS B3), so no CAZyme annotations come out.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddRequirement(lib.GetType("annotation::dram_db"))
asm   = model.AddRequirement(lib.GetType("sequences::spades_assembly"))
bins  = model.AddRequirement(lib.GetType("sequences::metawrap_bin_fasta"), parents={asm})
out_annot   = model.AddProduct(lib.GetType("e3::mag_dram_annotations"))
out_distill = model.AddProduct(lib.GetType("e3::mag_dram_distill"))


def protocol(context: ExecutionContext):
    idb = context.Input(db)
    iannot = context.Output(out_annot)
    idistill = context.Output(out_distill)
    threads = context.params.get("cpus", 8)

    mags = Path("mags")
    mags.mkdir(exist_ok=True)
    for i, handle in enumerate(context.InputGroup(bins)):
        (mags / f"bin_{i}.fa").write_bytes(handle.local.read_bytes())

    context.ExecWithEnv(env=image, binds=[(idb.external, "/db")], cmd=f"""
        export HOME=/tmp
        DRAM.py annotate -i 'mags/*.fa' -o dram_annot --threads {threads} --min_contig_size 1000 \
            --config_loc /db/DRAM.config
        DRAM.py distill -i dram_annot/annotations.tsv -o dram_distill --config_loc /db/DRAM.config
    """)
    context.LocalShell(f"cp dram_annot/annotations.tsv {iannot.local}")
    context.LocalShell(f"cp -r dram_distill {idistill.local}")
    return ExecutionResult(manifest=[{out_annot: iannot.local, out_distill: idistill.local}],
                           success=iannot.local.exists() and idistill.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=16, memory=Size.GB(64), duration=Duration(hours=24)),
)
