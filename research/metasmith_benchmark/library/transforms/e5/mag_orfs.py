# One MAG's proteins, selected from its assembly's ORFs by the contigs the MAG holds, in place of
# calling genes again on the bin. Prodigal names each protein <contig>_<n>, so the contig is the id
# minus its last underscore field.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm   = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
bin_  = model.AddRequirement(lib.GetType("sequences::das_tool_bin_fasta"), parents={asm})
out   = model.AddProduct(lib.GetType("sequences::bin_orfs"))


def _contigs(path: Path) -> set[str]:
    with open(path) as f:
        return {line[1:].split()[0] for line in f if line.startswith(">")}


def protocol(context: ExecutionContext):
    ibin, iorfs, iout = context.Input(bin_), context.Input(orfs), context.Output(out)
    contigs = _contigs(ibin.local)
    kept, keep = 0, False
    with open(iorfs.local) as src, open(iout.local, "w") as dst:
        for line in src:
            if line.startswith(">"):
                keep = line[1:].split()[0].rpartition("_")[0] in contigs
                kept += keep
            if keep:
                dst.write(line)
    Log.Info(f"{kept} ORFs on {len(contigs)} contigs")
    return ExecutionResult(manifest=[{out: iout.local}], success=kept > 0)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin_,
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)
