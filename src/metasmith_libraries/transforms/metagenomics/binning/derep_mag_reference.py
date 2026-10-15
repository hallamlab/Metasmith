from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

img_mm2 = model.AddRequirement(lib.GetType("env::minimap2.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
table   = model.AddRequirement(lib.GetType("binning_local::cluster_table"), parents={asm})
bins    = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
out     = model.AddProduct(lib.GetType("binning::derep_mag_ref"))

# The three filenames are fixed by the consumers, not by this transform:
# `metagenomics/instrain.py` binds the product at /magref and names mag_ref.mmi and
# mag_ref.fna, and `instrain_compare.py` names mag_ref.stb. Renaming any of them here
# is a silent break there.
STEM = "mag_ref"
CENTROID_COL = "is_centroid_95"


def _centroids(path: Path) -> list[str]:
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        assert CENTROID_COL in header, (
            f"cluster table [{path.name}] has no [{CENTROID_COL}] column "
            f"(has {header}) -- this is skani_dedup's output format"
        )
        bid, cen = header.index("bin_id"), header.index(CENTROID_COL)
        picked = []
        for line in f:
            toks = line.rstrip("\n").split("\t")
            if len(toks) <= max(bid, cen):
                continue
            if toks[cen].strip() == "1":
                picked.append(toks[bid].strip())
    return picked


def protocol(context: ExecutionContext):
    itable = context.Input(table)
    ibins = context.InputGroup(bins)
    iout = context.Output(out)

    picked = _centroids(itable.local)
    by_stem = {p.local.stem: p for p in ibins}
    missing = [b for b in picked if b not in by_stem]
    assert not missing, (
        f"[{len(missing)}] centroid bins named by [{itable.local.name}] are not in "
        f"this task's bins: {missing[:5]} -- the cluster table and the bins must "
        f"descend from the same assembly or they are two different dedup runs"
    )

    iout.local.mkdir(parents=True, exist_ok=True)
    fna = iout.local/f"{STEM}.fna"
    stb = iout.local/f"{STEM}.stb"

    # Scaffold names are namespaced by bin id. Assemblers name contigs per-assembly
    # (`k141_37` from megahit, `NODE_1_...` from spades), so bare contig names collide
    # across the bins being concatenated here, and inStrain would attribute one
    # genome's coverage to another.
    n_scaffolds = 0
    with open(fna, "w") as ofa, open(stb, "w") as ostb:
        for bin_id in sorted(picked):
            with open(by_stem[bin_id].local) as f:
                for line in f:
                    if not line.startswith(">"):
                        ofa.write(line)
                        continue
                    contig = line[1:].split()[0].strip()
                    scaffold = f"{bin_id}~{contig}"
                    ofa.write(f">{scaffold}\n")
                    ostb.write(f"{scaffold}\t{bin_id}\n")
                    n_scaffolds += 1

    Log.Info(f"{len(picked)} centroid bins, {n_scaffolds} scaffolds -> {fna.name}")

    context.ExecWithEnv(
        env=img_mm2,
        cmd=f"minimap2 -x sr -d {iout.container}/{STEM}.mmi {iout.container}/{STEM}.fna",
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((iout.local/f"{STEM}.{x}").exists() for x in ("fna", "stb", "mmi")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    # One reference per dedup run, and the cluster table is what a run produces.
    group_by=table,
    output_signature={
        out: "mag_ref",
    },
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=2),
    ),
)
