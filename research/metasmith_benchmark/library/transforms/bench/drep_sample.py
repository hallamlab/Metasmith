# dRep over one assembly's bins from the three binners, one of four interchangeable dereplicators.
# Settings are Pratama's (MetaG_and_MAGs_bioinformatics.md:104). CheckM2 supplies the quality dRep
# would otherwise compute with CheckM 1.
import csv
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::drep.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
sets    = {}
for label in ("metabat2", "semibin2", "comebin"):
    bins = model.AddRequirement(lib.GetType(f"sequences::{label}_bin_fasta"), parents={asm})
    sets[label] = (bins, model.AddRequirement(lib.GetType("bench::checkm2_quality"), parents={bins}))
out     = model.AddProduct(lib.GetType("bench::drep_sample_winners"))

ARGS = "-pa 0.90 -sa 0.99 -comp 50 -con 10"
COLUMNS = ("genome", "primary_cluster", "secondary_cluster", "winner")


def protocol(context: ExecutionContext):
    Path("bins").mkdir()
    genomes, info = [], {}
    for label, (bins, quality) in sets.items():
        names = {}
        for k, ibin in enumerate(context.InputGroup(bins)):
            name = f"{label}__{k}_{ibin.local.stem}.fa"
            shutil.copy(ibin.local, Path("bins") / name)
            names[str(ibin.local)] = name
            genomes.append(name)
        # The quality slot may carry other samples' rows, so pair each row to a bin of this group.
        for iq in context.InputGroup(quality):
            source = context.SourceOf(iq, bins)
            if source is None or str(source.local) not in names:
                continue
            with open(iq.local, newline="") as f:
                row = next(csv.DictReader(f, delimiter="\t"))
            info[names[str(source.local)]] = (row["Completeness"], row["Contamination"])

    ounit = context.Output(out)
    if genomes and not info:
        Log.Error(f"none of {len(genomes)} bins paired to a CheckM2 row")
        return ExecutionResult(manifest=[], success=False)
    if len(info) < len(genomes):
        Log.Warn(f"{len(genomes) - len(info)} of {len(genomes)} bins have no CheckM2 row; marked no_quality")

    clusters, winners = {}, set()
    if info:
        with open("genome_info.csv", "w", newline="") as f:
            csv.writer(f).writerows([("genome", "completeness", "contamination"),
                                     *((g, *q) for g, q in info.items())])
        cpus = context.params.get("cpus") or 1
        context.ExecWithEnv(env=image, cmd=f"""
            export HOME="$PWD" MPLCONFIGDIR="$PWD/.mpl"
            dRep dereplicate drep_out -p {cpus} {ARGS} --genomeInfo genome_info.csv -g {" ".join(f"bins/{g}" for g in info)}
        """)
        # dRep writes no tables when every genome fails its length or quality filter.
        if Path("drep_out/data_tables/Cdb.csv").exists():
            with open("drep_out/data_tables/Cdb.csv", newline="") as f:
                clusters = {r["genome"]: r for r in csv.DictReader(f)}
            with open("drep_out/data_tables/Wdb.csv", newline="") as f:
                winners = {r["genome"] for r in csv.DictReader(f)}
        else:
            Log.Warn("dRep wrote no Cdb.csv; every genome failed its filters")

    with open(ounit.local, "w", newline="") as f:
        w = csv.writer(f, delimiter="\t", lineterminator="\n")
        w.writerow(COLUMNS)
        for genome in genomes:
            c = clusters.get(genome)
            if genome not in info:
                status = "no_quality"
            elif c is None:
                status = "filtered"
            else:
                status = int(genome in winners)
            w.writerow((genome, c["primary_cluster"] if c else "", c["secondary_cluster"] if c else "", status))

    return ExecutionResult(manifest=[{out: ounit.local}], success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(32), duration=Duration(hours=4)),
)
