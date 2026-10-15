# dRep over every bin in a study from the three binners, one of four interchangeable dereplicators.
# Settings are Pratama's (MetaG_and_MAGs_bioinformatics.md:104). CheckM2 supplies the quality dRep
# would otherwise compute with CheckM 1.
import csv
import json
import shutil
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::drep.env"))
study   = model.AddRequirement(lib.GetType("viromics::contig_study"))
pair    = model.AddRequirement(lib.GetType("sequences::read_pair"), parents={study})
asm     = model.AddRequirement(lib.GetType("sequences::assembly"), parents={pair})
sets    = {}
for label in ("metabat2", "semibin2", "comebin"):
    bins = model.AddRequirement(lib.GetType(f"sequences::{label}_bin_fasta"), parents={asm})
    sets[label] = (bins, model.AddRequirement(lib.GetType("bench::checkm2_quality"), parents={bins}))
out     = model.AddProduct(lib.GetType("bench::drep_study_winners"))

ARGS = "-pa 0.90 -sa 0.99 -comp 50 -con 10"
COLUMNS = ("genome", "sample", "binner", "primary_cluster", "secondary_cluster", "winner")


def sample_label(read_pair: Path) -> str:
    # A pool given's file is named for its content hash; the sample id is the file's text.
    try:
        text = read_pair.read_text().strip()
    except UnicodeDecodeError:
        return read_pair.stem
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = text
    return value if isinstance(value, str) and value and "\n" not in value else read_pair.stem


def protocol(context: ExecutionContext):
    Path("bins").mkdir()
    genomes, info = {}, {}
    for label, (bins, quality) in sets.items():
        names = {}
        for k, ibin in enumerate(context.InputGroup(bins)):
            source = context.SourceOf(ibin, pair)
            sample = sample_label(Path(source.local)) if source else "unknown"
            name = f"{sample}__{label}__{k}_{ibin.local.stem}.fa"
            shutil.copy(ibin.local, Path("bins") / name)
            names[str(ibin.local)] = name
            genomes[name] = (sample, label)
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
        Path("genomes.txt").write_text("".join(f"bins/{g}\n" for g in info))
        cpus = context.params.get("cpus") or 1
        context.ExecWithEnv(env=image, cmd=f"""
            export HOME="$PWD" MPLCONFIGDIR="$PWD/.mpl"
            dRep dereplicate drep_out -p {cpus} {ARGS} --genomeInfo genome_info.csv -g genomes.txt
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
        for genome, (sample, binner) in genomes.items():
            c = clusters.get(genome)
            if genome not in info:
                status = "no_quality"
            elif c is None:
                status = "filtered"
            else:
                status = int(genome in winners)
            w.writerow((genome, sample, binner, c["primary_cluster"] if c else "",
                        c["secondary_cluster"] if c else "", status))

    return ExecutionResult(manifest=[{out: ounit.local}], success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    resources=Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=12)),
)
