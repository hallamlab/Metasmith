# MAGScoT over one assembly's three binner tables, one of four interchangeable dereplicators.
# Defaults throughout; the image ships GTDB r207's marker HMMs at /opt/hmm.
from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("bench::magscot.env"))
asm     = model.AddRequirement(lib.GetType("sequences::assembly"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"), parents={asm})
tables  = {label: model.AddRequirement(lib.GetType(f"binning::{label}_contig_to_bin_table"), parents={asm})
           for label in ("metabat2", "semibin2", "comebin")}
table   = model.AddProduct(lib.GetType("bench::magscot_contig_to_bin"))
scores  = model.AddProduct(lib.GetType("bench::magscot_scores"))


def contig_of(protein_id):
    # The plan's ORFs are named `<batch prefix>~<contig>|<start>_<end>_<n>`; plain Prodigal writes `<contig>_<n>`.
    if "|" in protein_id:
        return protein_id.split("~")[-1].split("|")[0]
    return protein_id.rsplit("_", 1)[0]


def protocol(context: ExecutionContext):
    rows = []
    for label, dep in tables.items():
        for line in Path(context.Input(dep).local).read_text().splitlines():
            fields = line.split("\t")
            # metabat2 and SemiBin2 tables open with a `contig<TAB>bin` header; COMEBin's has none.
            if len(fields) < 2 or fields[0].strip().lower() == "contig":
                continue
            rows.append(f"{label}_{fields[1]}\t{fields[0]}\t{label}\n")
    Path("contig_to_bin.tsv").write_text("".join(rows))

    # MAGScoT maps a protein to its contig by dropping the trailing `_<n>`.
    counts = {}
    with open(context.Input(orfs).local) as src, open("proteins.faa", "w") as dst:
        for line in src:
            if line.startswith(">"):
                contig = contig_of(line[1:].split()[0])
                counts[contig] = counts.get(contig, 0) + 1
                line = f">{contig}_{counts[contig]}\n"
            dst.write(line)

    otable, oscores = context.Output(table), context.Output(scores)
    manifest = [{table: otable.local, scores: oscores.local}]
    if rows:
        cpus = context.params.get("cpus") or 1
        context.ExecWithEnv(env=image, cmd=f"""
            hmmsearch -o /dev/null --tblout tigr.out --noali --notextw --cut_nc --cpu {cpus} /opt/hmm/gtdbtk_rel207_tigrfam.hmm proteins.faa
            hmmsearch -o /dev/null --tblout pfam.out --noali --notextw --cut_nc --cpu {cpus} /opt/hmm/gtdbtk_rel207_Pfam-A.hmm proteins.faa
            {{ grep -v "^#" tigr.out | awk '{{print $1"\\t"$3"\\t"$5}}'; grep -v "^#" pfam.out | awk '{{print $1"\\t"$4"\\t"$5}}'; }} > markers.hmm
            Rscript /opt/MAGScoT.R -i contig_to_bin.tsv --hmm markers.hmm -o MAGScoT
        """)
    refined, stats = Path("MAGScoT.refined.contig_to_bin.out"), Path("MAGScoT.refined.out")
    if not (refined.exists() and stats.exists()):
        Log.Warn("MAGScoT selected no bin; writing empty tables")
        otable.local.write_text("contig\tbin\n")
        oscores.local.write_text("")
        return ExecutionResult(manifest=manifest, success=True)

    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        for r in refined.read_text().splitlines()[1:]:
            if r:
                bin_, contig = r.split("\t")
                f.write(f"{contig}\t{bin_}\n")
    oscores.local.write_bytes(stats.read_bytes())

    return ExecutionResult(manifest=manifest, success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
