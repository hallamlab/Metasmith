# MAGScoT over one assembly's three binner tables, one of four interchangeable dereplicators.
# Defaults throughout; the image ships GTDB r207's marker HMMs at /opt/hmm. Proteins come from the
# plan's Prodigal ORFs, whose `contig_N` names MAGScoT maps back to contigs.
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


def protocol(context: ExecutionContext):
    rows = []
    for label, dep in tables.items():
        for line in Path(context.Input(dep).local).read_text().splitlines():
            fields = line.split("\t")
            # metabat2 and SemiBin2 tables open with a `contig<TAB>bin` header.
            if len(fields) < 2 or fields[0].strip().lower() == "contig":
                continue
            rows.append(f"{label}_{fields[1]}\t{fields[0]}\t{label}\n")
    Path("contig_to_bin.tsv").write_text("".join(rows))

    otable, oscores = context.Output(table), context.Output(scores)
    if not rows:
        Log.Warn("no binner binned a contig; writing empty tables")
        otable.local.write_text("contig\tbin\n")
        oscores.local.write_text("")
        return ExecutionResult(manifest=[{table: otable.local, scores: oscores.local}], success=True)

    cpus = context.params.get("cpus") or 1
    context.ExecWithEnv(env=image, cmd=f"""
        hmmsearch -o /dev/null --tblout tigr.out --noali --notextw --cut_nc --cpu {cpus} /opt/hmm/gtdbtk_rel207_tigrfam.hmm {context.Input(orfs).container}
        hmmsearch -o /dev/null --tblout pfam.out --noali --notextw --cut_nc --cpu {cpus} /opt/hmm/gtdbtk_rel207_Pfam-A.hmm {context.Input(orfs).container}
        {{ grep -v "^#" tigr.out | awk '{{print $1"\\t"$3"\\t"$5}}'; grep -v "^#" pfam.out | awk '{{print $1"\\t"$4"\\t"$5}}'; }} > markers.hmm
        Rscript /opt/MAGScoT.R -i contig_to_bin.tsv --hmm markers.hmm -o MAGScoT
    """)

    refined = Path("MAGScoT.refined.contig_to_bin.out").read_text().splitlines()[1:]
    with open(otable.local, "w") as f:
        f.write("contig\tbin\n")
        f.writelines(f"{contig}\t{bin_}\n" for bin_, contig in (r.split("\t") for r in refined if r))
    oscores.local.write_bytes(Path("MAGScoT.refined.out").read_bytes())

    return ExecutionResult(manifest=[{table: otable.local, scores: oscores.local}], success=True)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
