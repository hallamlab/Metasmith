from metasmith.python_api import *
from pathlib import Path
import csv

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::interproscan.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
data_dir = model.AddRequirement(lib.GetType("ref::interproscan_data"))
out_gff = model.AddProduct(lib.GetType("annotation::interproscan_results_chunk"))


def parse_interpro_gff(input_path, output_path):
    headers = ["orf", "start", "stop", "score", "database", "Name", "Dbxref"]
    with open(input_path, 'r') as gff_file, open(output_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()
        for line in gff_file:
            if line.startswith("#") or not line.strip():
                continue
            parts = line.strip().split('\t')
            if len(parts) < 9:
                continue
            if parts[2] != "protein_match":
                continue
            score = parts[5]
            score = "" if score == "." else score
            attrs = {}
            for item in parts[8].split(';'):
                if '=' in item:
                    key, value = item.split('=', 1)
                    attrs[key] = value.strip('"')
            dbxref = attrs.get("Dbxref", "")
            if dbxref.startswith("InterPro:"):
                dbxref = dbxref.replace("InterPro:", "", 1)
            writer.writerow({
                "orf": parts[0],
                "start": parts[3],
                "stop": parts[4],
                "score": score,
                "database": parts[1],
                "Name": attrs.get("Name", ""),
                "Dbxref": dbxref,
            })


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idata = context.Input(data_dir)
    igff = context.Output(out_gff)

    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"--cpu {cpus}"

    # Extract InterProScan data archive locally
    context.LocalShell(f"pigz -dc {idata.local} | tar xf -")

    # sed to remove stop codon asterisks from protein sequences
    # Run InterProScan with extracted data directory. I5OPTS bumps the
    # JVM heap so the Java side can address most of the container RAM
    # (default Xmx is too small and OOMs on full-sample protein FASTAs).
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(context.external_cwd/"data", "/opt/interproscan/data")],
        cmd=f"""
            mkdir -p output
            export I5OPTS="-Xms4g -Xmx48g"
            sed '/^[^>]/s/\*//g' {iorfs.container} > ./{iorfs.container.stem}.clean.faa
            /opt/interproscan/interproscan.sh \
                --disable-precalc \
                --verbose \
                --seqtype p \
                {cpus_string} \
                -i ./{iorfs.container.stem}.clean.faa \
                -f gff3 \
                -d output
        """,
    )

    # Parse GFF3 output into CSV
    gff_files = list(Path("output").glob("*.gff3"))
    if gff_files:
        parse_interpro_gff(str(gff_files[0]), str(igff.local))

    return ExecutionResult(
        manifest=[
            {
                out_gff: igff.local,
            },
        ],
        success=igff.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=16,
        memory=Size.GB(64),
        duration=Duration(hours=24),
    ),
)
