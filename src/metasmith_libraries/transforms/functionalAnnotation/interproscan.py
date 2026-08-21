from metasmith.python_api import *
from pathlib import Path
import csv

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::interproscan.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
data_dir = model.AddRequirement(lib.GetType("ref::interproscan_data"))
out_gff = model.AddProduct(lib.GetType("annotation::interproscan_results_chunk"))
out_descriptions = model.AddProduct(lib.GetType("annotation::interproscan_descriptions_chunk"))


def parse_interpro_gff(input_path, output_path, descriptions_path):
    headers = ["orf", "start", "stop", "score", "database", "Name", "Dbxref"]
    descriptions = {}
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
            name = attrs.get("Name", "")
            if name:
                descriptions.setdefault(name, attrs.get("signature_desc", ""))
            writer.writerow({
                "orf": parts[0],
                "start": parts[3],
                "stop": parts[4],
                "score": score,
                "database": parts[1],
                "Name": name,
                "Dbxref": dbxref,
            })
    write_descriptions(descriptions, descriptions_path)


def write_descriptions(descriptions, path):
    with open(path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Name", "description"])
        for name in sorted(descriptions):
            writer.writerow([name, descriptions[name]])


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idata = context.Input(data_dir)
    igff = context.Output(out_gff)
    idesc = context.Output(out_descriptions)

    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"--cpu {cpus}"

    context.LocalShell(f"pigz -dc {idata.local} | tar xf -")

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

    gff_files = list(Path("output").glob("*.gff3"))
    if gff_files:
        parse_interpro_gff(str(gff_files[0]), str(igff.local), str(idesc.local))
    else:
        write_descriptions({}, str(idesc.local))

    return ExecutionResult(
        manifest=[
            {
                out_gff: igff.local,
                out_descriptions: idesc.local,
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
