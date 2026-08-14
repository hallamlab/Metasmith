from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::kofamscan.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
profiles = model.AddRequirement(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddRequirement(lib.GetType("ref::kofamscan_ko_list"))
out_results = model.AddProduct(lib.GetType("annotation::kofamscan_results_chunk"))


def parse_kofamscan(input_path, output_path):
    header = "gene_name,KO,thrshld,score,E-value,best\n"
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        outfile.write(header)
        for line in infile:
            if line.startswith('#') or not line.strip():
                continue
            stripped = line.strip()
            is_best = "*" if stripped.startswith("*") else ""
            parts = stripped.lstrip('* ').split()
            if len(parts) >= 5:
                gene, ko, thr, score, evalue = parts[:5]
                try:
                    if float(score) >= float(thr):
                        outfile.write(f"{gene},{ko},{thr},{score},{evalue},{is_best}\n")
                except ValueError:
                    outfile.write(f"{gene},{ko},{thr},{score},{evalue},{is_best}\n")


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iprofiles = context.Input(profiles)
    iko_list = context.Input(ko_list)
    iout = context.Output(out_results)

    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"--cpu={cpus}"

    # Extract profiles archive locally
    context.LocalShell(f"pigz -dc {iprofiles.local} | tar xf -")

    # Run KofamScan with extracted database references
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"profiles", "/profiles"),
            (iko_list.external.parent, "/ko"),
        ],
        cmd=f"""
            exec_annotation \
                -o kofam_results.txt \
                --profile=/profiles \
                --ko-list=/ko/{iko_list.external.name} \
                {cpus_string} \
                --e-value=0.01 \
                --format=detail \
                --no-report-unannotated \
                {iorfs.container}
        """,
    )

    # Parse and filter results into CSV
    parse_kofamscan("kofam_results.txt", str(iout.local))

    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=8),
    ),
)
