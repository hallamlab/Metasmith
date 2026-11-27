from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
image    = model.AddRequirement(lib.GetType("env::kofamscan.env"))
proteins = model.AddRequirement(lib.GetType("e4abl::carveme166_bigg_proteins"))
profiles = model.AddRequirement(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddRequirement(lib.GetType("ref::kofamscan_ko_list"))
out      = model.AddProduct(lib.GetType("bench::bigg_gene_kos"))

CPUS = 16


def protocol(context: ExecutionContext):
    iproteins = context.Input(proteins)
    iprofiles = context.Input(profiles)
    iko_list  = context.Input(ko_list)
    iout      = context.Output(out)

    # The bin side's kofamscan transform, with detail-tsv, whose first column is "*" on a hit
    # above its KO's threshold.
    context.ExecWithEnv(
        env=image,
        binds=[(iprofiles.external, "/profiles"), (iko_list.external.parent, "/ko")],
        cmd=f"""
            exec_annotation -o kofam_detail.tsv --profile=/profiles --ko-list=/ko/{iko_list.external.name} \
                --cpu={CPUS} --e-value=0.01 --format=detail-tsv --no-report-unannotated {iproteins.container}
        """,
    )

    genes, kos = set(), set()
    with open("kofam_detail.tsv") as fh, open(iout.local, "w") as fout:
        fout.write("bigg_gene\tKO\tscore\n")
        for line in fh:
            fields = line.rstrip("\n").split("\t")
            if line.startswith("#") or len(fields) < 5 or fields[0] != "*":
                continue
            fout.write(f"{fields[1]}\t{fields[2]}\t{fields[4]}\n")
            genes.add(fields[1])
            kos.add(fields[2])
    print(f"[kofam_bigg_reference] {len(genes):,} BiGG genes carry {len(kos):,} KOs", flush=True)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(genes) > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=proteins,
    resources=Resources(cpus=CPUS, memory=Size.GB(32), duration=Duration(hours=12)),
)
