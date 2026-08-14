from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
identity = model.AddRequirement(lib.GetType("clustering::min_identity"))
centroids_out = model.AddProduct(lib.GetType("clustering::centroids"))
table_out     = model.AddProduct(lib.GetType("clustering::cluster_table"))

CLUSTERS_TSV = "clusters.tsv"

def protocol(context: ExecutionContext):
    iorfs     = context.Input(orfs)
    iidentity = context.Input(identity)
    icentroids = context.Output(centroids_out)
    itable     = context.Output(table_out)

    # Read identity threshold from the typed input file
    min_id = open(iidentity.local).readline().strip()

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"

    memory = context.params.get('memory')
    memory = "" if memory is None else f"--memory-limit {int(float(memory))-8}G"

    # Build diamond database and run linclust inside container
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            diamond makedb --in {iorfs.container} -d orfs_db \
            && diamond linclust \
                {threads} {memory} \
                -d orfs_db \
                -o {CLUSTERS_TSV} \
                --approx-id {min_id} \
                --member-cover 80
        """,
    )

    # Parse cluster table to extract unique centroid IDs (column 1)
    centroid_ids = set()
    with open(Path(CLUSTERS_TSV)) as f:
        for line in f:
            fields = line.strip().split("\t")
            if fields:
                centroid_ids.add(fields[0])

    # Read input FASTA and write only centroid sequences to output
    with open(iorfs.local) as fin, open(icentroids.local, "w") as fout:
        writing = False
        for line in fin:
            if line.startswith(">"):
                seq_id = line[1:].split()[0]
                writing = seq_id in centroid_ids
            if writing:
                fout.write(line)

    # Copy cluster table to output path
    import shutil
    shutil.copy2(Path(CLUSTERS_TSV), itable.local)

    return ExecutionResult(
        manifest=[
            {
                centroids_out: icentroids.local,
                table_out: itable.local,
            },
        ],
        success=icentroids.local.exists() and itable.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=identity,
    resources=Resources(
        cpus=16,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
