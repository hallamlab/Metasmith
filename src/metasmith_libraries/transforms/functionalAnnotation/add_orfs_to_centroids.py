from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image    = model.AddRequirement(lib.GetType("env::diamond.env"))
centroids = model.AddRequirement(lib.GetType("clustering::centroids"))
orfs      = model.AddRequirement(lib.GetType("sequences::orfs"))
identity  = model.AddRequirement(lib.GetType("clustering::min_identity"))
aug_out   = model.AddProduct(lib.GetType("clustering::augmented_centroids"))
assign_out = model.AddProduct(lib.GetType("clustering::new_cluster_assignments"))

HITS_TSV = "hits.tsv"

def protocol(context: ExecutionContext):
    icentroids = context.Input(centroids)
    iorfs      = context.Input(orfs)
    iidentity  = context.Input(identity)
    iaug       = context.Output(aug_out)
    iassign    = context.Output(assign_out)

    # Read identity threshold from the typed input file
    min_id = open(iidentity.local).readline().strip()

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"

    # Build diamond database from existing centroids and search new ORFs
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            diamond makedb --in {icentroids.container} -d centroid_db \
            && diamond blastp \
                -d centroid_db \
                -q {iorfs.container} \
                -o {HITS_TSV} \
                --id {min_id} \
                --query-cover 80 \
                --max-target-seqs 1 \
                {threads}
        """,
    )

    # Parse hits to find which new ORFs matched existing centroids
    # hits.tsv is BLAST tabular: query, subject, pident, ...
    matched = {}  # new_orf_id -> centroid_id
    with open(Path(HITS_TSV)) as f:
        for line in f:
            fields = line.strip().split("\t")
            if len(fields) >= 2:
                query_id = fields[0]
                subject_id = fields[1]
                if query_id not in matched:
                    matched[query_id] = subject_id

    # Read new ORFs FASTA, separate matched from unmatched (new centroids)
    unmatched_seqs = []  # list of (header, seq_lines)
    all_new_orfs = []    # (orf_id, centroid_id, status)
    current_id = None
    current_lines = []
    is_unmatched = False

    with open(iorfs.local) as f:
        for line in f:
            if line.startswith(">"):
                # Flush previous sequence
                if current_id is not None:
                    if is_unmatched:
                        unmatched_seqs.append(current_lines)
                        all_new_orfs.append((current_id, current_id, "new_centroid"))
                    else:
                        all_new_orfs.append((current_id, matched[current_id], "matched"))
                current_id = line[1:].split()[0]
                current_lines = [line]
                is_unmatched = current_id not in matched
            else:
                current_lines.append(line)
        # Flush last sequence
        if current_id is not None:
            if is_unmatched:
                unmatched_seqs.append(current_lines)
                all_new_orfs.append((current_id, current_id, "new_centroid"))
            else:
                all_new_orfs.append((current_id, matched[current_id], "matched"))

    # Write augmented centroids = existing centroids + unmatched new ORFs
    import shutil
    shutil.copy2(icentroids.local, iaug.local)
    with open(iaug.local, "a") as fout:
        for seq_lines in unmatched_seqs:
            for line in seq_lines:
                fout.write(line)

    # Write assignment table
    with open(iassign.local, "w") as fout:
        fout.write("orf_id\tcentroid_id\tstatus\n")
        for orf_id, centroid_id, status in all_new_orfs:
            fout.write(f"{orf_id}\t{centroid_id}\t{status}\n")

    return ExecutionResult(
        manifest=[
            {
                aug_out: iaug.local,
                assign_out: iassign.local,
            },
        ],
        success=iaug.local.exists() and iassign.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(16),
        duration=Duration(hours=3),
    )
)
