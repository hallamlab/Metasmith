import csv
import gzip

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::hmmer.env"))
sources = model.AddRequirement(lib.GetType("ref::vogdb_sources"))
db      = model.AddProduct(lib.GetType("ref::vogdb_hmm"))


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    idb = context.Output(db)
    idb.local.mkdir(parents=True, exist_ok=True)

    # DRAM's own description string (DatabaseHandler.process_vogdb_descriptions), so a
    # VOG's category codes are the last `; ` field the way DRAM's formatter reads them.
    n = 0
    with gzip.open(isrc.local/"vog.annotations.tsv.gz", "rt") as fin, \
            open(idb.local/"descriptions.tsv", "w") as fout:
        for row in csv.DictReader(fin, delimiter="\t"):
            fout.write(f"{row['#GroupName']}\t{row['ConsensusFunctionalDescription']}; "
                       f"{row['FunctionalCategory']}\n")
            n += 1
    if n == 0:
        raise SystemExit("[vogdb] vog.annotations.tsv.gz holds no VOGs")

    # The archive keeps one model per file under hmm/; hmmsearch takes one pressed file.
    # Concatenated with find, because 38,000 names overflow a shell glob's argv.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        mkdir vog_hmms
        tar -xzf {isrc.container}/vog.hmm.tar.gz -C vog_hmms
        find vog_hmms -name 'VOG*.hmm' -print0 | sort -z | xargs -0 cat > {idb.container}/vog.hmm
        rm -rf vog_hmms
        hmmpress -f {idb.container}/vog.hmm
    """)

    models = sum(1 for line in open(idb.local/"vog.hmm") if line.startswith("NAME "))
    Log.Info(f"pressed {models:,} VOG models against {n:,} described VOGs")
    pressed = all((idb.local/f"vog.hmm.{ext}").exists() for ext in ("h3f", "h3i", "h3m", "h3p"))
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=pressed and models > 0.95 * n,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
