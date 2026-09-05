from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::blast.env"))
subject = model.AddRequirement(lib.GetType("sequences::assembly"))
query   = model.AddRequirement(lib.GetType("sequences::orfs"))
hits    = model.AddProduct(lib.GetType("annotation::homolog_hits"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   makeblastdb -in {isubject.container} -dbtype nucl -out subject_db
    #   tblastn -query {iquery.container} -db subject_db \
    #       -evalue 1e-5 -outfmt 6 -max_target_seqs 10000 -out {ihits.container}
    #
    # Generalises `amplicon/blast_map_asvs.py`, which runs the same two commands and
    # differs only in being typed to `amplicon::asv_seqs` and `amplicon::asv_contig_map`.
    # The query is a chosen set, NOT a fixed reference database -- which is what lets this
    # answer "is this gene in these genomes" rather than "what is in this genome".
    made = {hits: context.Output(hits)}
    for key, path in made.items():
        make = 'mkdir -p' if key in _DIRECTORY_PRODUCTS else 'touch'
        context.external_shell.Exec(f'{make} {path.external}')
    return ExecutionResult(
        manifest=[{k: v.local for k, v in made.items()}],
        success=all(v.local.exists() for v in made.values()),
    )

_DIRECTORY_PRODUCTS = set()

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=subject,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
