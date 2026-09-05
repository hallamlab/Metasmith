from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::gtdbtk.env"))
ref     = model.AddRequirement(lib.GetType("ref::gtdb"))
asm     = model.AddRequirement(lib.GetType("sequences::putative_genome"))
msa     = model.AddProduct(lib.GetType("taxonomy::gtdbtk_msa"))
tree    = model.AddProduct(lib.GetType("taxonomy::gtdbtk_tree"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   export GTDBTK_DATA_PATH=/ref
    #   gtdbtk identify --genome_dir ./assemblies -x fna --out_dir ws --cpus $cpus
    #   gtdbtk align --identify_dir ws --out_dir ws --cpus $cpus
    #   cp ws/align/*.user_msa.fasta.gz -> {imsa}
    #   gtdbtk infer --msa_file {imsa} --out_dir ws_infer --cpus $cpus
    #   cp ws_infer/*.unrooted.tree -> {itree}
    #
    # Same tool as `metagenomics/taxonomy/gtdbtk.py`, which runs `classify_wf` --
    # identify and align included -- and then globs only `classify/*summary.tsv`, so the
    # concatenated marker MSA it wrote is discarded. This declares that MSA and adds the
    # one command classify_wf does not run.
    made = {
        msa:  context.Output(msa),
        tree: context.Output(tree),
    }
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
    group_by=asm,
    resources=Resources(
        cpus=8,
        memory=Size.GB(240),
        duration=Duration(hours=24),
    ),
)
