from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bakta.env"))
orfs    = model.AddRequirement(lib.GetType("sequences::orfs"))
db      = model.AddRequirement(lib.GetType("annotation::bakta_db"))
hits    = model.AddProduct(lib.GetType("annotation::bakta_protein_hits"))
infer   = model.AddProduct(lib.GetType("annotation::bakta_protein_inference"))
hypo    = model.AddProduct(lib.GetType("annotation::bakta_hypotheticals"))
record  = model.AddProduct(lib.GetType("annotation::bakta_protein_json"))
named   = model.AddProduct(lib.GetType("annotation::bakta_annotated_proteins"))

def protocol(context: ExecutionContext):
    # STUB. The protocol this replaces:
    #   bakta_proteins --db /db/db-light --prefix bakta_cds --output bakta_out \
    #       --threads $cpus {iorfs.container}
    #   cp bakta_out/bakta_cds.tsv                  {ihits.local}
    #   cp bakta_out/bakta_cds.inference.tsv        {iinfer.local}
    #   cp bakta_out/bakta_cds.hypotheticals.tsv    {ihypo.local}
    #   cp bakta_out/bakta_cds.json                 {irecord.local}
    #   cp bakta_out/bakta_cds.faa                  {inamed.local}
    #
    # `bakta_proteins` is a SEPARATE entry point beside `bakta` in the pinned 1.11.0 image
    # and takes a protein FASTA, so this is the CDS arm the shipped `bakta_noncoding.py`
    # skips with --skip-cds. The two are complementary, not alternatives.
    made = {
        hits:   context.Output(hits),
        infer:  context.Output(infer),
        hypo:   context.Output(hypo),
        record: context.Output(record),
        named:  context.Output(named),
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
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
