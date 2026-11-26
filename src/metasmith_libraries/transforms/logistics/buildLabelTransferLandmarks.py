# The pool and the pbert lane's query must come from the same embedding function, so the
# image, the alphabet and the `pbert run` flags are functionalAnnotation/proteinbert.py's.
# Change the two together. ProteinBERT's weights are baked into the image, and it runs on
# CPU, as the query does.
from metasmith.python_api import *

lib       = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model     = Transform()
image     = model.AddRequirement(lib.GetType("env::proteinbert.env"))
swissprot = model.AddRequirement(lib.GetType("ref::swissprot_sequences"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
builder   = model.AddRequirement(lib.GetType("lib::label_transfer_landmarks.py"))
pool      = model.AddProduct(lib.GetType("ref::label_transfer_landmarks"))


def protocol(context: ExecutionContext):
    script = context.Input(builder).container
    ipool = context.Output(pool)
    threads = context.params.get("cpus", 16)
    scratch = "--fasta _pool.faa --labels _pool_labels.parquet --source _pool_source.txt"
    context.ExecWithEnv(env=image, cmd=f"""
        set -e
        python3 {script} select --bridge {context.Input(bridge).container} \
            --swissprot {context.Input(swissprot).container} {scratch}
        pbert run -i _pool.faa -o pbert_output \
            --threads {threads} --protein_size 512 --model_batch 1024 -x 1
        python3 {script} assemble --shards pbert_output {scratch} --out {ipool.container}
    """)
    return ExecutionResult(
        manifest=[{pool: ipool.local}],
        success=all((ipool.local/n).exists() for n in ("landmarks.parquet", "source.txt")),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # ~222k Swiss-Prot sequences, about 44 of the query's 5,000-ORF chunks.
    resources=Resources(cpus=16, memory=Size.GB(48), duration=Duration(hours=48)),
)
