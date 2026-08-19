from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::proteinbert.env"))
source = model.AddRequirement(lib.GetType("fabfos_data::swissprot"))
bridge = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
bl        = model.AddRequirement(lib.GetType("buildlib::pool"))
pool   = model.AddProduct(lib.GetType("ref::reference_label_pool"))

POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"

FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"

INDEX_NAME = "orf_index.parquet"
STACK_NAME = "emb_pbert.npy"
SOURCE_NAME = "pool_source.txt"

def protocol(context: ExecutionContext):
    ibridge = context.Input(bridge)
    ibl = context.Input(bl)
    isrc    = context.Input(source)
    ipool   = context.Output(pool)

    cmd = f"""
            python3 {ibl.container}/select_dmnd.py \
            --bridge {ibridge.container} \
            --swissprot {isrc.container} \
            --fasta-file {FASTA_FILE} \
            --reldate-file {RELDATE_FILE} \
            --id-source {POOL_ID_SOURCE} \
            --evidence {POOL_EVIDENCE}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    threads = context.params.get("cpus", 4)
    _cmd = f"""
        pbert run -i _pool.faa -o pbert_output \
            --threads {threads} --protein_size 512 --model_batch 1024 -x 1
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    cmd = f"""
            python3 {ibl.container}/assemble_dmnd.py \
            --pool {ipool.container} \
            --index-name {INDEX_NAME} \
            --stack-name {STACK_NAME} \
            --source-name {SOURCE_NAME}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    ok = all((ipool.local / n).exists() for n in (INDEX_NAME, STACK_NAME, SOURCE_NAME))
    return ExecutionResult(
        manifest=[{pool: ipool.local}],
        success=ok,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=8)),
)
