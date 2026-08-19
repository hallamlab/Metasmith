from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image   = model.AddRequirement(lib.GetType("env::esmc.env"))
source  = model.AddRequirement(lib.GetType("fabfos_data::swissprot"))
bridge  = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
weights = model.AddRequirement(lib.GetType("ref::esm_c_600m_weights"))
bl        = model.AddRequirement(lib.GetType("buildlib::pool"))
pool    = model.AddProduct(lib.GetType("ref::reference_label_pool_esmc"))

POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"

FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"

INDEX_NAME = "orf_index.parquet"
STACK_NAME = "emb_esmc.npy"
SOURCE_NAME = "pool_source.txt"

MODEL_NAME = "esmc_600m"
BATCH_SIZE = 32
MAX_LEN = 2048
CHUNK_OVERLAP = 128

def protocol(context: ExecutionContext):
    ibridge = context.Input(bridge)
    ibl = context.Input(bl)
    isrc    = context.Input(source)
    iw      = context.Input(weights)
    ipool   = context.Output(pool)

    cmd = f"""
            python3 {ibl.container}/select_esmc.py \
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

    context.LocalShell(f"mkdir -p weights && tar -xzf {iw.local} -C weights")

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"weights", "/weights"),
        ],
        cmd=f"""
            python {ibl.container}/embed_esmc.py \
                --weights /weights \
                --model-name {MODEL_NAME} \
                --fasta _pool.faa \
                --out-npy _esmc_emb.npy \
                --out-index _esmc_index.csv \
                --batch-size {BATCH_SIZE} \
                --max-len {MAX_LEN} \
                --chunk-overlap {CHUNK_OVERLAP}
        """,
    )

    cmd = f"""
            python3 {ibl.container}/assemble_esmc.py \
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
    resources=Resources(cpus=8, memory=Size.GB(64), duration=Duration(hours=4),
                        gpus=Gpus.REQUIRED, gpu_memory=Size.GB(24)),
)
