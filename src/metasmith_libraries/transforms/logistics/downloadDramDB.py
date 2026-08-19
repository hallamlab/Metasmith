from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddProduct(lib.GetType("annotation::dram_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    threads = context.params.get("cpus", 8)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            export HOME=/tmp
            export PYTHONHTTPSVERIFY=0
            export DRAM_CONFIG_LOCATION={idb.container}/DRAM.config
            mkdir -p {idb.container}
            DRAM-setup.py prepare_databases \
                --output_dir {idb.container} \
                --select_db kofam_hmm \
                --select_db kofam_ko_list \
                --select_db pfam \
                --select_db pfam_hmm \
                --select_db dbcan \
                --threads {threads}
            sed -i 's|{idb.container}|/db|g' {idb.container}/DRAM.config
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=(idb.local / "DRAM.config").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
