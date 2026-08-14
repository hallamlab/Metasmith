from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddProduct(lib.GetType("annotation::dram_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    threads = context.params.get("cpus", 8)

    # DRAM gene annotation (dram_annotate_genes.py) only needs the KOfam, Pfam
    # and dbCAN databases, so we whitelist those and skip the ~100s-of-GB
    # UniRef build. Mirrors the build documented in cyanoverse/ab48 DRAM_JOURNAL:
    #   - `--select_db` uses action='append' -> one flag per DB, not space-sep.
    #   - PYTHONHTTPSVERIFY=0 works around the bcb.unl.edu (dbCAN) SSL cert.
    #     If dbCAN still 404s/HTMLs (server flaky), pre-stage dbCAN-HMMdb from
    #     the AWS S3 mirror + `hmmpress` and re-run with that file in place.
    #   - HOME=/tmp keeps DRAM's config/cache writes off the read-only image.
    #
    # DRAM.config records *absolute* DB paths from build time; the consumer
    # bind-mounts this dir at /db, so we rewrite the build prefix -> /db after
    # setup. DRAM_CONFIG_LOCATION (resolved bug #7) directs the config write
    # into the output dir so it travels with the database.
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
