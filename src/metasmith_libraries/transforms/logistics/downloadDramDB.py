from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::dram.env"))
db    = model.AddProduct(lib.GetType("annotation::dram_db"))


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    threads = context.params.get("cpus", 8)

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export HOME=/tmp
            export PYTHONHTTPSVERIFY=0
            export DRAM_CONFIG_LOCATION={idb.container}/DRAM.config
            mkdir -p {idb.container}
            # Seed a valid config BEFORE DRAM-setup.py runs. DRAM 1.5.0's
            # DatabaseHandler.__init__ calls load_config(DRAM_CONFIG_LOCATION)
            # UNCONDITIONALLY, including during prepare_databases' own first-time setup,
            # so json.loads on a path nothing has created yet raises FileNotFoundError
            # about seven seconds in, before a single byte is downloaded. `mkdir -p`
            # makes the directory and nothing makes the file. The image ships a valid
            # 705-byte default at mag_annotator/CONFIG; copy it and let
            # prepare_databases write its own entries into our writable copy.
            python -c "import mag_annotator, os, shutil; \
                src=os.path.join(os.path.dirname(mag_annotator.__file__), 'CONFIG'); \
                shutil.copyfile(src, '{idb.container}/DRAM.config')"
            # CAUTION the five *_form / *_database entries are NOT optional extras.
            # database_processing.prepare_databases narrows its whole settings dict to
            # `select_db`, and DRAM's five distillation SHEETS are members of that same
            # dict -- so naming only the search databases drops them. The image ships no
            # mag_annotator/data/, and the seeded CONFIG has every sheet null, so
            # summarize_vgfs.py then raises "Genome summary form location must be set in
            # order to summarize genomes" and `DRAM-v.py distill` cannot run at all.
            # `DRAM-v.py annotate` only WARNS about missing databases, so the failure
            # surfaces one step later than its cause. The sheets are small GitHub fetches.
            DRAM-setup.py prepare_databases \
                --output_dir {idb.container} \
                --select_db kofam_hmm \
                --select_db kofam_ko_list \
                --select_db pfam \
                --select_db pfam_hmm \
                --select_db dbcan \
                --select_db genome_summary_form \
                --select_db module_step_form \
                --select_db etc_module_database \
                --select_db function_heatmap_form \
                --select_db amg_database \
                --threads {threads}
            sed -i 's|{idb.container}|/db|g' {idb.container}/DRAM.config
        """,
    )

    # CAUTION do NOT test for DRAM.config. The protocol above CREATES it as its first
    # action, before a byte is downloaded, so a predicate on its existence is vacuous --
    # it would report success for a step that downloaded nothing. That is exactly what
    # this predicate did until it was caught in review.
    #
    # Test the PROCESSED artefacts instead, and test the config's CONTENT rather than its
    # presence: DRAM's downloaders log per-database failures and carry on, so a config
    # whose entries are null is the signature of a step that ran and achieved nothing.
    import json

    config = idb.local / "DRAM.config"
    processed = list((idb.local).rglob("*.hmm")) + list((idb.local).rglob("*.hmm.gz")) \
        + list((idb.local).rglob("kofam*")) + list((idb.local).rglob("*dbcan*"))
    sheets_set = False
    if config.exists():
        try:
            loaded = json.loads(config.read_text())
            sheets = loaded.get("dram_sheets") or {}
            sheets_set = any(v for v in sheets.values())
        except (ValueError, OSError):
            sheets_set = False

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=config.exists() and bool(processed) and sheets_set,
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
