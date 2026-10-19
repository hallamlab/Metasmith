import json

from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::dram.env"))
sources = model.AddRequirement(lib.GetType("annotation::dram_sources"))
db      = model.AddProduct(lib.GetType("annotation::dram_db"))

SHEETS = ("genome_summary_form", "module_step_form", "etc_module_database",
          "function_heatmap_form", "amg_database")
# prepare_databases builds its locations from its `*_loc` arguments only, so the CLI's
# --dbcan_fam_activities never arrives, and subfam_ec has no flag at all. Selected without
# a location, DRAM downloads both from bcb.unl.edu, which answers with a web page.
# DatabaseHandler.set_database_paths takes both, and a --select_db run keeps that config.
# The set_database_locations CLI over it fails on its own camper_tar_gz_loc argument.
DESCRIPTIONS = ("dbcan_fam_activities", "dbcan_subfam_ec")


def protocol(context: ExecutionContext):
    idb = context.Output(db)
    isrc = context.Input(sources)
    threads = context.params.get("cpus", 8)

    rows = [line.rstrip("\n").split("\t") for line in open(isrc.local/"sources.tsv") if line.strip()]
    locs = []
    described = {key: name for key, name, _ in rows if key in DESCRIPTIONS}
    rows = [row for row in rows if row[0] not in DESCRIPTIONS]
    for key, name, _ in rows:
        # process_dbcan MOVES its input into --output_dir, and the input is a cached
        # product, so it is handed a copy. Every other database is only read.
        path = name if key == "dbcan" else f"{isrc.container}/{name}"
        locs.append(f"--{key}_loc {path} --select_db {key}")
    dbcan = next(name for key, name, _ in rows if key == "dbcan")
    # A server that answers with a web page passes wget, so check what each file is
    # before DRAM-setup.py, which logs a bad database and carries on.
    checks = [f"gzip -t {isrc.container}/{name}" for _, name, _ in rows if name.endswith(".gz")] + [
        f'[ "$(head -c 6 {isrc.container}/{dbcan})" = HMMER3 ] || {{ echo "{dbcan} is not an HMMER3 file" >&2; exit 1; }}',
        *(f"awk -F'\\t' 'NR==1 {{exit !(NF>1)}}' {isrc.container}/{name} || {{ echo \"{name} is not a TSV\" >&2; exit 1; }}"
          for _, name, _ in rows if name.endswith(".tsv")),
        *(f'! grep -qi "<html" {isrc.container}/{name} || {{ echo "{name} is a web page" >&2; exit 1; }}'
          for name in described.values()),
    ]
    context.ExecWithEnv(env=image, cmd="\n".join(["set -euo pipefail", *checks]))

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            export HOME=/tmp
            export DRAM_CONFIG_LOCATION={idb.container}/DRAM.config
            mkdir -p {idb.container}
            # Seed a valid config BEFORE DRAM-setup.py runs. DRAM 1.5.0's
            # DatabaseHandler.__init__ calls load_config(DRAM_CONFIG_LOCATION)
            # UNCONDITIONALLY, including during prepare_databases' own first-time setup,
            # so json.loads on a path nothing has created yet raises FileNotFoundError
            # before any work is done. `mkdir -p` makes the directory and nothing makes
            # the file. The image ships a valid 705-byte default at mag_annotator/CONFIG;
            # copy it and let prepare_databases write its own entries into our copy.
            python -c "import mag_annotator, os, shutil; \
                src=os.path.join(os.path.dirname(mag_annotator.__file__), 'CONFIG'); \
                shutil.copyfile(src, '{idb.container}/DRAM.config')"
            cp {isrc.container}/{dbcan} {dbcan}
            {' '.join(f"cp {isrc.container}/{name} {idb.container}/{name};" for name in described.values())}
            python -c "import logging; from mag_annotator.database_handler import DatabaseHandler; \
                DatabaseHandler(logging.getLogger('dram')).set_database_paths( \
                {', '.join(f"{key}_loc='{idb.container}/{name}'" for key, name in described.items())}, write_config=True)"
            # CAUTION the five sheets are NOT optional extras. prepare_databases narrows
            # its whole settings dict to `select_db`, and DRAM's distillation SHEETS are
            # members of that same dict -- so naming only the search databases drops
            # them, and `DRAM-v.py distill` then raises "Genome summary form location
            # must be set". `DRAM-v.py annotate` only WARNS about missing databases, so
            # that failure surfaces one step later than its cause.
            DRAM-setup.py prepare_databases \
                --output_dir {idb.container} \
                {' '.join(locs)} \
                --threads {threads}
            # convertmsa's alignment database is only the way to pfam.mmspro, and it is
            # the bulk of the directory.
            rm -f {idb.container}/pfam.mmsmsa {idb.container}/pfam.mmsmsa.*
            sed -i -e "s|\\"$(realpath {idb.container})/|\\"/db/|g" -e 's|"{idb.container}/|"/db/|g' \
                {idb.container}/DRAM.config
        """,
    )

    # CAUTION do NOT test for DRAM.config. The protocol above CREATES it as its first
    # action, so a predicate on its existence is vacuous. Test that every entry the
    # consumers read is set, rewritten to /db, and names a file that is there: DRAM logs
    # per-database failures and carries on, so a null entry is the signature of a step
    # that ran and achieved nothing. A null description_db, for one, passes here and
    # then fails every DRAM pfam annotation that reads it.
    try:
        config = json.loads((idb.local/"DRAM.config").read_text())
    except (OSError, ValueError):
        config = {}

    def located(value):
        return isinstance(value, str) and value.startswith("/db/") \
            and (idb.local/value.removeprefix("/db/")).exists()

    search = config.get("search_databases") or {}
    descriptions = config.get("database_descriptions") or {}
    sheets = config.get("dram_sheets") or {}
    whole = all(located(search.get(k)) for k in ("kofam_hmm", "kofam_ko_list", "pfam", "dbcan")) \
        and all(located(descriptions.get(k)) for k in ("pfam_hmm", *DESCRIPTIONS)) \
        and all(located(sheets.get(k)) for k in SHEETS) \
        and located(config.get("description_db"))
    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=whole,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["noscratch"],
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    ),
)
