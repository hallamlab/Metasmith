from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::metabuli.env"))
scripts = model.AddRequirement(lib.GetType("lib::metabuli_gtdb"))
sources = model.AddRequirement(lib.GetType("ref::metabuli_sources"))
ref     = model.AddProduct(lib.GetType("ref::metabuli_ref"))

ARCHIVE = "gtdb_genomes_reps_r232.tar.gz"
ARCHIVE_MD5 = "a63a586186871a429bf12ea5bc7be0db"
HUMAN = "GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz"


def protocol(context: ExecutionContext):
    isrc = context.Input(sources)
    iscripts = context.Input(scripts)
    idb = context.Output(ref)
    threads = context.params.get("cpus") or 64
    # The fir reference build ran --max-ram 220 under a 256 GB allocation and peaked at
    # 177 GiB resident, so keep the same proportion of whatever the task was given.
    max_ram = int(float(context.params.get("memory") or 256)) * 86 // 100
    src = isrc.container
    common = f"--threads {threads} --max-ram {max_ram}"

    # The fir reference recipe (2026-11-22): GTDB genomes first with --gtdb 1, then human
    # and RefSeq viral added under their NCBI taxa by createnewtaxalist and updateDB.
    # Human is exempt from low-complexity masking. The md5 runs beside the extraction so
    # the 192 GB archive is checked without a separate pass.
    cmd = f"""\
        set -euo pipefail
        mkdir -p genomes taxdump ncbi_taxdump extra
        md5sum {src}/{ARCHIVE} > archive.md5 &
        md5_pid=$!
        tar -xzf {src}/{ARCHIVE} -C genomes -T {src}/members.txt
        wait $md5_pid
        [ "$(cut -d' ' -f1 archive.md5)" = "{ARCHIVE_MD5}" ] || {{ echo "{ARCHIVE} does not match md5 {ARCHIVE_MD5}" >&2; exit 1; }}
        find "$PWD/genomes" -name '*_genomic.fna.gz' | sort > gtdb_fasta.list
        [ "$(wc -l < gtdb_fasta.list)" -eq "$(wc -l < {src}/members.txt)" ]

        tar -xzf {src}/gtdb-taxdump-R232.tar.gz -C taxdump --strip-components=1
        unzip -q {src}/taxdmp_2026-10-01.zip nodes.dmp names.dmp merged.dmp delnodes.dmp -d ncbi_taxdump
        gunzip -c {src}/viral.1.1.genomic.fna.gz > extra/refseq_viral.fna
        gunzip -c {src}/{HUMAN} > extra/human_t2t.fna
        {{
            printf 'accession\\taccession.version\\ttaxid\\tgi\\n'
            zcat {src}/viral.1.genomic.gbff.gz | awk '/^VERSION/{{v=$2}} /\\/db_xref="taxon:/{{if(v!=""){{match($0,/taxon:[0-9]+/); t=substr($0,RSTART+6,RLENGTH-6); a=v; sub(/\\..*/,"",a); print a"\\t"v"\\t"t"\\t0"; v=""}}}}'
            grep '^>' extra/human_t2t.fna | awk '{{v=substr($1,2); a=v; sub(/\\..*/,"",a); print a"\\t"v"\\t9606\\t0"}}'
        }} > extra.accession2taxid
        ls "$PWD"/extra/*.fna > extra_fasta.list

        mkdir -p db_gtdb {idb.container}
        metabuli build --gtdb 1 db_gtdb gtdb_fasta.list taxdump/taxid.map \
            --taxonomy-path taxdump \
            --space-mask 11101110111 --custom-metamer {iscripts.container}/reduced_15_pattern.txt \
            --syncmer 1 --smer-len 6 {common}
        rm -rf genomes
        metabuli createnewtaxalist db_gtdb extra_fasta.list ncbi_taxdump extra.accession2taxid newtaxa
        human=$(awk -F'\\t' '$4=="Homo sapiens"{{print $1}}' newtaxa/newtaxa.tsv)
        [ -n "$human" ] || {{ echo "createnewtaxalist placed no Homo sapiens taxon" >&2; exit 1; }}
        metabuli updateDB {idb.container} extra_fasta.list newtaxa/newtaxa.accession2taxid db_gtdb \
            --new-taxa newtaxa/newtaxa.tsv --make-library 1 --no-mask-taxa "$human" \
            --validate-db 1 {common}
        rm -rf db_gtdb extra

        mkdir -p {idb.container}/taxonomy
        cp taxdump/nodes.dmp taxdump/names.dmp taxdump/merged.dmp taxdump/delnodes.dmp {idb.container}/taxonomy/
        awk -F'\\t' '{{print $1"\\t|\\t"$2"\\t|\\t"$3"\\t|\\t\\t|"}}' newtaxa/newtaxa.tsv >> {idb.container}/taxonomy/nodes.dmp
        awk -F'\\t' '{{print $1"\\t|\\t"$4"\\t|\\t\\t|\\tscientific name\\t|"}}' newtaxa/newtaxa.tsv >> {idb.container}/taxonomy/names.dmp
        cp {src}/selected.tsv {src}/RELEASE_NUMBER {idb.container}/taxonomy/
    """
    context.ExecWithEnv(env=image, cmd=cmd)

    # Assert the database, not the exit code: `metabuli databases` once reported success
    # with no data landed. taxonomy/names.dmp is what lcastar resolves Metabuli calls with.
    def present(*names):
        return all((idb.local/n).is_file() and (idb.local/n).stat().st_size > 0 for n in names)
    return ExecutionResult(
        manifest=[{ref: idb.local}],
        success=present("db.parameters", "diffIdx", "info", "split", "taxonomyDB",
                        "taxonomy/names.dmp", "taxonomy/nodes.dmp"),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=sources,
    labels=["noscratch"],
    resources=Resources(
        cpus=64,
        memory=Size.GB(256),
        duration=Duration(hours=8),
    ),
)
