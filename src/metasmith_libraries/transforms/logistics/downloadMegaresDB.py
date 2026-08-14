from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::diamond.env"))
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
db      = model.AddProduct(lib.GetType("annotation::megares_diamond_db"))

# MEGARes v3.00 ships a *nucleotide* CDS FASTA (ARG reference genes). megares.py
# aligns with `diamond blastp`, so the DB must be protein — the CDS must be
# translated (frame 1, standard table) before `diamond makedb`. tcdb/bacmet/vfdb
# skip this only because their sources are already protein. meglab.org's TLS
# chain trips wget verification, hence --no-check-certificate.
MEGARES_URL = "https://www.meglab.org/downloads/megares_v3.00/megares_database_v3.00.fasta"


def protocol(context: ExecutionContext):
    idb = context.Output(db)

    # 1) fetch the nucleotide CDS FASTA (diamond image has wget, per tcdb).
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"wget -q --no-check-certificate {MEGARES_URL} -O megares.fasta",
    )

    # 2) translate CDS -> protein. --clean maps internal '*' stops to 'X' and
    #    --trim strips trailing X/* so `diamond makedb` sees no '*' (which it
    #    rejects). No --append-frame: the MEGARes accession in each header must
    #    stay intact for downstream hit -> ARG-annotation mapping.
    context.ExecWithEnv().ifContainerDo(
        env=img_sqk,
        cmd="seqkit translate --frame 1 --transl-table 1 --clean --trim "
            "megares.fasta -o megares_prot.fasta",
    )

    # 3) build the protein DIAMOND DB. megares.py mounts the .dmnd's parent at
    #    /db and references by name (tcdb pattern), so the product is a single
    #    `.dmnd` file (ext: dmnd).
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            diamond makedb --in megares_prot.fasta -d megares
            mv megares.dmnd {idb.container}
        """,
    )

    return ExecutionResult(
        manifest=[{db: idb.local}],
        success=idb.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
