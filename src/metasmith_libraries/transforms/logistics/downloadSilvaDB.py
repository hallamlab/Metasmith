from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
db      = model.AddProduct(lib.GetType("amplicon::silva_db"))

SILVA_URL = "https://www.arb-silva.de/fileadmin/silva_databases/release_138.2/Exports/SILVA_138.2_SSURef_NR99_tax_silva.fasta.gz"

def protocol(context: ExecutionContext):
    idb = context.Output(db)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            mkdir -p {idb.container}
            python3 -c "
            import urllib.request, sys
            print('Downloading SILVA SSURef NR99 database...')
            urllib.request.urlretrieve('{SILVA_URL}', 'silva.fasta.gz')
            print('Done.')
            "
            gunzip -c silva.fasta.gz > {idb.container}/silva_ssu_nr99.fasta
        """,
    )

    return ExecutionResult(
        manifest=[
            {
                db: idb.local,
            },
        ],
        success=(idb.local / "silva_ssu_nr99.fasta").exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    )
)
