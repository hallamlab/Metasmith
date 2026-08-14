from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::eggnog-mapper.env"))
src   = model.AddRequirement(lib.GetType("annotation::eggnog_source"))
out   = model.AddProduct(lib.GetType("annotation::eggnog_data"))

# Official download_eggnog_data.py uses eggnogdb.embl.de which is 404.
# Mirror at eggnog5.embl.de works. Download manually with wget.
# See: https://github.com/eggnogdb/eggnog-mapper/issues/589
BASE_URL = "http://eggnog5.embl.de/download/emapperdb-5.0.2"
DB_FILES = [
    "eggnog.db.gz",
    "eggnog.taxa.tar.gz",
    "eggnog_proteins.dmnd.gz",
]

def protocol(context: ExecutionContext):
    iout = context.Output(out)

    downloads = " && ".join(
        f"wget -q {BASE_URL}/{f} -O {iout.container}/{f}"
        for f in DB_FILES
    )
    decompress = " && ".join([
        f"gunzip -f {iout.container}/eggnog.db.gz",
        f"gunzip -f {iout.container}/eggnog_proteins.dmnd.gz",
        f"cd {iout.container} && tar xzf eggnog.taxa.tar.gz && rm -f eggnog.taxa.tar.gz",
    ])

    downloads_local = " && ".join(
        f"wget -q {BASE_URL}/{f} -O {iout.local}/{f}"
        for f in DB_FILES
    )
    decompress_local = " && ".join([
        f"gunzip -f {iout.local}/eggnog.db.gz",
        f"gunzip -f {iout.local}/eggnog_proteins.dmnd.gz",
        f"cd {iout.local} && tar xzf eggnog.taxa.tar.gz && rm -f eggnog.taxa.tar.gz",
    ])

    # Run directly on host (wget/gunzip/tar are available without container;
    # avoids apptainer spurious exit code 1 on this image)
    context.LocalShell(f"""
        mkdir -p {iout.local}
        {downloads_local}
        {decompress_local}
    """)

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=src,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
