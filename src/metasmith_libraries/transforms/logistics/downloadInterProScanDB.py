from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
img_ipr = model.AddRequirement(lib.GetType("env::interproscan.env"))
data    = model.AddProduct(lib.GetType("ref::interproscan_data"))

# InterProScan 5.67-99.0 data bundle
IPRSCAN_DATA_URL = "https://ftp.ebi.ac.uk/pub/databases/interpro/iprscan/5/5.67-99.0/interproscan-5.67-99.0-64-bit.tar.gz"

def protocol(context: ExecutionContext):
    idata = context.Output(data)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q {IPRSCAN_DATA_URL} -O interproscan-data.tar.gz
            mkdir -p {idata.container}
            tar xzf interproscan-data.tar.gz -C ipr_data --strip-components=1
        """,
    )

    # compile hmmer indexes
    context.ExecWithEnv().ifContainerDo(
        env=img_ipr,
        binds=[(context.external_cwd/"ipr_data/data", "/opt/interproscan/data")],
        cmd=f"""\
            python3 setup.py -f interproscan.properties --force
        """
    )

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-p {threads}"
    context.LocalShell(f"mv ipr_data/data ./ && tar -I 'pigz {threads}' -cf {idata.local} ./data")

    return ExecutionResult(
        manifest=[{data: idata.local}],
        success=idata.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=img_ipr,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
