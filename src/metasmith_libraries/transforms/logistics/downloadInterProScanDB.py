from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
img_ipr = model.AddRequirement(lib.GetType("env::interproscan.env"))
data    = model.AddProduct(lib.GetType("ref::interproscan_data"))

IPRSCAN_DATA_URL = "https://ftp.ebi.ac.uk/pub/databases/interpro/iprscan/5/5.67-99.0/interproscan-5.67-99.0-64-bit.tar.gz"

def protocol(context: ExecutionContext):
    idata = context.Output(data)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q {IPRSCAN_DATA_URL} -O interproscan-data.tar.gz
            mkdir -p ipr_data
            tar xzf interproscan-data.tar.gz -C ipr_data --strip-components=1
        """,
    )

    # setup.py and interproscan.properties are the IMAGE's copies at
    # /opt/interproscan, and the container's working directory is the task's /ws,
    # so the indexing has to run from there. The downloaded `data/` is bound over
    # the image's missing one, so what runs is the installed tool indexing the
    # models we just fetched.
    #
    # In a subshell, because the exit trap the command is wrapped in writes its
    # exitcode marker to a relative path: a bare `cd` leaves the trap firing in
    # /opt/interproscan, which is not writable by the task's uid, and the step
    # fails on the marker write after setup.py has already succeeded.
    context.ExecWithEnv().ifContainerDo(
        env=img_ipr,
        binds=[(context.external_cwd/"ipr_data/data", "/opt/interproscan/data")],
        cmd=f"""\
            (cd /opt/interproscan && python3 setup.py -f interproscan.properties --force)
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
