from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
profiles = model.AddProduct(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddProduct(lib.GetType("ref::kofamscan_ko_list"))

PROFILES_URL = "ftp://ftp.genome.jp/pub/db/kofam/profiles.tar.gz"
KO_LIST_URL = "ftp://ftp.genome.jp/pub/db/kofam/ko_list.gz"

def protocol(context: ExecutionContext):
    iprofiles = context.Output(profiles)
    iko_list = context.Output(ko_list)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            wget -q {PROFILES_URL} -O profiles.tar.gz
            wget -q {KO_LIST_URL} -O ko_list.gz
            mv profiles.tar.gz {iprofiles.container}
            gunzip ko_list.gz
            mv ko_list {iko_list.container}
        """,
    )

    return ExecutionResult(
        manifest=[{profiles: iprofiles.local, ko_list: iko_list.local}],
        success=iprofiles.local.exists() and iko_list.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=4),
    ),
)
