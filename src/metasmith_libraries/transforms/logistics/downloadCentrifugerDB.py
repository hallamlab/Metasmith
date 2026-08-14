# REVIEW: Mirrors what `centrifuger-download cfr_gtdb_r226` does — pulls 4
# split .cfr files from the Liu lab's Dropbox. Centrifuger has no
# `--install` flag, and there is no Zenodo mirror of the GTDB-r226 index.
# The four .cfr files (.1.cfr .. .4.cfr) share the "cfr_gtdb_r226" prefix
# per the ref::centrifuger_db dtype declaration; classifier `-x` arg uses
# the same prefix (no trailing index number / suffix).
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::centrifuger_db"))

CENTRIFUGER_DB_URLS = [
    (".1.cfr", "https://www.dropbox.com/scl/fi/g1c7obzhwbuoq6yngeu8z/cfr_gtdb_r226.1.cfr?rlkey=1f8b6abs46dil6m4ganrj56rx&st=5kvx0ga0&dl=1"),
    (".2.cfr", "https://www.dropbox.com/scl/fi/6efgktjv82cq7vd98k15t/cfr_gtdb_r226.2.cfr?rlkey=dp2t1ah0iof5uqk556propmur&st=9exqm2al&dl=1"),
    (".3.cfr", "https://www.dropbox.com/scl/fi/ngffuycwroimz70h16dsr/cfr_gtdb_r226.3.cfr?rlkey=xlrxflxxeqz63mrboebkl4gbr&st=01wlltu1&dl=1"),
    (".4.cfr", "https://www.dropbox.com/scl/fi/jh0ftr1lbu9yb54sik2e2/cfr_gtdb_r226.4.cfr?rlkey=nmiftf7uru5pbxtiq1lnh58v3&st=qdpe8pz3&dl=1"),
]


def protocol(context: ExecutionContext):
    # ref::centrifuger_db is a PREFIX (not a dir): files written as
    # <prefix>.1.cfr .. <prefix>.4.cfr so the classifier can use `-x <prefix>`.
    iout = context.Output(out)
    wget_lines = "\n            ".join(
        f'wget -q "{url}" -O {iout.container}{suffix}'
        for suffix, url in CENTRIFUGER_DB_URLS
    )
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            mkdir -p $(dirname {iout.container})
            {wget_lines}
        """,
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(
        cpus=1,
        memory=Size.GB(8),
        duration=Duration(hours=12),
    ),
)
