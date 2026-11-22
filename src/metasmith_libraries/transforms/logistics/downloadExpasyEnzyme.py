from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::diamond.env"))
out   = model.AddProduct(lib.GetType("ref::expasy_enzyme"))

BASE = "https://ftp.expasy.org/databases/enzyme"


# ExPASy keeps no past releases, so this is the current ENZYME; Expasy_reldate.txt names it,
# as MetaPathways 4's build_db records it.
def protocol(context: ExecutionContext):
    iout = context.Output(out)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        FetchCommand([f"{BASE}/enzyme.dat"], iout.local/"enzyme.dat", label="enzyme.dat"),
        FetchCommand([f"{BASE}/enzclass.txt"], iout.local/"enzclass.txt", label="enzclass.txt"),
        f"(date -u; grep '^Release:' {iout.local}/enzclass.txt) > {iout.local}/Expasy_reldate.txt",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)
    return ExecutionResult(manifest=[{out: iout.local}],
                           success=(iout.local/"enzyme.dat").is_file())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(hours=1)),
)
