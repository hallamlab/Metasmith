from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::centrifuger_db"))

PREFIX = "cfr_gtdb_r226"
# Dropbox publishes no checksums, and an error page arrives as a 200, so each part is
# held to its size. A run that names other parts passes [url, size] pairs as
# centrifuger_parts.
PARTS = [
    ("https://www.dropbox.com/scl/fi/g1c7obzhwbuoq6yngeu8z/cfr_gtdb_r226.1.cfr?rlkey=1f8b6abs46dil6m4ganrj56rx&st=5kvx0ga0&dl=1", 175626291985),
    ("https://www.dropbox.com/scl/fi/6efgktjv82cq7vd98k15t/cfr_gtdb_r226.2.cfr?rlkey=dp2t1ah0iof5uqk556propmur&st=9exqm2al&dl=1", 14931061),
    ("https://www.dropbox.com/scl/fi/ngffuycwroimz70h16dsr/cfr_gtdb_r226.3.cfr?rlkey=xlrxflxxeqz63mrboebkl4gbr&st=01wlltu1&dl=1", 2297824),
    ("https://www.dropbox.com/scl/fi/jh0ftr1lbu9yb54sik2e2/cfr_gtdb_r226.4.cfr?rlkey=nmiftf7uru5pbxtiq1lnh58v3&st=qdpe8pz3&dl=1", 72),
]


def fetch_sized(url, dest, size):
    return "\n".join([
        FetchCommand(url, dest),
        f'if [ "$(stat -c %s {dest})" != "{size}" ]; then',
        f'    echo "{dest.name} is not {size} bytes; discarding it" >&2',
        f'    rm -f {dest}',
        f'    exit 1',
        f'fi',
    ])


def protocol(context: ExecutionContext):
    iout  = context.Output(out)
    parts = context.params.get("centrifuger_parts", PARTS)
    files = [iout.local/f"{PREFIX}.{n}.cfr" for n in range(1, len(parts)+1)]

    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        *(fetch_sized(url, f, size) for (url, size), f in zip(parts, files)),
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)

    sized = all(f.is_file() and f.stat().st_size == size for (_, size), f in zip(parts, files))
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=sized and files[-1].read_text().startswith("version\t"),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(
        cpus=2,
        memory=Size.GB(2),
        duration=Duration(hours=48),
    ),
)
