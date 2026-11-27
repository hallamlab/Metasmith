from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("ref::kegg_ko_reaction"))

# KEGG REST serves the live database and keeps no releases; kegg-info.txt names the one
# that answered. The body is kept verbatim, namespace prefixes included.
URLS = {"ko2reaction.tsv": "https://rest.kegg.jp/link/reaction/ko",
        "kegg-info.txt": "https://rest.kegg.jp/info/kegg"}
# KEGG answers an outage with an empty HTTP 200; the 2026-11 body is 12,416 lines.
MIN_LINES = 5000


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    if context.LocalShell("\n".join([
        "set -euo pipefail",
        f"mkdir -p {iout.local}",
        *(FetchCommand(url, iout.local/name) for name, url in URLS.items()),
        f"grep -E '^kegg|Release' {iout.local}/kegg-info.txt || true",
    ])) != 0:
        return ExecutionResult(manifest=[], success=False)
    with open(iout.local/"ko2reaction.tsv") as fh:
        n = sum(1 for _ in fh)
    Log.Info(f"KEGG link/reaction/ko: {n:,} lines")
    return ExecutionResult(manifest=[{out: iout.local}], success=n >= MIN_LINES)


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    download_size=Size.MB(0.3),  # 248,320 B, 2026-11-26
    resources=Resources(cpus=1, memory=Size.GB(1), duration=Duration(minutes=30)),
)
