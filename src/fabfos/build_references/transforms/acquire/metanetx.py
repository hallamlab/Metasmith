"""MetaNetX/MNXref -- the identity space everything else is expressed in.

Four TSVs and their upstream .md5 sidecars, into `metanetx/<version>/`.

THE VERSION DIRECTORY IS PART OF THE PRODUCT. MetaNetX serves one tree per release
and the release IS the identity of these files: `chem_prop.tsv` on its own says
nothing about which MNXM space it belongs to, and two builds a year apart would put
incompatible id spaces at the same path with nothing to tell them apart.

THE SIDECARS ARE FETCHED, NOT COMPUTED. MetaNetX publishes an .md5 next to every
file, so this transform can verify what it received against what upstream says it
served -- which is a stronger claim than Content-Length, and the only one that
catches a truncated or mid-flight-corrupted 800 MB transfer. Keeping the sidecar in
the product means the check is re-runnable offline later, by anyone, without
trusting this transform's word for it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::metanetx"))

# Pinned. MetaNetX keeps old releases served at their own paths, so this is a real
# pin rather than a label on whatever is current -- unlike Rhea and KOfam below,
# where the top-level path rolls forward under you.
VERSION = "4.5"
BASE_URL = f"https://www.metanetx.org/ftp/{VERSION}"

FILES = ("chem_prop.tsv", "chem_xref.tsv", "reac_prop.tsv", "reac_xref.tsv")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # `cut` rather than awk throughout: awk's braces would need doubling inside the
    # f-string, and a mis-escaped brace here fails at run time in a shell, not at
    # import in python.
    _cmd = f"""
        set -e
        D={iout.container}/{VERSION}
        mkdir -p $D
        for f in {" ".join(FILES)}; do
            wget -q {BASE_URL}/$f -O $D/$f
            wget -q {BASE_URL}/$f.md5 -O $D/$f.md5
            want=$(cut -d' ' -f1 < $D/$f.md5)
            have=$(md5sum $D/$f | cut -d' ' -f1)
            if [ "$want" != "$have" ]; then
                echo "[metanetx] MD5 MISMATCH for $f: upstream says $want, got $have" >&2
                exit 1
            fi
            echo "[metanetx] $f verified $have"
        done
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    d = iout.local / VERSION
    got = [f for f in FILES if (d / f).exists() and (d / f).stat().st_size > 0]
    Log.Info(f"metanetx {VERSION}: {len(got)}/{len(FILES)} files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(4), duration=Duration(hours=2)),
)
