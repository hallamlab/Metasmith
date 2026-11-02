from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::metanetx"))

VERSION = "4.5"
BASE_URL = f"https://www.metanetx.org/ftp/{VERSION}"

FILES = ("chem_prop.tsv", "chem_xref.tsv", "reac_prop.tsv", "reac_xref.tsv")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

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
    context.ExecWithEnv(env=image, cmd=_cmd)

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
