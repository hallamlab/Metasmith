from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::chebi"))

RELEASE = "rel252"
BASE_URL = f"https://ftp.ebi.ac.uk/pub/databases/chebi/archive/{RELEASE}/flat_files"

FILES = ("compounds.tsv.gz", "names.tsv.gz", "structures.tsv.gz")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        D={iout.container}/{RELEASE}
        mkdir -p $D
        for f in {" ".join(FILES)}; do
            wget -q {BASE_URL}/$f -O $D/$f
            if ! gzip -t $D/$f; then
                echo "[chebi] $f is not a valid gzip stream -- truncated transfer" >&2
                exit 1
            fi
            echo "[chebi] $f $(stat -c%s $D/$f) bytes, gzip ok"
        done
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    d = iout.local / RELEASE
    got = [f for f in FILES if (d / f).exists() and (d / f).stat().st_size > 0]
    Log.Info(f"chebi {RELEASE}: {len(got)}/{len(FILES)} files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)
