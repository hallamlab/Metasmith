from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::ezpred"))

MODELS_RECORD = "15792215"
DATA_RECORD = "15812849"
MODELS_URL = f"https://zenodo.org/records/{MODELS_RECORD}/files/models.zip?download=1"
DATA_URL = f"https://zenodo.org/records/{DATA_RECORD}/files/Data2.zip?download=1"

FILES = (("models.zip", MODELS_URL), ("Data2.zip", DATA_URL))
MIN_BYTES = {"models.zip": 100_000_000, "Data2.zip": 1_000_000}

STAGE = "_incoming"


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    release = f"{MODELS_RECORD}+{DATA_RECORD}"

    checks = "\n".join(
        f'''        n=$(stat -c%s $D/{name})
        [ "$n" -ge {MIN_BYTES[name]} ] || {{ echo "[ezpred] {name} is $n bytes, under the {MIN_BYTES[name]} floor -- that is an error page, not the archive" >&2; exit 1; }}
        unzip -tq $D/{name} >/dev/null || {{ echo "[ezpred] {name} is not a readable zip" >&2; exit 1; }}'''
        for name, _ in FILES)
    fetches = "\n".join(
        f'        wget -q -c "{url}" -O $S/{name}' for name, url in FILES)

    _cmd = f"""
        set -e
        S={iout.container}/{STAGE}
        D={iout.container}/{release}
        mkdir -p $S
{fetches}
        mkdir -p $D
        mv $S/* $D/
        rmdir $S
{checks}
        # Zenodo publishes md5 per file in its record metadata; recorded here so the
        # transfer stays re-checkable offline, by anyone, without trusting this step.
        ( cd $D && md5sum *.zip > MD5SUMS )
        echo "[ezpred] release {release}"
        ls -l $D
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    d = iout.local / release
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=all((d / name).exists() for name, _ in FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(4), duration=Duration(hours=2)),
)
