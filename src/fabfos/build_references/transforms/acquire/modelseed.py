from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::modelseed"))

COMMIT = "4d3395fe71538fe271c34a38638fa775edaedcd8"
BASE_URL = f"https://raw.githubusercontent.com/ModelSEED/ModelSEEDDatabase/{COMMIT}"

FILES = ("compounds.tsv",)


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        D={iout.container}/{COMMIT}
        mkdir -p $D
        for f in {" ".join(FILES)}; do
            wget -q {BASE_URL}/Biochemistry/$f -O $D/$f
            n=$(wc -l < $D/$f)
            if [ "$n" -lt 2 ]; then
                echo "[modelseed] $f has $n lines -- truncated transfer" >&2
                exit 1
            fi
            echo "[modelseed] $f $n lines, $(stat -c%s $D/$f) bytes"
        done
        echo "{COMMIT}" > $D/COMMIT
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    d = iout.local / COMMIT
    got = [f for f in FILES if (d / f).exists() and (d / f).stat().st_size > 0]
    Log.Info(f"modelseed {COMMIT[:12]}: {len(got)}/{len(FILES)} files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=len(got) == len(FILES),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(minutes=30)),
)
