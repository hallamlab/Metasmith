from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::bench_laser"))

URL    = "https://bitbucket.org/jdwinkler/laser_release.git"
COMMIT = "f6ce080a8993ee259c4914ce92f83b1f966bab2d"

EXPECTED = ("README.md", "database_store", "inputs", "metabolic_models")


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    _cmd = f"""
        set -e
        rm -rf {iout.container}
        git clone -q {URL} {iout.container}
        cd {iout.container}
        git checkout -q {COMMIT}
        HEAD_SHA=$(git rev-parse HEAD)
        if [ "$HEAD_SHA" != "{COMMIT}" ]; then
            echo "[laser] checked out $HEAD_SHA, wanted {COMMIT}" >&2
            exit 1
        fi
        # Recorded BEFORE the .git goes, so the folder still states its own version.
        printf '%s\\n%s\\n' "{URL}" "$HEAD_SHA" > .upstream
        rm -rf .git
        echo "[laser] {COMMIT} checked out; .git removed (this tree is annexed downstream)"
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    missing = [e for e in EXPECTED if not (iout.local / e).exists()]
    if missing:
        Log.Error(f"laser: checkout is missing {missing}")
    Log.Info(f"laser: {sum(1 for _ in iout.local.rglob('*') if _.is_file()):,} files")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=not missing and (iout.local / ".upstream").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    labels=["local"],
    resources=Resources(cpus=1, memory=Size.GB(2), duration=Duration(hours=1)),
)
