"""The ModelSEED biochemistry compound table -- the second external name supplier.

ModelSEED's value here is not its structures, which largely restate ChEBI's. It is its
ALIASES column: ModelSEED reconciled a dozen model-organism namespaces into one table,
so a MetaNetX stub named the way a genome-scale model names it resolves here when it
does not resolve in ChEBI. The two suppliers are kept separate rather than merged
upstream because the curation sweep attributes what it recovered to the lane that
recovered it, and a merged vocabulary erases that.

PINNED BY COMMIT, WHICH IS STRONGER THAN A RELEASE NUMBER. ModelSEED publishes no
releases; the file is served off a branch, so a URL naming the branch is a URL that
means something different next month. The commit sha names the exact tree and always
will. This particular file has not changed since 2020-08-19, which makes the pin cheap
and makes a future change loudly visible rather than silent.

ONE FILE, NOT A CHECKOUT. Cloning the repository would pull ~2 GB of model templates
and reaction sets nothing here reads, into the acquisition tier, where a folder's
contract is fidelity to the source rather than convenience.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
out   = model.AddProduct(lib.GetType("fabfos_data::modelseed"))

# The commit that last touched Biochemistry/compounds.tsv (2020-08-19).
COMMIT = "4d3395fe71538fe271c34a38638fa775edaedcd8"
BASE_URL = f"https://raw.githubusercontent.com/ModelSEED/ModelSEEDDatabase/{COMMIT}"

FILES = ("compounds.tsv",)


def protocol(context: ExecutionContext):
    iout = context.Output(out)

    # raw.githubusercontent serves no checksum, so the pin IS the verification: the URL
    # names an immutable tree, and the only failure left is a truncated transfer. A
    # header-plus-one-row floor catches that without pretending to a content check.
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
