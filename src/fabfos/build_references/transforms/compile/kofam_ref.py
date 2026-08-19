from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
source   = model.AddRequirement(lib.GetType("fabfos_data::kofam"))
profiles = model.AddProduct(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddProduct(lib.GetType("ref::kofamscan_ko_list"))

ARCHIVE     = "profiles.tar.gz"
KO_LIST_GZ  = "ko_list.gz"


def protocol(context: ExecutionContext):
    isrc  = context.Input(source)
    iprof = context.Output(profiles)
    iko   = context.Output(ko_list)

    _cmd = f"""
        set -e
        N=$(find {isrc.container} -mindepth 1 -maxdepth 1 -type d | wc -l)
        if [ "$N" -ne 1 ]; then
            echo "[kofam] expected exactly one release under {isrc.container}, found $N." \\
                 'The profiles and the ko_list must come from the same build, so which' \\
                 'release to compile is a decision, not a default' >&2
            exit 1
        fi
        REL=$(find {isrc.container} -mindepth 1 -maxdepth 1 -type d)
        echo "[kofam] compiling $(basename $REL)"

        mkdir -p {iprof.container}
        tar -xzf $REL/{ARCHIVE} -C {iprof.container} --strip-components=1
        gunzip -c $REL/{KO_LIST_GZ} > {iko.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    n_hmm = len(list(iprof.local.glob("*.hmm"))) if iprof.local.is_dir() else 0
    n_ko = 0
    if iko.local.exists():
        with open(iko.local, errors="replace") as fh:
            n_ko = sum(1 for _ in fh) - 1
    Log.Info(f"unpacked {n_hmm:,} HMM profiles, {n_ko:,} scoring thresholds")
    if n_hmm and n_ko and abs(n_hmm - n_ko) > 0.05 * n_hmm:
        Log.Warn(f"{n_hmm:,} profiles against {n_ko:,} thresholds -- more than 5% apart, "
                 f"which is what a mismatched profiles/ko_list pair looks like")
    return ExecutionResult(
        manifest=[{profiles: iprof.local, ko_list: iko.local}],
        success=n_hmm > 0 and n_ko > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=1)),
)
