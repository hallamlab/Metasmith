"""R3 -- unpack the KOfam distribution into the two things kofamscan is handed.

Takes the whole `fabfos_data::kofam` source folder and produces a profile DIRECTORY
plus a plain-text KO list. This is the only step that opens either archive: the
acquisition tier holds both exactly as served, so a new kofamscan or a new directory
layout re-runs this without re-fetching 1.5 GB.

BOTH ARCHIVES ARE OPENED HERE, WHICH IS THE CHANGE. `acquire/kofam.py` used to gunzip
`ko_list.gz` on the way in, so the acquisition tier held a file no upstream URL would
ever return. It now keeps the gzip, which means this transform must decompress it --
the previous `cp` would have copied a gzip stream onto a product typed as a TSV and
kofamscan would have read the compressed bytes as its scoring table.

WHAT ko_list ACTUALLY IS, because the name misleads: not a KEGG KO registry but the
per-profile scoring table, generated when the profiles are built and released with
them. Pairing a ko_list with profiles from a different release applies the wrong
threshold to every hit, silently -- which is why both come from ONE release directory
and why finding two is a refusal rather than a choice.
"""
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

    # The tarball unpacks to a `profiles/` directory; --strip-components=1 puts the .hmm
    # files directly under the product, because kofamscan is handed a profile DIRECTORY
    # and a nested extra level makes it find nothing while raising nothing.
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
    # A profile with no threshold row scores against nothing, so the two counts being in
    # the same ballpark is the cheap check that these came from one release. It is a NOTE
    # rather than a refusal because KOfam has always shipped a few more of one than the
    # other and a hard equality would fail on a healthy download.
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
    # NOT labels=["local"]. That label is right for `acquire/` -- a download needs the
    # login node's network -- and copying it here is what pinned every compile to the
    # login node under the slurm preset: `xlocalx` sets `executor = 'local'`, whose pool
    # slurm.nf declares as 8 cores / 8 GB, and Nextflow's local executor REFUSES a
    # process asking for more rather than queueing it. It also sets
    # errorStrategy='ignore' with no retry, so the refusal is silent and the workflow
    # goes green with the reference absent. Nothing in this transform touches the
    # network; it belongs on a compute node.
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=1)),
)
