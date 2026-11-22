from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::hmmer.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db    = model.AddRequirement(lib.GetType("ref::pathofact_db"))
tox   = model.AddProduct(lib.GetType("annotation::pathofact_tox_hmm_chunk"))
vf    = model.AddProduct(lib.GetType("annotation::pathofact_vf_hmm_chunk"))

# PathoFact 2.0 runs pyhmmer's hmmsearch with T=40 for both sets (Config.yaml tox_params,
# VF_params) and writes the reported targets as --tblout. A bitscore cut does not depend
# on how the ORFs were chunked; the fixed -Z only keeps the E-values in the table from
# depending on it either.
BITSCORE = 40
SEARCH_Z = 5000


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb   = context.Input(db)
    itox, ivf = context.Output(tox), context.Output(vf)
    threads = context.params.get("cpus", 8)
    search = (f"hmmsearch --cpu {threads} -Z {SEARCH_Z} -T {BITSCORE} --noali -o /dev/null "
              "--tblout")

    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        sed '/^[^>]/s/\\*//g' {iorfs.container} > query.faa
        {search} {itox.container} {idb.container}/HMM/TOX_CDD.hmm query.faa
        {search} {ivf.container} {idb.container}/HMM/virulence_factors_CDD.hmm query.faa
    """)

    # hmmsearch writes its comment trailer even with no hits, so an empty file is a crash.
    return ExecutionResult(
        manifest=[{tox: itox.local, vf: ivf.local}],
        success=all(p.local.exists() and p.local.stat().st_size > 0 for p in (itox, ivf)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(8),
        duration=Duration(hours=8),
    ),
)
