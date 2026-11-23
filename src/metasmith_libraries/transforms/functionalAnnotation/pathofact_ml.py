import re

from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::pathofact_models.env"))
orfs  = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
db    = model.AddRequirement(lib.GetType("ref::pathofact_db"))
tox   = model.AddProduct(lib.GetType("annotation::pathofact_tox_ml_chunk"))
vf    = model.AddProduct(lib.GetType("annotation::pathofact_vf_ml_chunk"))

NOT_RESIDUE = re.compile(r"[^ACDEFGHIKLMNPQRSTVWY]")


# PathoFact's proteins come from pyrodigal-gv with --no-stop-codon, so neither model has
# seen a `*`; Prodigal ends each complete gene with one. The VF encoder also maps any other
# letter to `-` and then indexes its dipeptide table with it, a KeyError on the first X
# (an N codon), so the VF model is given the residues it can encode and nothing else.
def _write_inputs(src, tox_path, vf_path):
    with open(src) as fin, open(tox_path, "w") as ftox, open(vf_path, "w") as fvf:
        for line in fin:
            if line.startswith(">"):
                ftox.write(line)
                fvf.write(line)
            else:
                seq = line.strip().upper().replace("*", "")
                ftox.write(seq + "\n")
                fvf.write(NOT_RESIDUE.sub("", seq) + "\n")


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    idb   = context.Input(db)
    itox, ivf = context.Output(tox), context.Output(vf)
    threads = context.params.get("cpus", 4)
    _write_inputs(iorfs.local, "tox.faa", "vf.faa")

    # The two commands of PathoFact's rules ml_tox and ml_vf, against its own scripts.
    context.ExecWithEnv(env=image, cmd=f"""
        set -euo pipefail
        python {idb.container}/src/predict.py -s tox.faa -k 5 \\
            -m {idb.container}/Models/TOX/final_model.joblib -v {idb.container}/Models/TOX \\
            --cpus {threads} -o {itox.container}
        python {idb.container}/src/vf_prediction2.py --file vf.faa \\
            --model {idb.container}/Models/VF/final_model.joblib \\
            --cpus {threads} --outfile {ivf.container}
    """)

    return ExecutionResult(
        manifest=[{tox: itox.local, vf: ivf.local}],
        success=all(p.local.exists() and p.local.stat().st_size > 0 for p in (itox, ivf)),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=2),
    ),
)
