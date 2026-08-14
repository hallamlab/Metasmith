import shutil
from pathlib import Path

from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::bakta.env"))
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
db       = model.AddRequirement(lib.GetType("annotation::bakta_db"))
out_gff  = model.AddProduct(lib.GetType("annotation::bakta_gff"))
out_tsv  = model.AddProduct(lib.GetType("annotation::bakta_tsv"))

HELPER = Path(__file__).parent / "_piler_cr_to_gff3.py"


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb  = context.Input(db)
    ogff = context.Output(out_gff)
    otsv = context.Output(out_tsv)

    # Copy PILER-CR→GFF3 helper into working directory (mounted at /ws/ in container)
    shutil.copy(HELPER, Path.cwd() / "_piler_cr_to_gff3.py")

    # Step 1: bakta non-coding annotation (always succeeds)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/db")],
        cmd=f"""
            bakta \
                --db /db/db-light \
                --skip-cds --skip-sorf --skip-pseudo --skip-plot --skip-crispr \
                --force \
                --prefix bakta_noncoding \
                --output bakta_out \
                {iasm.container}
        """,
    )

    # Step 2: PILER-CR CRISPR detection (isolated, best-effort)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            pilercr -in {iasm.container} -out crispr_raw.txt -noinfo -quiet || true

            if [ -s crispr_raw.txt ]; then
                python3 /ws/_piler_cr_to_gff3.py crispr_raw.txt bakta_out/bakta_noncoding.gff3
            fi
        """,
    )

    context.LocalShell(f"cp bakta_out/bakta_noncoding.gff3 {ogff.local}")
    context.LocalShell(f"cp bakta_out/bakta_noncoding.tsv {otsv.local}")

    return ExecutionResult(
        manifest=[
            {
                out_gff: ogff.local,
                out_tsv: otsv.local,
            },
        ],
        success=ogff.local.exists() and otsv.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=4,
        memory=Size.GB(32),
        duration=Duration(hours=8),
    ),
)
