from pathlib import Path
import shutil
import tarfile
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::antismash.env"))
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
db       = model.AddRequirement(lib.GetType("ref::antismash"))

out_json = model.AddProduct(lib.GetType("annotation::antismash_json"))
out_gbk  = model.AddProduct(lib.GetType("annotation::antismash_regions_gbk"))


MIN_CONTIG_LENGTH = 1000


def _has_long_enough_contigs(fasta_path: Path, min_len: int = MIN_CONTIG_LENGTH) -> bool:
    """Check if at least one contig meets the minimum length."""
    length = 0
    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                if length >= min_len:
                    return True
                length = 0
            else:
                length += len(line.strip())
    return length >= min_len


def _write_mock_outputs(ojson_path: Path, ogbk_path: Path, assembly_name: str):
    """Write empty/mock outputs for assemblies with no qualifying contigs."""
    import json as _json
    mock = {
        "records": [],
        "version": "mock",
        "input_file": assembly_name,
        "note": f"Skipped: all contigs < {MIN_CONTIG_LENGTH} bp",
    }
    with open(ojson_path, "w") as f:
        _json.dump(mock, f, indent=2)
    with tarfile.open(ogbk_path, "w:gz") as tar:
        pass


def protocol(context: ExecutionContext):
    iasm = context.Input(assembly)
    idb  = context.Input(db)
    ojson = context.Output(out_json)
    ogbk  = context.Output(out_gbk)

    # Pre-check: skip assemblies where all contigs are below antiSMASH minimum
    if not _has_long_enough_contigs(iasm.local):
        print(f"NOTICE: All contigs in {iasm.local.name} are < {MIN_CONTIG_LENGTH} bp. "
              f"Producing mock output (antiSMASH requires >= {MIN_CONTIG_LENGTH} bp contigs).")
        _write_mock_outputs(ojson.local, ogbk.local, iasm.local.name)
        return ExecutionResult(
            manifest=[{out_json: ojson.local, out_gbk: ogbk.local}],
            success=True,
        )

    cpus = context.params.get("cpus")
    cpus = 8 if cpus is None else cpus

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/antismash_db")],
        cmd=f"""
            antismash \
                --databases /antismash_db \
                --genefinding-tool prodigal-m \
                --cb-general \
                --cb-knownclusters \
                --cb-subclusters \
                --cpus {cpus} \
                --output-dir antismash_out \
                {iasm.container}
        """,
    )

    outdir = Path("antismash_out")

    # Collect the main JSON result (one per assembly)
    json_files = list(outdir.glob("*.json"))
    if json_files:
        shutil.copy2(json_files[0], ojson.local)

    # Tar up all region GenBank files
    gbk_files = list(outdir.glob("*.region*.gbk"))
    if gbk_files:
        with tarfile.open(ogbk.local, "w:gz") as tar:
            for gbk in gbk_files:
                tar.add(gbk, arcname=gbk.name)
    else:
        # No BGC regions found — create empty tar
        with tarfile.open(ogbk.local, "w:gz") as tar:
            pass

    return ExecutionResult(
        manifest=[
            {
                out_json: ojson.local,
                out_gbk: ogbk.local,
            },
        ],
        success=ojson.local.exists() and ogbk.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=12),
    ),
)
