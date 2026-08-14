"""strainphlan — per-sample consensus markers for StrainPhlAn.

First (per-sample) step of StrainPhlAn: reconstruct consensus marker sequences
from a MetaPhlAn read-vs-marker alignment. The upstream `taxonomy::metaphlan_sam`
product is NOT a real SAM — it is MetaPhlAn's 2-column `--bowtie2out` map
(read_id -> marker), which lacks the POS/CIGAR/SEQ records sample2markers.py
needs. So we regenerate a true SAM here by re-mapping the sample's clean reads to
the same SGB marker DB (mpa_vJan25_CHOCOPhlAnSGB_202503) with `metaphlan -s`, then
run sample2markers.py on that SAM.

The cross-sample `strainphlan` tree-building step is an aggregate run downstream
and is gated on relevant pathogens appearing (conditional per Antonio's table);
it keys on the per-sample marker pkl produced here, whose stem carries the
sample id.
"""
import glob
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::metaphlan.env"))
reads = model.AddRequirement(lib.GetType("sequences::clean_short_reads"))
db = model.AddRequirement(lib.GetType("annotation::metaphlan_db"))
out_markers = model.AddProduct(lib.GetType("annotation::strainphlan_consensus_markers"))

# The MetaPhlAn SGB db that produced the metaphlan_sam upstream (profile header:
# mpa_vJan25_CHOCOPhlAnSGB_202503). metaphlan reads the index base name from
# /mpa_db/mpa_latest; sample2markers.py -d wants the matching .pkl.
MPA_INDEX = "mpa_vJan25_CHOCOPhlAnSGB_202503"
MPA_PKL = f"{MPA_INDEX}.pkl"


def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    idb = context.Input(db)
    iout = context.Output(out_markers)

    threads = context.params.get("cpus", 8)

    # Re-map reads -> SGB markers to get a real SAM, then reconstruct consensus
    # markers. HOME is forced into the (writable) work dir: the compute node
    # binds /home read-only, but metaphlan/bowtie2/sample2markers scribble under
    # $HOME. The SAM (and thus the emitted <stem>.pkl) is named for the sample so
    # the downstream cross-sample strainphlan step keys on it. --offline: never
    # touch the network; the SGB index is already staged under /mpa_db.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[(idb.external, "/mpa_db")],
        cmd=f"""
            export HOME="$(pwd)/home"; mkdir -p "$HOME" markers work
            S=$(basename {ireads.container}); S=${{S%.gz}}; S=${{S%.fq}}; S=${{S%.fastq}}
            metaphlan {ireads.container} \
                --input_type fastq --offline \
                --db_dir /mpa_db --index {MPA_INDEX} \
                --nproc {threads} \
                --mapout work/$S.mapout \
                -s work/$S.sam.bz2 \
                -o work/$S.profile.tsv
            sample2markers.py -i work/$S.sam.bz2 -f bz2 \
                -d /mpa_db/{MPA_PKL} -o markers -n {threads}
        """,
    )

    # MetaPhlAn 4.2.4 sample2markers.py emits consensus markers as
    # `<stem>.json.bz2` (older releases used `.pkl`); match both. An empty
    # markers dir is a genuine failure, so we DON'T touch a placeholder and
    # report success=False — otherwise errorStrategy 'ignore' would silently
    # bank an empty marker file.
    hits = sorted(glob.glob("markers/*.json.bz2") + glob.glob("markers/*.pkl"))
    if hits:
        context.LocalShell(f"cp {hits[0]} {iout.local}")

    return ExecutionResult(
        manifest=[{out_markers: iout.local}],
        success=bool(hits) and iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(48),
        duration=Duration(hours=3),
    ),
)
