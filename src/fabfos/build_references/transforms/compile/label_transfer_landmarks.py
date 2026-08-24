# R7 -- the labelled landmarks the kNN transfer lane votes against.
#
# ONE PARQUET, ONE ROW PER ACCESSION: the accession, its MNXR labels and its 512-float
# embedding side by side. Nothing addresses anything by row, which is the point. This
# artifact used to be an index parquet beside an .npy stack, and it shipped scrambled --
# see THE CHUNKS DO NOT SORT below.
#
#
# Swiss-Prot sequences, embedded with ProteinBERT, labelled with MetaNetX reaction ids
# mapped through Rhea. Those three clauses are one sentence and each is load-bearing:
#
#   SWISS-PROT      the sequences. `fabfos_data::swissprot`, the reviewed half of
#                   UniProtKB, ~93 MB.
#   PROTEINBERT     the embedder, from the same pinned image the run-side lane uses.
#   MNXR VIA RHEA   the labels. `ref::mnxr_lookup` rows with `id_source == "uniprot"`,
#                   which is exactly the rhea2uniprot route -- UniProt accession -> Rhea
#                   reaction -> MNXR through MetaNetX's `rhea:` xrefs.
#
# SWISS-PROT REPLACES UNIREF50 AS THE SEQUENCE SOURCE, and the reason is that the cut
# and the sequences now describe the same set. The pool is defined by the bridge's
# `reviewed` rows, and `reviewed` means the accession came from `rhea2uniprot.tsv`
# rather than `rhea2uniprot_trembl.tsv.gz` -- i.e. it means Swiss-Prot, exactly. Taking
# the sequences from UniRef50 instead meant an accession only had a sequence if it
# happened to be its cluster's REPRESENTATIVE: UniRef50 clusters at 50% identity, so an
# entry sitting under another entry's representative dropped out of the pool silently.
# That was counted rather than substituted for, but it was a coverage loss with no
# upside once ~93 MB of exactly the right sequences is already a source folder in the
# graph. It also drops an 8.8 GB gzip stream out of this transform's inputs.
#
# WHAT THIS IS NOT. The deployed pool is KEGG-derived -- 54,005 sequences keyed on KEGG
# gene ids (`dme:Dmel_CG3481`), labelled by KO, projecting KO -> MNXR. Building from the
# Rhea route means one label source instead of two (so a protein cannot be labelled one
# way here and a different way in the GPR mapper) and no KEGG-licensed sequences in the
# tree -- but a DIFFERENT set, so the pbert lane's numbers move. This is not a
# reproduction of the deployed lane and must not be reported as one.
#
# Two traps worth stating because both are silent:
#   * SAME MODEL. A pool embedded with a different model from the query is not a
#     weaker pool, it is a meaningless one -- cosine distance between two embedding
#     spaces is a number with no referent. Enforced by sharing `env::proteinbert.env`
#     with functionalAnnotation/proteinbert.py, whose flags are copied verbatim below.
#   * THE CHUNKS DO NOT SORT INTO THE ORDER THEY WERE WRITTEN. `pbert` writes fixed
#     1,024-sequence chunks in FASTA order, named `<stem>.1`, `<stem>.2`, ... with no
#     zero padding, so `sorted(glob("*.npy"))` gives `.1, .10, .11, ... .2, .20, ...`
#     while its index stays in FASTA order. The shipped artifact was assembled from
#     those two orders as though they agreed: every one of its 222,019 references
#     carried another protein's reactions, the length check passed, the label merge
#     passed, and the lane emitted a full, confident, wrong table. The chunks are
#     stacked by their integer suffix here, and the result is checked against the FASTA
#     this transform wrote before a label is attached to it.
#
# THE WEIGHTS ARE FREE. ProteinBERT's are baked into the pinned image, so this
# transform acquires no model and the pool is the only artifact it produces. That is
# also why it runs under a container runtime: `proteinbert.env` carries no `conda:`
# key, so there is no MAMBA path for it.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::proteinbert.env"))
source = model.AddRequirement(lib.GetType("fabfos_data::swissprot"))
bridge = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
pool   = model.AddProduct(lib.GetType("ref::label_transfer_landmarks"))

POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"

FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"

TABLE_NAME = "landmarks.parquet"
SOURCE_NAME = "source.txt"

SELECT = r'''
import gzip, os
from pathlib import Path
import pandas as pd

BRIDGE = "{bridge}"
SP_ROOT = "{swissprot}"
FASTA_OUT = "_pool.faa"
LABELS_OUT = "_pool_labels.parquet"
SOURCE_OUT = "_pool_source.txt"

subs = sorted(p for p in Path(SP_ROOT).iterdir() if p.is_dir())
if len(subs) != 1:
    raise SystemExit(f"[pool] expected exactly one Swiss-Prot release under {{SP_ROOT}}, "
                     f"found {{len(subs)}} ({{[p.name for p in subs]}}) -- which release the "
                     f"pool was built from is not recoverable from the embeddings")
SP = subs[0]
FASTA = str(SP / "{fasta_file}")
print(f"[pool] Swiss-Prot release {{SP.name}}", flush=True)

b = pd.read_parquet(BRIDGE, columns=["id", "id_source", "mnxr", "evidence_quality"])
sel = b[(b["id_source"] == "{id_source}") & (b["evidence_quality"] == "{evidence}")]
labels = (sel.groupby("id")["mnxr"].apply(lambda s: ";".join(sorted(set(s))))
             .rename("mnxr_list").reset_index().rename(columns={{"id": "accession"}}))
print(f"[pool] bridge slice: {{len(sel):,}} rows -> {{len(labels):,}} labelled accessions",
      flush=True)
if labels.empty:
    raise SystemExit("[pool] the bridge carries no reviewed uniprot rows -- the cut that "
                     "defines the pool selected nothing")

wanted = dict(zip(labels["accession"], labels["mnxr_list"]))

# THE ALPHABET IS NARROWED TO WHAT THE EMBEDDER CAN TOKENISE, and this is not
# cosmetic. ProteinBERT's `aa_to_token_index` covers exactly ACDEFGHIKLMNPQRSTUVWXY;
# the image's encoder builds a lookup array sized to the largest of those ordinals
# ('Y', 89) and guards it with `if c > len(arrayed_map)` -- an off-by-one, so a
# residue whose ordinal is exactly 90 falls into the else branch and indexes past
# the end. 'Z' (Glx) is ordinal 90, Swiss-Prot uses it, and the whole run dies with
# `IndexError: getitem out of range` after the model has loaded. Measured on 2,000
# reviewed sequences: one of them carried a 'Z' and that was enough.
#
# So every residue outside the tokenisable set becomes 'X' -- which is what the
# encoder does with 'B' and '*' anyway (they map to the OTHER token), just done here
# where it cannot crash. Lowercase is upper-cased for the same reason: 'a' is
# ordinal 97 and would trip the identical bug.
POOL_ALPHABET = set("ACDEFGHIKLMNPQRSTUVWXY")


def tokenisable(seq):
    return "".join(c if c in POOL_ALPHABET else "X" for c in seq.upper())


# Swiss-Prot headers are `>sp|P12345|NAME_ORGANISM Description OS=...`, so the accession
# is the second pipe-delimited field. The bridge's uniprot ids come from rhea2uniprot,
# which writes bare accessions, so the two join directly.
written = 0
recoded = 0
seen = set()
keep = False
with gzip.open(FASTA, "rt") as fh, open(FASTA_OUT, "w") as out:
    for line in fh:
        if line.startswith(">"):
            parts = line[1:].split("|")
            acc = parts[1] if len(parts) >= 3 else line[1:].split(None, 1)[0]
            keep = acc in wanted and acc not in seen
            if keep:
                seen.add(acc)
                # Header is the bare accession: the embedder echoes it into its index,
                # and that is the key the labels are re-joined on.
                out.write(">" + acc + "\n")
                written += 1
        elif keep:
            s = line.strip()
            t = tokenisable(s)
            if t != s:
                recoded += 1
            out.write(t + "\n")
print(f"[pool] {{recoded:,}} sequence lines carried a residue outside the embedder's "
      f"alphabet and were recoded to X", flush=True)

missing = len(wanted) - written
print(f"[pool] {{written:,}} of {{len(wanted):,}} labelled accessions have a Swiss-Prot "
      f"sequence; {{missing:,}} do not", flush=True)
if written == 0:
    raise SystemExit("[pool] no pool sequences found -- the accession join broke. The "
                     "bridge's uniprot ids and Swiss-Prot's `sp|ACC|` field are the same "
                     "id space, so zero overlap is a parse bug, not a coverage fact")
# The cut IS Swiss-Prot: `reviewed` means the accession came from rhea2uniprot.tsv rather
# than the trembl file. So a labelled accession with no sequence here is an accession Rhea
# still lists that this Swiss-Prot release has demerged or deleted -- a handful, and a
# real finding about release skew between Rhea and UniProt if it is ever more than that.
if missing > 0.02 * len(wanted):
    raise SystemExit(f"[pool] {{missing:,}} of {{len(wanted):,}} reviewed accessions "
                     f"({{100.0*missing/len(wanted):.1f}}%) are absent from Swiss-Prot "
                     f"{{SP.name}}. The reviewed cut is meant to BE this release; a gap "
                     f"this size means Rhea and UniProt are far enough apart that the "
                     f"pool would silently be a subset of what it claims")

labels[labels["accession"].isin(seen)].to_parquet(LABELS_OUT, index=False)

reldate = SP / "{reldate_file}"
with open(SOURCE_OUT, "w") as fh:
    fh.write("sequences\tswissprot " + SP.name + "\n")
    fh.write("labels\tmnxr_lookup id_source={id_source} evidence_quality={evidence}\n")
    fh.write("embedder\tproteinbert (weights baked into env::proteinbert.env)\n")
    fh.write("sequences_written\t" + str(written) + "\n")
    fh.write("labelled_accessions\t" + str(len(wanted)) + "\n")
    if reldate.exists():
        fh.write("reldate\t" + reldate.read_text().strip().replace("\n", " | ") + "\n")
'''

ASSEMBLE = r"""
import re
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

SHARDS = Path("pbert_output")
POOL = Path("{pool}")
POOL.mkdir(parents=True, exist_ok=True)


# `<stem>.<k>.embedding.npy` with k unpadded, so the chunks order by k as an INTEGER.
# The number is INSIDE the name, not at its end.
def chunk_no(path):
    m = re.search(r"\.(\d+)(?:\.embedding)?$", path.stem)
    if m is None:
        raise SystemExit(
            f"[pool] {{path.name}} does not end in a chunk number, so the order the "
            f"embedder wrote its chunks in is not recoverable. Guessing one is what "
            f"scrambled this artifact the first time")
    return int(m.group(1))


npys = sorted(SHARDS.glob("*.npy"), key=chunk_no)
csvs = sorted(SHARDS.glob("*.csv"))
if len(csvs) > 1:
    csvs = sorted(csvs, key=chunk_no)
if not npys or not csvs:
    raise SystemExit(f"[pool] embedder produced no output under {{SHARDS}}")

# Narrow and downcast EACH chunk before stacking, never after. Written the other way
# round -- vstack the full chunks, then slice to 512 and cast -- the peak is every
# chunk at full ProteinBERT width in its native dtype, PLUS vstack's own copy of all
# of it, and only then is 99% of that thrown away. On ~222k sequences that is the
# difference between tens of GB of transient and a couple.
stack = np.vstack([np.load(f)[:, -512:].astype(np.float32) for f in npys])
idx = pd.concat([pd.read_csv(f) for f in csvs], ignore_index=True)
# `pbert` names its id column `id`; the run-side transform renames it to
# `sequence_id` on the way out. This reads the embedder's raw output, so it takes
# either -- and refuses rather than producing an unlabelled pool if neither is there.
for _cand in ("sequence_id", "id"):
    if _cand in idx.columns:
        accession = idx[_cand].astype(str).to_numpy()
        break
else:
    raise SystemExit(f"[pool] the embedder index has no id column: {{list(idx.columns)}}")

if len(accession) != len(stack):
    raise SystemExit(f"[pool] the embedder index has {{len(accession)}} rows and its "
                     f"chunks stack to {{len(stack)}}")

# THE ORDER IS CHECKED, NOT ASSUMED. Both the stack and the index claim to be in the
# order of _pool.faa, which this transform wrote from its own selection. Disagreement
# means the embedder changed how it names or splits its output, and the result of
# proceeding is a complete, schema-valid, wrong artifact -- so it stops here.
want = [ln[1:].split()[0] for ln in Path("_pool.faa").read_text().splitlines()
        if ln.startswith(">")]
if list(accession) != want:
    bad = next((i for i, (a, b) in enumerate(zip(accession, want)) if a != b),
               min(len(accession), len(want)))
    raise SystemExit(
        f"[pool] the embedder index is not in the order of the FASTA it was given: "
        f"row {{bad}} is {{list(accession[bad:bad+1])}}, the FASTA has {{want[bad:bad+1]}}. "
        f"Every landmark would carry another protein's reactions")

labels = pd.read_parquet("_pool_labels.parquet")
table = pd.DataFrame({{"accession": accession}}).merge(labels, on="accession", how="left")
n_unlabelled = int(table["mnxr_list"].isna().sum())
# Every sequence written was selected BECAUSE it had labels, so an unlabelled row here is
# the embedder having dropped or renamed an id between the FASTA and its index -- which
# would misalign the merge rather than merely thin the set.
if n_unlabelled:
    raise SystemExit(f"[pool] {{n_unlabelled:,}} embedded sequences carry no label, but "
                     f"the landmarks were selected on having one -- the embedder's index "
                     f"ids do not match the FASTA headers this transform wrote")

table = pd.concat([table, pd.DataFrame(
    stack, columns=[f"dim_{{i}}" for i in range(stack.shape[1])])], axis=1)
table.to_parquet(POOL / "{table_name}", index=False)
shutil.copy("_pool_source.txt", POOL / "{source_name}")
print(f"[pool] {{len(table):,}} landmarks, "
      f"{{table['mnxr_list'].str.split(';').explode().nunique():,}} distinct MNXR, "
      f"{{stack.shape[1]}} dims", flush=True)
"""


def protocol(context: ExecutionContext):
    ibridge = context.Input(bridge)
    isrc    = context.Input(source)
    ipool   = context.Output(pool)

    select = SELECT.format(bridge=ibridge.container, swissprot=isrc.container,
                           fasta_file=FASTA_FILE, reldate_file=RELDATE_FILE,
                           id_source=POOL_ID_SOURCE, evidence=POOL_EVIDENCE)
    context.LocalShell("cat > _pool_select.py << 'PYEOF'\n" + select + "\nPYEOF\n")

    # Both halves run in the ProteinBERT image. It carries numpy/pandas, and running the
    # slice somewhere else would mean staging the bridge across two environments.
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _pool_select.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _pool_select.py")

    threads = context.params.get("cpus", 4)
    _cmd = f"""
        pbert run -i _pool.faa -o pbert_output \
            --threads {threads} --protein_size 512 --model_batch 1024 -x 1
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    assemble = ASSEMBLE.format(pool=ipool.container, table_name=TABLE_NAME,
                               source_name=SOURCE_NAME)
    context.LocalShell("cat > _pool_assemble.py << 'PYEOF'\n" + assemble + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _pool_assemble.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _pool_assemble.py")

    ok = all((ipool.local / n).exists() for n in (TABLE_NAME, SOURCE_NAME))
    return ExecutionResult(
        manifest=[{pool: ipool.local}],
        success=ok,
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
    resources=Resources(cpus=4, memory=Size.GB(32), duration=Duration(hours=8)),
)
