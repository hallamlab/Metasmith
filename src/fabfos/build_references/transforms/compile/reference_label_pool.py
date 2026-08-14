"""R7 -- the labelled embedding pool the kNN transfer lane votes against.

Swiss-Prot sequences, embedded with ProteinBERT, labelled with MetaNetX reaction ids
mapped through Rhea. Those three clauses are one sentence and each is load-bearing:

  SWISS-PROT      the sequences. `fabfos_data::swissprot`, the reviewed half of
                  UniProtKB, ~93 MB.
  PROTEINBERT     the embedder, from the same pinned image the run-side lane uses.
  MNXR VIA RHEA   the labels. `ref::mnxr_lookup` rows with `id_source == "uniprot"`,
                  which is exactly the rhea2uniprot route -- UniProt accession -> Rhea
                  reaction -> MNXR through MetaNetX's `rhea:` xrefs.

SWISS-PROT REPLACES UNIREF50 AS THE SEQUENCE SOURCE, and the reason is that the cut
and the sequences now describe the same set. The pool is defined by the bridge's
`reviewed` rows, and `reviewed` means the accession came from `rhea2uniprot.tsv`
rather than `rhea2uniprot_trembl.tsv.gz` -- i.e. it means Swiss-Prot, exactly. Taking
the sequences from UniRef50 instead meant an accession only had a sequence if it
happened to be its cluster's REPRESENTATIVE: UniRef50 clusters at 50% identity, so an
entry sitting under another entry's representative dropped out of the pool silently.
That was counted rather than substituted for, but it was a coverage loss with no
upside once ~93 MB of exactly the right sequences is already a source folder in the
graph. It also drops an 8.8 GB gzip stream out of this transform's inputs.

WHAT THIS IS NOT. The deployed pool is KEGG-derived -- 54,005 sequences keyed on KEGG
gene ids (`dme:Dmel_CG3481`), labelled by KO, projecting KO -> MNXR. Building from the
Rhea route means one label source instead of two (so a protein cannot be labelled one
way here and a different way in the GPR mapper) and no KEGG-licensed sequences in the
tree -- but a DIFFERENT set, so the pbert lane's numbers move. This is not a
reproduction of the deployed lane and must not be reported as one.

Two traps worth stating because both are silent:
  * SAME MODEL. A pool embedded with a different model from the query is not a
    weaker pool, it is a meaningless one -- cosine distance between two embedding
    spaces is a number with no referent. Enforced by sharing `env::proteinbert.env`
    with functionalAnnotation/proteinbert.py, whose flags are copied verbatim below.
  * ONE PASS. The consumer indexes the embedding stack by row, so pairing an index
    from one build with a stack from another misindexes every row and emits a full,
    confident, WRONG table with nothing raised. Index and stack come out together
    or not at all.

THE WEIGHTS ARE FREE. ProteinBERT's are baked into the pinned image, so this
transform acquires no model and the pool is the only artifact it produces. That is
also why it runs under a container runtime: `proteinbert.env` carries no `conda:`
key, so there is no MAMBA path for it.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image  = model.AddRequirement(lib.GetType("env::proteinbert.env"))
source = model.AddRequirement(lib.GetType("fabfos_data::swissprot"))
bridge = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
pool   = model.AddProduct(lib.GetType("ref::reference_label_pool"))

# The cut that defines the pool: bridge rows whose id_source is uniprot and whose
# evidence_quality is reviewed. The second condition is what makes Swiss-Prot the right
# sequence source rather than merely a convenient one -- see the header.
POOL_ID_SOURCE = "uniprot"
POOL_EVIDENCE = "reviewed"

FASTA_FILE = "uniprot_sprot.fasta.gz"
RELDATE_FILE = "reldate.txt"

# The layout the consumer reads. functionalAnnotation/gpr_4lane.py's embedding lane opens
# exactly these two names inside the pool directory and indexes the stack by the index's
# `row` column, which is why they are produced together and never separately.
INDEX_NAME = "orf_index.parquet"
STACK_NAME = "emb_pbert.npy"
# A third file, read by nothing. An embedding pool is comparable to a query only if the
# model matches and reproducible only if the sequence release does, and neither fact is
# recoverable from the two files above.
SOURCE_NAME = "pool_source.txt"

# The slice step: pick the pool members and write their sequences out as a FASTA for the
# embedder. Split from the embedding step so the failure modes stay separable -- this half
# fails on a join, the other on a GPU.
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

# The assemble step: stitch the embedder's shards into ONE stack and write the index that
# addresses it, in one pass. The consumer indexes the stack BY ROW, so an index from one
# build against a stack from another misindexes every row and emits a full, confident,
# wrong table with nothing raised -- which is why these two files are written together,
# from the same in-memory arrays, or not at all.
ASSEMBLE = r'''
import shutil
import numpy as np
import pandas as pd
from pathlib import Path

SHARDS = Path("pbert_output")
POOL = Path("{pool}")
POOL.mkdir(parents=True, exist_ok=True)

# The embedder writes one .npy per shard plus a csv index naming the sequences in order.
# Both are read in the SAME sorted shard order, so row i of the stack is sequence i of
# the index by construction rather than by coincidence.
npys = sorted(SHARDS.glob("*.npy"))
csvs = sorted(SHARDS.glob("*.csv"))
if not npys or not csvs:
    raise SystemExit(f"embedder produced no output under {{SHARDS}}")
# Narrow and downcast EACH shard before stacking, never after. Written the other way
# round -- vstack the full shards, then slice to 512 and cast -- the peak is every
# shard at full ProteinBERT width in its native dtype, PLUS vstack's own copy of all
# of it, and only then is 99% of that thrown away. On ~222k sequences that is the
# difference between tens of GB of transient and a couple.
stack = np.vstack([np.load(f)[:, -512:].astype(np.float32) for f in npys])
idx = pd.concat([pd.read_csv(f) for f in csvs], ignore_index=True)
if len(idx) != len(stack):
    raise SystemExit(f"index has {{len(idx)}} rows but the stack has {{len(stack)}} -- "
                     f"pairing them would misindex every row silently")

labels = pd.read_parquet("_pool_labels.parquet")
# `pbert` names its id column `id`; the run-side transform renames it to
# `sequence_id` on the way out. This reads the embedder's raw output, so it takes
# either -- and refuses rather than producing an unlabelled pool if neither is there.
for _cand in ("sequence_id", "id"):
    if _cand in idx.columns:
        idx = idx.rename(columns={{_cand: "orf"}})
        break
else:
    raise SystemExit(f"the embedder index has no id column: {{list(idx.columns)}}")
idx["row"] = np.arange(len(idx), dtype=np.int64)
idx["role"] = "reference"
merged = idx.merge(labels.rename(columns={{"accession": "orf"}}), on="orf", how="left")
n_unlabelled = int(merged["mnxr_list"].isna().sum())
merged["mnxr_list"] = merged["mnxr_list"].fillna("")

np.save(POOL / "{stack_name}", stack)
merged[["role", "row", "orf", "mnxr_list"]].to_parquet(POOL / "{index_name}", index=False)
shutil.copy("_pool_source.txt", POOL / "{source_name}")
print(f"[pool] {{len(merged):,}} reference embeddings, {{n_unlabelled:,}} unlabelled, "
      f"{{merged['mnxr_list'].str.split(';').explode().replace('', None).nunique():,}} "
      f"distinct MNXR", flush=True)
# Every sequence written was selected BECAUSE it had labels, so an unlabelled row here is
# the embedder having dropped or renamed an id between the FASTA and its index -- which
# would misalign the merge rather than merely thin the pool.
if n_unlabelled:
    raise SystemExit(f"{{n_unlabelled:,}} embedded sequences carry no label, but the pool "
                     f"was selected on having one -- the embedder's index ids do not "
                     f"match the FASTA headers this transform wrote")
'''


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
    # SAME MODEL as the query: these flags are functionalAnnotation/proteinbert.py's,
    # verbatim.
    _cmd = f"""
        pbert run -i _pool.faa -o pbert_output \
            --threads {threads} --protein_size 512 --model_batch 1024 -x 1
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    assemble = ASSEMBLE.format(pool=ipool.container, index_name=INDEX_NAME,
                               stack_name=STACK_NAME, source_name=SOURCE_NAME)
    context.LocalShell("cat > _pool_assemble.py << 'PYEOF'\n" + assemble + "\nPYEOF\n")
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd="python3 _pool_assemble.py") \
        .ifVirtualEnvDo(env=image, cmd="python3 _pool_assemble.py")

    ok = all((ipool.local / n).exists() for n in (INDEX_NAME, STACK_NAME, SOURCE_NAME))
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
