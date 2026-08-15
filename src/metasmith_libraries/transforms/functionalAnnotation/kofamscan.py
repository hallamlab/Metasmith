"""KOfam HMM annotation of the ORFs -> kofamscan_results.

`ref::kofamscan_profiles` IS A DIRECTORY of .hmm files, not an archive. This step
used to `pigz -dc | tar xf -` it, which was right when the only producer was
`transforms/logistics/downloadKofamscanDB.py` (a run-time fetch of the upstream
tarball) and wrong once `build_references/compile/kofam_ref.py` became the producer
-- that one untars with `--strip-components=1` precisely because kofamscan is handed
a profile DIRECTORY, and a nested extra level makes it find nothing while raising
nothing. The two producers disagreeing about the shape of one reference is the same
hazard that keeps `logistics/` out of every reference plan.

The ko_list is a plain TSV and carries each family's own score threshold; a list
paired with profiles from a different build applies the wrong cut to every hit,
silently, which is why the two are one DVC chunk.

THE QUERY IS SANITIZED FIRST, and this is not optional politeness. HMMER rejects a
gap character in an unaligned FASTA, and NCBI writes `-` into translated CDS for
pseudogenes and frameshifts -- 77 sequences across the three benchmark host
proteomes. Every hmmsearch then dies instantly with a zero-byte table, and kofamscan
does not report it: the Ruby driver and its `parallel` child deadlock against each
other, both blocked in pipe_write on a pipe neither drains. The lane presents as
RUNNING at 0% CPU forever, so it is not a failed task, no error strategy sees it, and
only a wall-clock timeout ends it. DIAMOND, CLEAN and ProteinBERT all accept the same
input without complaint, so nothing upstream flags the sequences as unusual.
"""
from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::kofamscan.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
profiles = model.AddRequirement(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddRequirement(lib.GetType("ref::kofamscan_ko_list"))
out_results = model.AddProduct(lib.GetType("annotation::kofamscan_results_chunk"))


def parse_kofamscan(input_path, output_path):
    header = "gene_name,KO,thrshld,score,E-value,best\n"
    with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
        outfile.write(header)
        for line in infile:
            if line.startswith('#') or not line.strip():
                continue
            stripped = line.strip()
            is_best = "*" if stripped.startswith("*") else ""
            parts = stripped.lstrip('* ').split()
            if len(parts) >= 5:
                gene, ko, thr, score, evalue = parts[:5]
                try:
                    if float(score) >= float(thr):
                        outfile.write(f"{gene},{ko},{thr},{score},{evalue},{is_best}\n")
                except ValueError:
                    outfile.write(f"{gene},{ko},{thr},{score},{evalue},{is_best}\n")


# HMMER's protein alphabet, plus the degenerate codes and the stop character it
# accepts. Anything outside this set stops the run rather than reaching hmmsearch,
# because hmmsearch's own way of complaining is a hang, not an error.
#
# `*` IS LEGAL and is deliberately not stripped, though it looks like the same class
# of artifact as a gap: these proteomes carry ~1,030 internal stops across 151
# sequences from pseudogenes, and hmmsearch annotates them without complaint. Removing
# it would edit three times as many sequences as the gaps do, to fix nothing.
_LEGAL = set("ACDEFGHIKLMNPQRSTVWYBJZOUX*")
_STRIPPED = "-."


def sanitize_for_hmmer(src: Path, dest: Path) -> list[str]:
    """Write `src` to `dest` without gap characters; return the sequences changed.

    Gaps are REMOVED rather than their sequences dropped. Dropping would lose 76 of
    CP189566.1's proteins from this lane alone, leaving the four lanes disagreeing
    about which proteins exist -- and the mapper joins them by protein id, so a lane
    that is short 76 rows is a lane that silently contributes nothing for them. A gap
    in a translated CDS is a frameshift artifact, not a residue, so closing it is the
    smaller distortion.

    Anything still outside the alphabet after that is raised by name. hmmsearch would
    otherwise take it, die per profile, and hang the whole step.
    """
    touched: list[str] = []
    name = "?"
    with open(src) as fin, open(dest, "w") as fout:
        for line in fin:
            if line.startswith(">"):
                name = line[1:].split()[0] if len(line) > 1 else "?"
                fout.write(line)
                continue
            seq = line.strip()
            clean = seq.translate(str.maketrans("", "", _STRIPPED))
            if clean != seq and name not in touched:
                touched.append(name)
            illegal = sorted(set(clean.upper()) - _LEGAL)
            if illegal:
                raise SystemExit(
                    f"[kofamscan] {name} carries {illegal}, which is outside HMMER's "
                    f"protein alphabet and is not a gap this step knows to close. "
                    f"hmmsearch answers this by writing an empty table and hanging, so "
                    f"it stops here instead.")
            fout.write(clean + "\n")
    return touched


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iprofiles = context.Input(profiles)
    iko_list = context.Input(ko_list)
    iout = context.Output(out_results)

    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"--cpu={cpus}"

    # A DIRECTORY of .hmm files, bound straight in. Refuse anything else by name:
    # kofamscan given a path with no profiles under it reports zero annotations and
    # exits 0, so a wrong shape here is a silently empty lane.
    if not iprofiles.local.is_dir():
        raise SystemExit(
            f"[kofamscan] ref::kofamscan_profiles staged at {iprofiles.local} is not a "
            f"directory. compile/kofam_ref.py produces the unpacked profile directory; "
            f"an archive here means the reference came from logistics/"
            f"downloadKofamscanDB.py instead, which is the duplicate producer every "
            f"reference plan excludes.")
    n_hmm = len(list(iprofiles.local.glob("*.hmm")))
    if n_hmm == 0:
        raise SystemExit(
            f"[kofamscan] no .hmm files directly under {iprofiles.local}. kofamscan "
            f"does not recurse, and it reports zero annotations rather than failing.")
    print(f"[kofamscan] {n_hmm:,} HMM profiles", flush=True)

    # Written into the task's working directory, which is the container's --pwd, so the
    # bare name resolves inside without another bind.
    query = Path("kofam_query.faa")
    gapped = sanitize_for_hmmer(iorfs.local, query)
    if gapped:
        shown = ", ".join(gapped[:5]) + (" ..." if len(gapped) > 5 else "")
        print(f"[kofamscan] closed gaps in {len(gapped):,} sequence(s): {shown}",
              flush=True)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (iprofiles.external, "/profiles"),
            (iko_list.external.parent, "/ko"),
        ],
        cmd=f"""
            exec_annotation \
                -o kofam_results.txt \
                --profile=/profiles \
                --ko-list=/ko/{iko_list.external.name} \
                {cpus_string} \
                --e-value=0.01 \
                --format=detail \
                --no-report-unannotated \
                {query.name}
        """,
    )

    # Parse and filter results into CSV
    parse_kofamscan("kofam_results.txt", str(iout.local))

    # Header-only is not success: the GPR mapper's kofam lane would join to nothing
    # and the whole table would be short one channel with no explanation.
    n_rows = sum(1 for _ in open(iout.local)) - 1 if iout.local.exists() else 0
    print(f"[kofamscan] {n_rows:,} above-threshold hits", flush=True)
    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
            },
        ],
        success=iout.local.exists() and n_rows > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=8),
    ),
)
