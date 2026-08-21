from metasmith.python_api import *
from pathlib import Path
import csv

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image    = model.AddRequirement(lib.GetType("env::kofamscan.env"))
orfs     = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
profiles = model.AddRequirement(lib.GetType("ref::kofamscan_profiles"))
ko_list  = model.AddRequirement(lib.GetType("ref::kofamscan_ko_list"))
out_results = model.AddProduct(lib.GetType("annotation::kofamscan_results_chunk"))
out_descriptions = model.AddProduct(lib.GetType("annotation::kofamscan_descriptions_chunk"))


def parse_kofamscan(input_path, output_path):
    header = "gene_name,KO,thrshld,score,E-value,best\n"
    kos = set()
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
                        kos.add(ko)
                except ValueError:
                    outfile.write(f"{gene},{ko},{thr},{score},{evalue},{is_best}\n")
                    kos.add(ko)
    return kos


# The detail format's own trailing "KO definition" field is not read: the parser
# above splits on whitespace, which shreds a multi-word definition. ko_list is a
# declared requirement and its last tab-separated column is the same string.
def write_ko_definitions(ko_list_path, kos, output_path):
    definitions = {k: "" for k in kos}
    with open(ko_list_path, errors="replace") as fh:
        next(fh, None)
        for line in fh:
            fields = line.rstrip("\n").split("\t")
            if len(fields) < 2 or fields[0] not in definitions:
                continue
            definitions[fields[0]] = fields[-1]
    with open(output_path, 'w', newline='') as fh:
        writer = csv.writer(fh)
        writer.writerow(["KO", "definition"])
        for ko in sorted(definitions):
            writer.writerow([ko, definitions[ko]])


_LEGAL = set("ACDEFGHIKLMNPQRSTVWYBJZOUX*")
_STRIPPED = "-."


def sanitize_for_hmmer(src: Path, dest: Path) -> list[str]:
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
    idesc = context.Output(out_descriptions)

    cpus = context.params.get("cpus")
    cpus_string = "" if cpus is None else f"--cpu={cpus}"

    if not iprofiles.local.is_dir():
        raise SystemExit(
            f"[kofamscan] ref::kofamscan_profiles staged at {iprofiles.local} is not a "
            f"directory. Both producers -- logistics/downloadKofamDB.py and "
            f"fabfos build_references compile/kofam_ref.py -- unpack the archive "
            f"with --strip-components=1; an archive here is one of them having "
            f"moved profiles.tar.gz into the slot instead.")
    n_hmm = len(list(iprofiles.local.glob("*.hmm")))
    if n_hmm == 0:
        raise SystemExit(
            f"[kofamscan] no .hmm files directly under {iprofiles.local}. kofamscan "
            f"does not recurse, and it reports zero annotations rather than failing.")
    print(f"[kofamscan] {n_hmm:,} HMM profiles", flush=True)

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

    kos = parse_kofamscan("kofam_results.txt", str(iout.local))
    write_ko_definitions(iko_list.local, kos, str(idesc.local))

    n_rows = sum(1 for _ in open(iout.local)) - 1 if iout.local.exists() else 0
    print(f"[kofamscan] {n_rows:,} above-threshold hits over {len(kos):,} KOs",
          flush=True)
    return ExecutionResult(
        manifest=[
            {
                out_results: iout.local,
                out_descriptions: idesc.local,
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
