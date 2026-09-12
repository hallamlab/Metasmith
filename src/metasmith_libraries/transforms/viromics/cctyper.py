# Antonio's step 22, on the MAG lane: CRISPR arrays and cas operons per quality
# bin. Its third product, the spacers, is the input to the within-survey half of
# host prediction.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# No ref:: requirement: this image ships its own 705-profile database. But --db
# MUST be passed explicitly, and finding that out cost a green run with empty
# tables. The image sets CCTYPER_DB through a conda activate.d hook, which only a
# LOGIN shell sources; the container exec here does not use one, so the variable
# is unset at run time and cctyper reports "Could not find database directory".
# Probing the image with `bash -lc` says the opposite and is the wrong test --
# run the tool the way the pipeline runs it.
image = model.AddRequirement(lib.GetType("env::cctyper.env"))

# Where the bundled database sits inside the image. A literal, because it is a
# property of the pinned image rather than of the host, and the env file's digest
# is what holds it still.
CCTYPER_DB = "/usr/local/cct_data"
bin = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"))

out_arrays  = model.AddProduct(lib.GetType("viromics::crispr_arrays"))
out_operons = model.AddProduct(lib.GetType("viromics::cas_operons"))
out_spacers = model.AddProduct(lib.GetType("viromics::crispr_spacers"))


def protocol(context: ExecutionContext):
    threads = context.params.get("cpus", 8)
    results = []
    for k, item in enumerate(context.AsBatch()):
        ibin = item.Input(bin)
        outs = {p: item.Output(p) for p in (out_arrays, out_operons, out_spacers)}
        work = f"cct_{k}"
        stem = ibin.local.stem

        # Two failure modes share one non-zero exit and must not share a meaning.
        # A bin with no CRISPR array is a result, so the run itself is allowed to
        # fail. A missing database is a misconfiguration, so it is checked first
        # and kills the step -- without this, "no arrays anywhere in the survey"
        # and "the tool never ran" are the same empty table.
        _cmd = f"""
            test -d {CCTYPER_DB} || {{ echo "cctyper database absent at {CCTYPER_DB}" >&2; exit 3; }}
            cctyper {ibin.container} {work} --db {CCTYPER_DB} \
                --prodigal meta -t {threads} --no_plot || true
        """
        context.ExecWithEnv(env=image, cmd=_cmd)

        w = Path(work)
        for prod, name in ((out_arrays, "crisprs_all.tab"), (out_operons, "cas_operons.tab")):
            src = w/name
            if src.exists():
                outs[prod].local.write_bytes(src.read_bytes())
            else:
                Log.Info(f"[{stem}] no {name}; emitting an empty table")
                outs[prod].local.write_text("")

        # The spacers are a DIRECTORY, one FASTA per array, and the paper's
        # "non-redundant spacer database" is that directory concatenated. Each
        # header is tagged with its bin and array so a hit downstream names the
        # MAG it came from -- nothing else in the chain carries that back.
        n = 0
        with open(outs[out_spacers].local, "w") as dst:
            for fa in sorted((w/"spacers").glob("*.fa")) if (w/"spacers").is_dir() else []:
                array = fa.stem
                for line in fa.read_text().splitlines():
                    if line.startswith(">"):
                        n += 1
                        dst.write(f">{stem}|{array}|{line[1:].strip()}\n")
                    elif line.strip():
                        dst.write(line.strip() + "\n")
        Log.Info(f"[{stem}] {n} spacers")

        results.append(ExecutionResult(
            manifest=[{p: o.local for p, o in outs.items()}],
            success=all(o.local.exists() for o in outs.values()),
        ))
    return results


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=bin,
    batch_size=50,
    output_signature={
        out_arrays: "crisprs_all.tab",
        out_operons: "cas_operons.tab",
        out_spacers: "spacers.fna",
    },
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
