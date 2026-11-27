# The reference-free host links: each frozen virus against the survey's own
# quality MAGs, by CRISPR spacer and by position on a binned contig. Nothing is
# filtered by mismatches; the table carries what a filter needs.
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

python = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
link   = model.AddRequirement(lib.GetType("lib::virus_host_links.py"))

# merge_candidate_calls' shape: one assembly per sample, and the MAGs hung off it,
# so the MAGs come from the assembly the frozen set was cut from.
study  = model.AddRequirement(lib.GetType("viromics::contig_study"))
sample = model.AddRequirement(lib.GetType("sequences::read_metadata"), parents={study})
asm    = model.AddRequirement(lib.GetType("sequences::assembly"), parents={sample})
bins   = model.AddRequirement(lib.GetType("binning_local::quality_bin_fasta"), parents={asm})
arrays = model.AddRequirement(lib.GetType("viromics::crispr_arrays"), parents={bins})
prov   = model.AddRequirement(lib.GetType("viromics::candidate_call_provenance"), parents={study})
hits   = model.AddRequirement(lib.GetType("viromics::spacer_host_links"), parents={study})

out = model.AddProduct(lib.GetType("viromics::virus_host_links"))


def protocol(context: ExecutionContext):
    arrays_of = {}
    for a in context.InputGroup(arrays):
        b = context.SourceOf(a, bins)
        assert b is not None, f"no quality MAG in the lineage of [{a.local.name}]"
        arrays_of[b.local.stem] = a

    manifest = Path("mags.tsv")
    with open(manifest, "w") as f:
        f.write("mag\tsample\tassembly\tfasta\tarrays\n")
        for b in context.InputGroup(bins):
            meta = context.SourceOf(b, sample)
            a = context.SourceOf(b, asm)
            assert meta is not None and a is not None, (
                f"no read_metadata or assembly in the lineage of [{b.local.name}]; the "
                "sample label and assembly stem are what place a frozen virus on a MAG contig"
            )
            arr = arrays_of.get(b.local.stem)
            # The assembly stem is the `<stem>~` prefix splitContigsForAmr puts on every
            # contig the frozen set was cut from; matching it keeps a hybrid sample's
            # MEGAHIT draft and OPERA-MS contigs apart.
            f.write("\t".join([
                b.local.stem, Path(meta.local).stem, Path(a.local).stem,
                str(b.container), "NA" if arr is None else str(arr.container),
            ]) + "\n")

    o = context.Output(out)
    context.ExecWithEnv(env=python, cmd=f"""
        python3 {context.Input(link).container} \
            --provenance {context.Input(prov).container} \
            --spacer-hits {context.Input(hits).container} \
            --mags {manifest} --out {o.container}
    """)
    return ExecutionResult(manifest=[{out: o.local}], success=o.local.exists())


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=study,
    output_signature={out: "virus_host_links.tsv"},
    resources=Resources(cpus=1, memory=Size.GB(8), duration=Duration(hours=2)),
)
