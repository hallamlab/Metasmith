# Gene calling on the frozen viral set, and the whole reason step 25 needs no new
# annotation transform: it produces sequences::orfs, so the library's existing
# chunkOrfsForAnnotation -> kofamscan -> merge_kofamscan chain runs on the viral
# contigs unchanged.
#
# prodigal-gv rather than the library's plain `pprodigal -p meta`, and this is a
# correctness point rather than packaging: many phage recode TAG or TGA, and the
# standard tables truncate their genes. geNomad and CheckV both call viral genes
# with pyrodigal-gv for exactly this reason. It is also why prodigal.py is left
# alone -- widening it to sequences::contig_fasta would have made the whole
# per-sample assembly reachable by every contig-level annotator.
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

# checkv.env, not a prodigal-gv env of its own: that image already ships
# /usr/local/bin/prodigal-gv at the same upstream 2.11.0-gv build, so this is
# one fewer image to pull, cache and stage on every host.
image  = model.AddRequirement(lib.GetType("env::checkv.env"))

frozen = model.AddRequirement(lib.GetType("viromics::dereplicated_candidate_virus"))
cds    = model.AddProduct(lib.GetType("sequences::orfs"))
gff    = model.AddProduct(lib.GetType("sequences::gff"))


def protocol(context: ExecutionContext):
    ifrozen = context.Input(frozen)
    outs = {p: context.Output(p) for p in (cds, gff)}

    # -p meta because the frozen set is many unrelated genomes, not one. -a takes
    # the protein FASTA and -o the annotation, whose format -f selects; prodigal
    # writes nucleotide CDS only with -d, which nothing here consumes.
    _cmd = f"""
        prodigal-gv -p meta -i {ifrozen.container} \
            -a viral_orfs.faa -f gff -o viral_orfs.gff
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=_cmd) \
        .ifVirtualEnvDo(env=image, cmd=_cmd)

    context.LocalShell(f"cp viral_orfs.faa {outs[cds].local}")
    context.LocalShell(f"cp viral_orfs.gff {outs[gff].local}")

    return ExecutionResult(
        manifest=[{p: o.local for p, o in outs.items()}],
        success=all(o.local.exists() for o in outs.values()),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=frozen,
    output_signature={cds: "viral_orfs.faa", gff: "viral_orfs.gff"},
    resources=Resources(cpus=8, memory=Size.GB(16), duration=Duration(hours=4)),
)
