from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::megahit.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_short_reads"), parents={meta})
out     = model.AddProduct(lib.GetType("sequences::megahit_assembly"))
graph   = model.AddProduct(lib.GetType("sequences::megahit_assembly_graph"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    imeta=context.Input(meta)
    iout=context.Output(out)
    igraph=context.Output(graph)
    with open(imeta.local) as j:
        read_meta = json.load(j)
    parity = read_meta["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    if parity == "paired":
        parg = "--12"
    else:
        parg = "-r"
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--num-cpu-threads {threads}"
    mem_gb = context.params.get('memory')
    mem = f"--memory {int(mem_gb * 0.85 * 1024**3)}" if mem_gb else ""
    _cmd = f"""\
            megahit {threads} {mem} \
                {parg} {ireads.container} \
                -o megahit_ws
            [[ $(head megahit_ws/final.contigs.fa | wc -c) -ne 0 ]] && mv megahit_ws/final.contigs.fa {iout.container} || echo "assembly was empty"

            IC=megahit_ws/intermediate_contigs
            # The largest k the run REACHED, not --k-max: megahit stops iterating
            # once the graph is exhausted, so a low-complexity pool can finish at
            # k21 and never write k141 at all. Match k<digits>.contigs.fa exactly,
            # or the glob also swallows k<digits>.final.contigs.fa.
            KMAX=$(ls $IC 2>/dev/null | grep -E '^k[0-9]+\\.contigs\\.fa$' | sed -E 's/^k([0-9]+)\\.contigs\\.fa$/\\1/' | sort -n | tail -1)
            if [[ -s {iout.container} && -n "$KMAX" ]]; then
                # Every final contig is named k<K>_<n> after the k that emitted it,
                # so the graph at KMAX covers a contig only if its K is KMAX. On a
                # real pool all 196 contigs were k141 and none was missing from
                # k141.contigs.fa -- but that is a property of these libraries, not
                # a guarantee, so print the distribution and let a violation be
                # seen in the log rather than assumed away.
                echo "megahit graph: kmax=$KMAX ; k of final contigs (count k):"
                grep '^>' {iout.container} | sed -E 's/^>k([0-9]+)_.*/\\1/' | sort -n | uniq -c
                megahit_toolkit contig2fastg $KMAX $IC/k$KMAX.contigs.fa > {igraph.container}
            else
                echo "no intermediate_contigs and/or no contigs -- graph not reconstructed"
            fi
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                graph: igraph.local,
            },
        ],
        success=iout.local.exists() and igraph.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=32,
        memory=Size.GB(32),
        duration=Duration(hours=18),
    )
)
