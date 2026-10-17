from metasmith.python_api import *
import pandas as pd
import json

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads   = model.AddRequirement(lib.GetType("sequences::short_reads"))
image   = model.AddRequirement(lib.GetType("env::fastqc.env"))
stats   = model.AddProduct(lib.GetType("sequences::read_qc_stats"))
html    = model.AddProduct(lib.GetType("sequences::fastqc_html_report"))
raw     = model.AddProduct(lib.GetType("sequences::fastqc_raw_report"))

def protocol(context: ExecutionContext):
    istats = context.Output(stats)
    ihtml = context.Output(html)
    iraw = context.Output(raw)
    ireads = context.Input(reads)
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"-t {threads}"
                
    rname = ireads.container.name.replace(".fq.gz", "")
    context.ExecWithEnv(
        env = image,
        cmd = f"""
            mkdir fastqc_out
            fastqc \
                {threads} \
                -o fastqc_out \
                {ireads.container}
            mv fastqc_out/{rname}_fastqc.html {ihtml.container}
            mv fastqc_out/{rname}_fastqc.zip {iraw.container}
            unzip {iraw.container}
        """
    )

    def _as_number(x):
        if "." not in x:
            try:
                return int(x)
            except ValueError:
                pass
        try:
            return float(x)
        except ValueError:
            return x
    blocks = {}
    with open(f"{rname}_fastqc/fastqc_data.txt") as f:
        in_block = False
        _block = None
        _header = None
        _entry = []
        for l in f:
            if not in_block:
                if l.startswith(">>"):
                    _block = l[len(">>"):-len("\n")]
                    _block = _block.split("\t")[0]
                    in_block = True
                continue
            if l.startswith(">>END_MODULE"):
                in_block = False
                blocks[_block] = dict(header=_header, rows=_entry)
                _entry = []
                continue
            l = l[:-1]
            row = l.split("\t")
            if l[0] == "#":
                row[0] = row[0][1:]
                _header = row
            else:
                row = [_as_number(x) for x in row]
                _entry.append(row)
    print(json.dumps(blocks, indent=4))

    basic_stats = {k:v for k, v in blocks["Basic Statistics"]["rows"]}
    n_seq = basic_stats["Total Sequences"]
    encoding = str(basic_stats["Encoding"])
    phred_encoding = "sanger" if "Sanger" in encoding else encoding.lower()

    qrows = blocks["Per sequence quality scores"]["rows"]
    n_scored = sum(n for _, n in qrows)
    mean_quality = sum(q*n for q, n in qrows)/n_scored
    _sum = 0
    median_quality = 0
    for q, n in qrows:
        _sum += n
        if _sum >= n_scored/2:
            median_quality = q
            break

    # FastQC bins a spread of lengths into ranges like "35-39"; a bin counts at its midpoint.
    def _length(x):
        if isinstance(x, str):
            lo, hi = x.split("-")
            return (int(lo)+int(hi))/2
        return x
    lrows = [(_length(l), n) for l, n in blocks["Sequence Length Distribution"]["rows"]]
    bases = sum(l*n for l, n in lrows)
    _sum = 0
    n50 = 0
    for l, n in sorted(lrows, reverse=True):
        _sum += l*n
        if _sum >= bases/2:
            n50 = int(round(l))
            break

    stats_json = dict(
        reads = n_seq,
        bases = int(round(bases)),
        mean_quality = mean_quality,
        q20 = None,
        q30 = None,
        phred_encoding = phred_encoding,
        N50 = n50,
        GC = basic_stats["%GC"],
        median_quality = median_quality,
        _raw = blocks,
    )
    
    with open(istats.local, "w") as j:
        json.dump(stats_json, j)

    return ExecutionResult(
        manifest=[{
            html:ihtml.local,
            raw:iraw.local,
            stats:istats.local,
        }],
        success=ihtml.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=reads,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=6),
    )
)
