from dataclasses import dataclass

_COMPLEMENT = str.maketrans("ACGTNacgtn", "TGCANtgcan")


@dataclass
class Feature:
    contig: str
    start: int
    end: int
    strand: str
    type: str
    product: str
    score: float
    source: str
    id: str = ""
    extra: str = ""

    def overlaps(self, other) -> bool:
        return self.start <= other.end and other.start <= self.end


def read_fasta(path):
    seqs, name, chunks = {}, None, []
    with open(path) as fh:
        for line in fh:
            if line.startswith(">"):
                if name is not None:
                    seqs[name] = "".join(chunks)
                name, chunks = line[1:].split()[0], []
            else:
                chunks.append(line.strip())
    if name is not None:
        seqs[name] = "".join(chunks)
    return seqs


def revcomp(seq: str) -> str:
    return seq.translate(_COMPLEMENT)[::-1]


def write_gff(feats, path):
    with open(path, "w") as fh:
        fh.write("##gff-version 3\n")
        for f in feats:
            score = "." if f.score is None else f"{f.score:g}"
            extra = f";{f.extra}" if f.extra else ""
            fh.write(f"{f.contig}\t{f.source}\t{f.type}\t{f.start}\t{f.end}\t{score}\t{f.strand}\t.\t"
                     f"ID={f.id};product={f.product}{extra}\n")
