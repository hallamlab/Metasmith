import json

import me_finder.tools
import me_finder.tools.blast as _blast


# blastn's -outfmt 15 can write several JSON documents back to back, one per query batch, and
# me_finder 1.1.2 reads the file with a bare json.load(), which raises on the second document.
# Hit-rich contig batches were the ones that failed. Every document is read and their reports joined.
def _documents(path):
    with open(path) as fh:
        text = fh.read()
    decoder = json.JSONDecoder()
    pos = 0
    while True:
        while pos < len(text) and text[pos].isspace():
            pos += 1
        if pos == len(text):
            return
        doc, pos = decoder.raw_decode(text, pos)
        yield doc


def _validate_blast_json(path):
    try:
        docs = list(_documents(path))
    except json.decoder.JSONDecodeError:
        _blast.LOG.warning("JSON file is invalid")
        return False
    return len(docs) > 0 and all("BlastOutput2" in d for d in docs)


def iter_blast_hits(blast_result_path):
    reports = [r for d in _documents(blast_result_path) for r in d["BlastOutput2"]]
    yield from _blast._parse_hits_json({"BlastOutput2": reports})


_blast._validate_blast_json = _validate_blast_json
_blast.iter_blast_hits = iter_blast_hits
# The package re-exports the function, and the predictors import it from there.
me_finder.tools.iter_blast_hits = iter_blast_hits
_blast.MSM_JSONFIX = True
