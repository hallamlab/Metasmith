"""A transform that fetches from the network declares its URLs in a module-level `URLS`.

`URLS` maps a name to a URL, or to a list of mirrors of one file. The protocol fetches from it, so
the live check in deploy/test_download_urls.py probes the addresses a run would use, and
repairing a moved link means editing the contract.
"""
from __future__ import annotations

from urllib.parse import urlsplit

from tests.metasmith.support import library_transforms, source_downloads

# A transform whose tool does its own fetching, from addresses the tool or the accession picks.
EXEMPT = {
    "logistics/downloadAntiSmashDB.py": "download-antismash-databases fetches from addresses pinned in the antiSMASH release",
    "logistics/downloadVcontact3DB.py": "vcontact3 prepare_databases reads the archive address from its remote db_versions table",
    "logistics/downloadVirsorter2DB.py": "virsorter setup fetches from addresses in its own snakemake rules",
    "logistics/downloadBiofactorialContaminants.py": "SOURCE is empty because the list has no public home yet; declare URLS when it gains one",
    "logistics/downloadVibrantDB.py": "download-db.sh fetches KEGG, Pfam and VOG archives from addresses inside VIBRANT's own setup script",
    "logistics/getNcbiAssembly.py": "datasets fetches the accession it is given from NCBI's API, per sample",
    "logistics/getSraLongReads.py": "prefetch fetches the accession it is given from SRA, per sample",
    "logistics/getSraShortReads.py": "prefetch fetches the accession it is given from SRA, per sample",
}


def _declared(tr):
    return tr.protocol.__globals__.get("URLS")


def _all(metasmith_libraries_root):
    return [(name, _declared(tr), source_downloads(src)) for name, tr, src in library_transforms(metasmith_libraries_root)]


def test_every_download_declares_its_urls(metasmith_libraries_root):
    missing = [n for n, urls, downloads in _all(metasmith_libraries_root) if downloads and not urls and n not in EXEMPT]
    assert not missing, f"these fetch from the network and declare no URLS (or add an EXEMPT reason): {missing}"


def test_an_exemption_names_a_transform_that_still_needs_one(metasmith_libraries_root):
    found = {n: (urls, downloads) for n, urls, downloads in _all(metasmith_libraries_root)}
    stale = [n for n in EXEMPT if n not in found or not found[n][1] or found[n][0]]
    assert not stale, f"these are exempt but no longer fetch, or now declare URLS: {stale}"


def test_urls_are_complete_addresses(metasmith_libraries_root):
    bad = []
    for name, urls, _ in _all(metasmith_libraries_root):
        if urls is None:
            continue
        for key, value in urls.items():
            for u in [value] if isinstance(value, str) else value:
                parts = urlsplit(u) if isinstance(u, str) else None
                if parts is None or parts.scheme not in {"http", "https", "ftp"} or not parts.hostname or "{" in u:
                    bad.append(f"{name}[{key}] = {u!r}")
            if not value:
                bad.append(f"{name}[{key}] is empty")
    assert not bad, bad
