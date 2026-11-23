"""Copy each dataset's existing metadata into data/engcyc/metadata/<dataset_id>.tsv.

It also rewrites the manifest's metadata_table column to link exactly the tables it built.

Every table keeps its source's own columns. A converter only selects the dataset's rows
from a shared source, or joins two sources that describe the same libraries or genomes.
A dataset with no source here gets no table.
"""
import datetime as dt
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "data/engcyc/sources"
OUT = ROOT / "data/engcyc/metadata"


def read_tsv(path, **kw):
    return pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False, **kw)


def read_sheet(path, sheet):
    d = pd.read_excel(path, sheet_name=sheet, dtype=object)
    empty_unnamed = [c for c in d.columns if str(c).startswith("Unnamed") and d[c].isna().all()]
    d = d.drop(columns=empty_unnamed).dropna(how="all")
    return d.map(lambda v: v.date().isoformat() if isinstance(v, dt.datetime) and v.time() == dt.time() else v)


# --- metagenomes ---------------------------------------------------------------------------

CAMI = {
    "cami2_marine": ["marine", "marine_long"],
    "cami2_strain": ["strain", "strain_long"],
    "cami2_plant_associated": ["plant_associated", "plant_associated_long_nano", "plant_associated_long_pacbio"],
    "cami2_toy_mousegut": ["toy_mousegut"],
    "cami2_toy_hmp_airskinurogenital": ["toy_hmp_airskinurogenital"],
    "cami2_toy_hmp_gastrooral": ["toy_hmp_gastrooral"],
    "cami3_toy_humangut": ["toy_humangut", "toy_humangut_long"],
}
METAGEM = ["korem2015", "karlsson2013", "li2019", "sunagawa2015", "bissett_base"]


def cami():
    s = read_tsv(ROOT / "research/cami/samples.tsv")
    return {did: s[s.dataset.isin(keys)] for did, keys in CAMI.items()}


def metagem():
    m = read_tsv(ROOT / "research/metagem/manifest.tsv")
    return {did: m[m.dataset == did] for did in METAGEM}


def single_sources():
    return {
        "pratama2026": read_tsv(ROOT / "research/pratama2026/runs.tsv"),
        "cyanoverse": pd.read_csv(SRC / "capella/cyanoverse_reads.csv", dtype=str, keep_default_na=False),
        "gmcf3495_lung": read_tsv(SRC / "chinook/gmcf3495/manifest.tsv"),
        "amp_lung_spark": read_tsv(SRC / "chinook/lung_spark/metadata_updated.tsv"),
        "amp_lung_lumins": read_tsv(SRC / "chinook/lung_lumens/Complete_LuMINS_Metadata.tsv"),
        "amp_lung_host_depletion": read_tsv(SRC / "chinook/lung_host_depletion/HD_metadata_final.tsv"),
        "amp_cyano_prjna801777": read_tsv(ROOT / "research/aspire/campaigns/cyano_r1/samples.tsv"),
        "amp_nostoc_anabaena": pd.read_csv(SRC / "aspire/Nostoc_Anabaena_Metadata.csv", dtype=str,
                                           keep_default_na=False, encoding="utf-8-sig"),
        "biofactorial_pilot": read_sheet(SRC / "biofactorial/pilot_2026.09.21_RecProtocolTest.xlsx", 0),
        "biofactorial_mini": read_sheet(SRC / "biofactorial/mini_2026.09.24_MiniProtocolTest.xlsx", 0),
    }


def hydrocarbon():
    h = read_sheet(SRC / "fosdb/Hydrocarbon library raw data.xlsx", 0)
    reads = ~h.description.astype(str).str.contains("fosmid", case=False)
    return {
        "scadc_metagenome": h[reads & (h.library == "SCADC")],
        "toldc_metagenome": h[reads & (h.library == "TOLDC")],
        "napdc_metagenome": h[reads & (h.library == "NAPDC")],
        "coalbed_metagenomes": h[reads & h.library.isin(["CO182", "CO183"])],
    }


def revio_2024():
    m = pd.read_csv(SRC / "chinook/revio_2024/metadata.csv", dtype=str, keep_default_na=False)
    return {
        "revio_2024_metagenomes": m[m.type == "Metagenome"],
        "revio_2024_isolates": m[m.type == "Isolate"],
        "revio_2024_fosmids": m[m.type == "FosmidLibrary"],
    }


def sharknado():
    d = SRC / "sharknado"
    jgi = d / "LineP/RawData/Joint_Genome_Institute"
    libraries = []
    for t in sorted((jgi / "Illumina_metagenomes").rglob("Metagenome_Report_Tables/Table_3_library_information.txt")):
        libraries.append(dict(line.split("=", 1) for line in t.read_text().splitlines() if "=" in line))
    plates = [pd.read_csv(f, dtype=str, keep_default_na=False, encoding="utf-8-sig")
              for f in sorted((jgi / "Illumina_iTags").glob("P26_*sample_data.csv"))]
    return {
        "saanich_metagenome": pd.read_csv(d / "Saanich/SaanichInlet_metagenome_SharknadoFile_map.csv", dtype=str,
                                          keep_default_na=False),
        "linep_metagenome": pd.DataFrame(libraries),
        "amp_linep_jgi_itags": pd.concat(plates, ignore_index=True),
        "amp_saanich_jgi_itags": read_tsv(d / "Saanich/RawData/Joint_Genome_Institute/Illumina_iTags/"
                                                "sample_name_metadata_updated_ALL.tsv", header=None),
        "amp_sakinaw_jgi_itags": read_sheet(d / "StratifiedLakes/Sakinaw/RawData/Joint_Genome_Institute/Illumina_iTags/"
                                                "Sakinaw_SZHZ_TWUN_name_map.xlsx", 0),
        "amp_powell_jgi_itags": read_sheet(d / "StratifiedLakes/Powell/RawData/Joint_Genome_Institute/Illumina_iTags/"
                                               "iTagsMarch2014/Powell_SHZH_name_map.xlsx", 0),
        "amp_knorr2013_jgi_itags": read_sheet(d / "Knorr_2013/RawData/Joint_Genome_Institute/Illumina_iTags/"
                                                  "Knorr_TBOC_TWUN_TUGS_name_map.xlsx", 0),
    }


# --- genomes -------------------------------------------------------------------------------

JGI_FIELD = re.compile(r"^(\S.*?)\s{2,}(.*)$")


def farmad_sags():
    d = SRC / "chinook/farmad_sags"
    files = pd.read_csv(d / "Globus_Download_692176_File_Manifest.csv", dtype=str, keep_default_na=False)
    sags = files[["Short Organism Name", "JGI Grouping ID", "Genome/Metagenome Name"]].drop_duplicates()
    info = []
    for t in sorted(d.glob("*/Metagenome_Report_Tables/Table_2_Sample_Information_*.txt")):
        row = {"Short Organism Name": t.parts[-3]}
        for line in t.read_text().splitlines()[1:]:
            m = JGI_FIELD.match(line)
            if m:
                row[m[1]] = m[2].strip()
            elif line.strip():
                row[line.strip()] = ""
        info.append(row)
    return {"farmad_sags": sags.merge(pd.DataFrame(info), on="Short Organism Name", how="left")}


# --- fosmids -------------------------------------------------------------------------------

FOSDB_SOURCE = {
    "LineP": "fosdb_linep", "Saanich": "fosdb_saanich", "WilliamsLakeLTSP": "fosdb_ltsp",
    "Hydrocarbon": "fosdb_hydrocarbon", "Marine Sponge": "fosdb_sponge", "Mining": "fosdb_mining",
    "Eel River Basin": "fosdb_eel_river", "Hogfuel Pile": "fosdb_hogfuel",
    "Lakelse hotsprings": "fosdb_lakelse", "Sakinaw": "fosdb_sakinaw", "SequencedFosmids": "fosdb_super",
}


def fosdb_dataset(r):
    """Same grouping as the manifest's fosdb_* rows."""
    if r.source == "?":
        return "fosdb_ltsp" if "/LTSP/" in str(r.sharknado_ends) else "fosdb_omz_unlabelled"
    if r.library_id == "ScaDC":
        return "scadc_fosmids"
    return FOSDB_SOURCE.get(r.source, "fosdb_beaver" if str(r.source).startswith("Beaver") else None)


def fosmids():
    j = read_sheet(SRC / "fosdb/FosDB Manifest.xlsx", "v1.JGI_2025-03-21").dropna(subset=["library_id"])
    j["library_id"] = j.library_id.astype(str).str.strip()
    inv = read_sheet(SRC / "fosdb/fosmid_library_inventory.xlsx", "libraries").dropna(subset=["library_id"])
    inv["library_id"] = inv.library_id.astype(str).str.strip()
    key = "_key"
    j[key], inv[key] = j.library_id.str.lower(), inv.library_id.str.lower()
    inv = inv.drop(columns="library_id")
    joined = j.merge(inv, on=key, how="left").drop(columns=key)
    groups = joined.apply(fosdb_dataset, axis=1)
    assert groups.notna().all(), joined[groups.isna()]
    return {did: g for did, g in joined.groupby(groups)}


# --- amplicons -----------------------------------------------------------------------------

def aspire_crosswalk():
    c = read_tsv(SRC / "aspire/sample_crosswalk.tsv")
    return {
        "amp_ab48_consortia": c[c.cohort.str.startswith("lab_") | (c.cohort == "ab48_historical")],
        "amp_purify_spirulina": c[c.cohort.str.startswith("purify_")],
        "amp_biofactorial": c[c.cohort == "patrik_2026_09_Biofactorial"],
    }


BUILDERS = [cami, metagem, single_sources, hydrocarbon, revio_2024, sharknado, farmad_sags, fosmids, aspire_crosswalk]
# Sources with no header row are written without one.
HEADERLESS = {"amp_saanich_jgi_itags"}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = read_tsv(ROOT / "data/engcyc/manifest.tsv")
    known = set(manifest.dataset_id)
    tables = {}
    for build in BUILDERS:
        for did, df in build().items():
            assert did in known, f"{build.__name__}: {did} is not a manifest dataset"
            assert did not in tables, f"{did} is built twice"
            tables[did] = df
    for did, df in sorted(tables.items()):
        df.to_csv(OUT / f"{did}.tsv", sep="\t", index=False, header=did not in HEADERLESS)
        print(f"{did}\t{len(df)}")
    for did in sorted(known - set(tables)):
        print(f"{did}\t-")
    manifest["metadata_table"] = [f"{d}.tsv" if d in tables else "" for d in manifest.dataset_id]
    manifest.to_csv(ROOT / "data/engcyc/manifest.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
