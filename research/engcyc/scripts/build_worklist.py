"""Write data/engcyc/worklist.tsv: one row per dataset still to move or manifest, with its final chinook folder.

A rerun keeps the date, centre, state and notes already recorded in the worklist, so the table stays the
place where each dataset's progress lives.
"""
import argparse
import csv
from pathlib import Path

RRD = "/Projects/Received_raw_data"
EXT = "/Projects/Resources/external_data"

# task, wave, dataset_id, final folder ("<date>" and "<Centre>" are filled per task in the worklist)
PLAN = """
T2	W1	farmad_boncat	unchanged
T2	W1	anaerobic_digester	unchanged, inside farmad_boncat
T3	W1	mycopots	unchanged
T4	W1	revio_2024_metagenomes	unchanged
T4	W1	revio_2024_isolates	unchanged
T4	W1	revio_2024_fosmids	unchanged
T5	W1	copper_enrichments	unchanged
T6	W1	amp_mine_enrichments	unchanged
T7	W1	gmcf3495_lung	unchanged
T8	W1	spanish_lakes_viromics	unchanged
T9	W1	spanish_lake_river	unchanged
T10	W1	farmad_sags	unchanged
T11	W1	ag_ad_hybrid	unchanged
T12	W1	juan_pacbio_revio	unchanged
T13	W1	seed_pacbio	unchanged
T14	W1	ad_nanopore_xuan	unchanged
T15	W1	amp_sakinaw_miseq	unchanged
T16	W1	ab48_revio	unchanged
T17	W1	amp_nostoc_anabaena	unchanged
T18	W1	amp_lung_spark	unchanged
T19	W1	amp_lung_lumins	unchanged
T20	W1	amp_lung_host_depletion	unchanged
T21	W1	cyanoverse	unchanged
T22	W1	chassis_cb2a	unchanged
T23	W1	cami2_marine	{EXT}/CAMI/
T23	W1	cami2_strain	{EXT}/CAMI/
T23	W1	cami2_plant_associated	{EXT}/CAMI/
T23	W1	cami2_toy_mousegut	{EXT}/CAMI/
T23	W1	cami2_toy_hmp_airskinurogenital	{EXT}/CAMI/
T23	W1	cami2_toy_hmp_gastrooral	{EXT}/CAMI/
T23	W1	cami3_toy_humangut	{EXT}/CAMI/
T24	W1	korem2015	{EXT}/metaGEM/korem2015/
T25	W1	karlsson2013	{EXT}/metaGEM/karlsson2013/
T26	W1	li2019	{EXT}/metaGEM/li2019/
T27	W1	sunagawa2015	{EXT}/metaGEM/sunagawa2015/
T28	W1	bissett_base	{EXT}/metaGEM/bissett_base/
T29	W1	pratama2026	{EXT}/Pratama_2026/pratama2026/
T32	W2	amp_ab48_consortia	{RRD}/Cyanogroup_16S/<date>_16S_<Centre>/
T33	W2	amp_purify_spirulina	{RRD}/Cyanogroup_16S/<date>_16S_<Centre>/
T34	W2	amp_biofactorial	{RRD}/Cyanogroup_16S/<date>_16S_<Centre>/
T35	W2	amp_cyano_prjna801777	{EXT}/ASPIRE_Tests/amp_cyano_prjna801777/
T36	W2	chassis_epi300	{EXT}/Host_Chassis/chassis_epi300/
T37	W2	scadc_metagenome	{RRD}/Hydrocarbon/<date>_MetaG_<Centre>/
T39	W2	amp_powell_jgi_itags	{RRD}/Stratified_Lakes/<date>_16S_iTags_JGI/
T40	W2	powell_metagenome	{RRD}/Stratified_Lakes/<date>_MetaG_JGI/; <date>_MetaG_SRA/
T41	W2	amp_hakai_microbiome_insights	{RRD}/Hakai/<date>_16S_MicrobiomeInsights/
T42	W2	amp_hakai_ucla	{RRD}/Hakai/<date>_16S_UCLAGenoSeq/
T43	W2	amp_linep_hakai	{RRD}/Line_P/<date>_16S18S_Hakai/
T44	W2	amp_dfe_biofuel	{RRD}/Biofuel_Experiments/<date>_16S_iTags_JGI/
T45	W2	amp_knorr2013_jgi_itags	{RRD}/South_Atlantic_Dark_Ocean_2013/<date>_16S_iTags_JGI/
T46	W2	amp_wastewater_lulu	{RRD}/Wastewater/<date>_16S_PharmaceuticalSciencesCentre/
T47	W2	amp_antamina_metamixis	{RRD}/Antamina_Mine/<date>_16S_<Centre>/
T48	W2	amp_oil_gas_wells	{RRD}/Hydrocarbon/<date>_16S_454Pyrotags_<Centre>/
T49	W2	etnp_mexico_metagenome	{RRD}/Oxygen_Minimum_Zones/<date>_MetaG_<Centre>_ETNP/
T50	W2	mediterranean_dcm_metagenome	{RRD}/Oxygen_Minimum_Zones/<date>_MetaG_<Centre>_Mediterranean/
T51	W2	ebpr_metagenome	{RRD}/Phosphorus_Removal_Bioreactor/<date>_MetaG_454_GenomeQuebec/; <date>_MetaG_GenomeQuebec/
T52	W2	rdx_metagenome	{RRD}/Demolition_Range/<date>_MetaG_454_GenomeQuebec/
T53	W2	beaver_gut_metagenome	{RRD}/Beaver_Gut/<date>_MetaG_<Centre>/
T54	W2	toldc_metagenome	{RRD}/Hydrocarbon/<date>_MetaG_<Centre>_TolDC/
T55	W2	coalbed_metagenomes	{RRD}/Hydrocarbon/<date>_MetaG_<Centre>_CoalBed/
T56	W2	amp_saanich_2008_2012	{RRD}/Saanich_Inlet/<date>_16S_<Centre>/
T57	W2	collaborations_metagenome	{RRD}/Collaborations/<date>_MetaG_JuniperLab/; <date>_MetaG_LeanderLab/; <date>_MetaG_CroweLab/
T58	W2	singlecell_anme_singular	{RRD}/Methanotroph_Single_Cells/<date>_SAG_GenomeSciencesCentre/; <date>_SAG_SingularGenomics/
T59	W2	napdc_metagenome	{RRD}/Hydrocarbon/<date>_MetaG_<Centre>_NapDC/
T61	W3	amp_linep_jgi_itags	{RRD}/Line_P/<date>_16S_iTags_JGI/
T62	W3	kivu_metagenome	{RRD}/Stratified_Lakes/<date>_MetaG_<Centre>_Kivu/; <date>_MetaG_<Centre>_KabunoBay/
T63	W3	malili_metagenome	{RRD}/Stratified_Lakes/<date>_MetaG_<Centre>_Matano/; _Towuti/; _TowutiDrillCore/
T64	W3	ligninase_metagenome	{RRD}/Lignin/<date>_MetaG_<Centre>_LigninaseScreen/; _InSitu/
T65	W3	ltsp_williams_metagenome	{RRD}/Long_Term_Soil_Productivity/<date>_MetaG_GenomeSciencesCentre_WilliamsLake/
T66	W3	ltsp_oconnor_metagenome	{RRD}/Long_Term_Soil_Productivity/<date>_MetaG_GenomeQuebec_OConnorLake/; <date>_MetaG_GenomeSciencesCentre_OConnorLake/
T67	W3	knorr2013_metagenome	{RRD}/South_Atlantic_Dark_Ocean_2013/<date>_MetaG_JGI/; <date>_MetaT_JGI/
T71	W4	fosdb_sakinaw	{RRD}/Fosmids/Sakinaw_Lake/<date>_FosmidPools_<Centre>/
T72	W4	fosdb_linep	{RRD}/Fosmids/Line_P/<date>_FosmidFull_JGI/; <date>_FosmidEnds_GenomeSciencesCentre/
T73	W4	fosdb_saanich	{RRD}/Fosmids/Saanich_Inlet/<date>_FosmidEnds_GenomeSciencesCentre/
T74	W4	fosdb_omz_unlabelled	{RRD}/Fosmids/Oxygen_Minimum_Zones_Unlabelled/<date>_FosmidEnds_GenomeSciencesCentre/
T75	W4	fosdb_hydrocarbon	{RRD}/Fosmids/Hydrocarbon/<date>_FosmidEnds_GenomeSciencesCentre/; _FosmidFull/
T76	W4	scadc_fosmids	{RRD}/Fosmids/Hydrocarbon_ScaDC/<date>_FosmidEnds_GenomeSciencesCentre/
T77	W4	fosdb_ltsp	{RRD}/Fosmids/Long_Term_Soil_Productivity/<date>_FosmidEnds_GenomeSciencesCentre/
T78	W4	fosdb_mining	{RRD}/Fosmids/Mining_Bioreactor/<date>_FosmidFull_GenomeSciencesCentre/; _FosmidEnds_GenomeSciencesCentre/
T79	W4	fosdb_sponge	{RRD}/Fosmids/Sponge/<date>_FosmidEnds_GenomeSciencesCentre/
T80	W4	fosdb_hogfuel	{RRD}/Fosmids/Hog_Fuel/2019-05-16_FosmidPools_SeqWell/
T81	W4	fosdb_lakelse	{RRD}/Fosmids/Lakelse_Lake/2021-01-05_FosmidPools_SeqWell/
T82	W4	paper_sludge	{RRD}/Fosmids/Paper_Sludge/<date>_FosmidPools_<Centre>/
T83	W4	fosdb_beaver	{RRD}/Fosmids/Beaver_Gut/<date>_FosmidEnds_<Centre>/
T84	W4	fosdb_eel_river	{RRD}/Fosmids/Eel_River/<date>_FosmidEnds_<Centre>/
T85	W4	biofactorial_pilot	{RRD}/Fosmids/Biofactorial/2026-09-25_FosmidPools_HallamLab/
T86	W4	biofactorial_mini	{RRD}/Fosmids/Biofactorial/2026-09-28_FosmidPools_HallamLab/
T88	W5	hakai_metagenome	{RRD}/Hakai/<date>_MetaG_JGI/; <date>_MetaG_GenomeQuebec/; <date>_MetaG_<Centre>_Seagrass/
T89	W5	etsp_chile	{RRD}/Oxygen_Minimum_Zones/<date>_SAG_GenomeSciencesCentre_ETSP/; <date>_SAG_Bigelow_ETSP/; <date>_MetaG_<Centre>_ETSP/
T90	W5	amp_saanich_jgi_itags	{RRD}/Saanich_Inlet/<date>_16S_iTags_JGI/
T91	W5	linep_metagenome	{RRD}/Line_P/<date>_MetaG_JGI/
T92	W5	methanome_metagenome	{RRD}/Methanome/<date>_MetaG_<Centre>/
T93	W5	hydrocarbon_survey_metagenome	{RRD}/Hydrocarbon/<date>_MetaG_<Centre>_Survey/; <date>_MetaG_454_GenomeQuebec/
T94	W5	saanich_metagenome	{RRD}/Saanich_Inlet/<date>_MetaG_JGI/
T95	W5	omz_sags	{RRD}/Oxygen_Minimum_Zones/<date>_SAG_<Centre>/; <date>_SAG_<Centre>_GlobalSurvey/; <date>_SAG_<Centre>_SUP05/
"""

COLUMNS = ["task", "wave", "dataset_id", "status", "now", "source_bytes", "gz_bytes", "uncompressed_read_bytes",
           "final_folder", "date", "centre", "state", "notes"]
KEPT = ["date", "centre", "state", "notes"]


def read_tsv(path):
    if not path.exists():
        return {}
    with open(path) as fh:
        return {r["dataset_id"]: r for r in csv.DictReader(fh, delimiter="\t")}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", type=Path, default=Path("data/engcyc/manifest.tsv"))
    ap.add_argument("--sizes", type=Path, default=Path("data/engcyc/sources/worker01/sharknado_sizes_2026-11-06.tsv"))
    ap.add_argument("--out", type=Path, default=Path("data/engcyc/worklist.tsv"))
    args = ap.parse_args()
    manifest = read_tsv(args.manifest)
    sizes = read_tsv(args.sizes)
    previous = read_tsv(args.out)
    rows = []
    for line in PLAN.strip().splitlines():
        task, wave, dataset_id, final = line.split("\t")
        m = manifest[dataset_id]
        s = sizes.get(dataset_id, {})
        row = {
            "task": task, "wave": wave, "dataset_id": dataset_id, "status": m["status"],
            "now": s.get("location") or m["locations"],
            "source_bytes": s.get("bytes") or m["size_bytes"],
            "gz_bytes": s.get("gz_bytes", ""), "uncompressed_read_bytes": s.get("uncompressed_read_bytes", ""),
            "final_folder": final.format(RRD=RRD, EXT=EXT),
            "date": "", "centre": "", "state": "pending", "notes": "",
        }
        row.update({k: previous[dataset_id][k] for k in KEPT if dataset_id in previous})
        rows.append(row)
    with open(args.out, "w", newline="") as out:
        w = csv.DictWriter(out, COLUMNS, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
