# E. coli intracellular metabolite concentration survey

Purpose: one physiological absolute intracellular concentration per metabolite, for a
reaction-quotient term in a bioinformatics reference build. Contents: one entry per source
considered, each stating its overlap with ECMDB and what it adds, and the rule that kept or
refused it. The two sources this kept are pinned under `data/fabfos/originals/`. The
downloads the rest produced were deleted, so an entry here is the only record of them.
Written before r10. Read it as a dated survey, not as a description of the shipped lane.

Baseline to beat: **ECMDB 2.0** — 1,186 growth-condition measurements over **891 metabolites**,
already downloaded elsewhere. Every entry below states overlap-with-ECMDB and what it adds.

Rejection rules applied: extracellular/biofluid pools rejected; relative / fold-change data
rejected; non-E.-coli organisms discarded.

---

## Per-resource entries

### 1. Metabolomics Workbench — REST API — **MARGINAL KEEP (3 studies), mostly REJECT**

- **Canonical URL** `https://www.metabolomicsworkbench.org/` · REST base `https://www.metabolomicsworkbench.org/rest/`
- **Commands that worked**
  ```
  curl -s "https://www.metabolomicsworkbench.org/rest/study/study_id/ST/summary" -o mw_all_studies_summary.json   # ALL 4,500 studies, 2.6 MB
  curl -s "https://www.metabolomicsworkbench.org/rest/study/study_id/<ST>/analysis"    # carries a `units` field — the cheap absolute-vs-relative probe
  curl -s "https://www.metabolomicsworkbench.org/rest/study/study_id/<ST>/mwtab/txt"   # full study incl. data matrix
  ```
  `study_title/coli/summary` is a trap: it string-matches "colitis" and returns human/mouse studies. Filter
  `mw_all_studies_summary.json` on `species` instead.
- **Access** REST API, no key, no login. 1 s sleep between the 58 per-study calls; no rate limiting hit.
- **Licence** every one of the 58 E. coli studies is **CC BY 4.0** (`license` field in the summary). Redistribution in a
  git repo is permitted with attribution.
- **Format / size** JSON + mwtab text. 3.0 MB downloaded total.
- **E. coli subset** 58 of 4,500 studies have `species` containing "Escherichia". Selector: `mw_ecoli_studies.json`,
  study IDs in `ecoli_study_ids.txt`.
- **Absolute vs relative — this is where it dies.** Units histogram over all 58 E. coli studies' analyses:
  `peak area` ×23, `counts`/`abundance`/`Abundance (Counts)` ×14, `Relative abundance` ×4, `-`/`N/A`/`no applicable
  unites` ×12, `retention time` ×2, `Log2 fold change` ×1. **Only three studies carry a real absolute intracellular
  unit**, and one further pair carries mM-in-the-NMR-tube.
  - `ST002419` declares `mmol/L` but its values are raw ion counts (xylose 4570, valine 135562). **Mislabelled unit —
    rejected as relative.**
  - `ST001394` `Height ratio/gDCW` is a ratio to an internal standard. **Relative — rejected.**
  - `ST001862` is µM but of *culture supernatant* sugars from a mixed Bacteroides/Escherichia community.
    **Extracellular and not pure E. coli — rejected.**
- **KEEPERS**

  | Study | Unit | Strain / condition | Distinct metabolites |
  |---|---|---|---|
  | `ST001421` | `umol/gDCW` | K-12 MG1655 WT + *fnr*, *arcA*, *ihf*; glucose minimal, **anaerobic** fermentation, exponential, 37 °C bioreactor | 33 |
  | `ST002076` | `umol/gDCW` | K-12 MG1655 WT + *fnr arcA*, *arcA ihf*, *fnr ihf* doubles; same condition | 34 (WT columns are the *same batch* as ST001421 — do not double-count) |
  | `ST000173` | `pmol/µg protein` | EcHW24 WT + isobutanol-tolerant 38-20-4, NG50 minimal ± 0.7 % isobutanol | 17 |
  | `ST004542` | `mM` (NMR) | BW25113 WT + *glnP*, ± benzoic acid; boiled-water extract | 68 — **mM in the extract tube, not in the cytoplasm**; the dilution factor is not reported, so it is unusable as a physiological concentration |
  | `ST001351` | `mM` (NMR) | WT + antimicrobial-resistant mutant, stationary & mid-log | 31 — same defect |

- **Distinct E. coli intracellular metabolites carrying a genuinely absolute concentration: 46 raw name strings,
  ≈ 40 distinct chemicals** after collapsing spelling variants (`Fructose 1_6-bisphosphate` vs
  `Fructose-1,6-bisphosphate`) and ambiguous pooled channels (`Citrate or Isocitrate`,
  `Glucose 6-phosphate or fructose-6-phosphate`, `2 or 3-phosphoglycerate_2PG/3PG` — three channels that name two
  compounds each and cannot be assigned).
- **Identifier namespaces: NAMES ONLY.** The `metabolites` endpoint returns exactly two fields per compound —
  `metabolite_name` and `refmet_name`. Real row:
  ```json
  {"study_id":"ST000117","analysis_id":"AN000198","metabolite_name":"3-phosphoglycerate","refmet_name":"3-Phosphoglyceric acid"}
  ```
  No KEGG, no ChEBI, no HMDB, no InChIKey, no PubChem in the study record. RefMet names can be resolved one at a
  time via `/rest/refmet/name/<name>/all`, which yields `pubchem_cid` + `inchi_key` — but **not** KEGG or ChEBI,
  which are the two prefixes the join used at survey time. So MW joins to MetaNetX only through a
  second hop (name → RefMet → InChIKey → `chem_prop`), and every ambiguous channel above breaks it.
- **Growth conditions** yes, and good ones: strain, genotype, medium, aerobic/anaerobic, growth phase, temperature,
  and per-sample `Factors` strings.
- **Overlap with ECMDB** ≈ **100 %**. All 40 are central-carbon / amino-acid / nucleotide-cofactor compounds that
  ECMDB's 891 already carry. Net new metabolites: **~0**.
- **Verdict** the *only* thing MW adds is an **anaerobic** glucose-minimal condition (ST001421/ST002076), which
  Bennett/Park do not cover. If the concentration table ever wants an anaerobic column, pin those two. Otherwise
  delete. Not worth a name-matching join for zero new compounds.

### 2. Park et al. 2016 (Nat Chem Biol) — GitHub companion repo — **TOP KEEP**

- **Paper** PMID **27159581**, PMCID **PMC4912430**, DOI 10.1038/nchembio.2077.
  "Metabolite concentrations, fluxes and free energies imply efficient enzyme usage."
- **Where the machine-readable data actually is** NOT in the supplement. The paper's full text cites
  `https://github.com/PrincetonUniversity/flux-ratio-based-gibbs-energy`, and that repo carries the numbers.
- **Command that worked**
  ```
  curl -sL "https://codeload.github.com/PrincetonUniversity/flux-ratio-based-gibbs-energy/tar.gz/refs/heads/master" \
       -o park2016/github_flux_ratio.tar.gz          # 47 MB
  tar xzf github_flux_ratio.tar.gz
  # concentrations live in flux-ratio-based-gibbs-energy-master/ecoli/ecoli_integrate.mat
  ```
  Extracted to TSV with `scipy.io.loadmat` — see `park2016_ecoli_iAF1260_concentrations.tsv`.
- **Access** bulk file (git tarball). No login, no rate limit.
- **Licence** **MIT** (`LICENSE`, © 2015 Princeton University). **Redistribution inside a git repo is explicitly
  permitted.** This is the only high-value source in this survey with an unambiguously permissive licence.
- **Format / size** MATLAB v5 `.mat` (14 MB) inside a 47 MB tarball; extracted tree 71 MB. Derived TSV 1668 rows.
- **Multi-organism — how to select E. coli.** The repo separates organisms **by directory**: `ecoli/`,
  `scerevisiae/`, `mammalia/`. Take `ecoli/` and nothing else. Inside it, the model is **iAF1260**
  (`description = "../ecoli/iAF1260.xml"`), and the compartment suffix separates the pools:
  `metCompartments ∈ {c, e, p}` — **take `c` only** (`e` = extracellular, `p` = periplasm).
- **Numbers.** 1,668 model metabolites. **217 carry a measured concentration** (`metConc_mea` column 1 non-NaN),
  split 105 cytoplasmic / 56 extracellular / 56 periplasmic. **→ 105 distinct E. coli cytoplasmic metabolites with an
  absolute measured concentration**, of which `h2o[c]` and `h[c]` are the activity-1 convention and must be dropped:
  **103 real measurements**, matching the paper's own claim of "103 metabolites in E. coli".
- **Absolute?** **Yes**, in **molar (M)**. Two distinct columns, and confusing them would be a mistake:
  - `metConc_mea[:,0]` = the **measurement** (NaN for 1,451 of 1,668 metabolites); cols 1–2 are its LB/UB.
  - `metConc[:,0]` = the **model-fitted** concentration with `[:,1:3]` LB/UB, populated for **951** cytoplasmic
    metabolites. Most of those are the optimiser sitting inside the default `[0, 0.1] M` box and are **not
    measurements** — using them as physiological concentrations would launder a bound into a datum.
- **Identifier namespaces: BiGG ONLY.** The model has `metKEGGID`, `metChEBIID`, `metPubChemID`, `metInChIString`
  fields and **all four are empty for all 1,668 rows** (MATLAB `[]`). What you get is the BiGG (iAF1260) metabolite
  id plus a display name. Real rows:
  ```
  bigg_met  name                         compartment  kegg  chebi  conc_measured_M  conc_fit_M  fit_lb   fit_ub
  glu-L[c]  L-Glutamate                  c            (empty)      0.096            0.09513     0.0924   0.0998
  fdp[c]    D-Fructose 1,6-bisphosphate  c            (empty)      0.0152           0.014       0.014    0.0164
  atp[c]    ATP                          c            (empty)      0.00963          0.009465    0.00813  0.0114
  ```
  So the join is **`bigg.metabolite:` into MetaNetX `chem_xref`**, after stripping the `[c]` suffix. That is a
  *third* namespace beyond the `kegg.compound:` / `chebi:` pair the join used at survey time.
  The bundled `ecoli.xml` is a 13CFLUX carbon-mapping file with zero KEGG/ChEBI annotation, and `ecoli_mea.xlsx`
  holds labeling fractions and fluxes, not concentrations — neither rescues the crosswalk.
- **Growth conditions** single condition, stated in Online Methods: **E. coli K-12 NCM3722, 37 °C, Gutnick minimal
  medium + 10 mM NH₄Cl + 0.4 % (w/v) D-glucose, exponential phase (OD₆₀₀ ≈ 0.3)**, vacuum-filtered onto nylon and
  quenched in −20 °C 40:40:20 acetonitrile:methanol:water. The model also pins the physical state: **T = 310.15 K,
  cytoplasmic pH 7.7**, periplasmic/extracellular pH 7.2, membrane potential 0/90/90 mV. Not a condition panel —
  one condition, deeply characterised.
- **Overlap with ECMDB** near-total, and *by construction*: the paper says the E. coli concentrations are taken from
  its ref 1 = **Bennett et al. 2009 (PMID 19561621)**, which is the PMID on ECMDB's own rows. Park does **not** add
  new E. coli measurements. **Net new metabolites over ECMDB: ≈ 0.**
- **What it does add, and why it is still the top keep**: (a) BiGG ids on the same numbers ECMDB gives you as
  names/KEGG — a second, independent join path into MetaNetX; (b) an explicit **cytoplasmic pH (7.7)**, temperature
  and ionic strength for the exact condition the concentrations belong to; (c) fitted concentrations *with
  thermodynamically consistent bounds*, which is a defensible prior for the ~850 metabolites nobody measured;
  (d) an MIT licence, so it can be pinned into the repo without a permissions argument.
- **Supplement route failed and was abandoned**: `europepmc .../PMC4912430/supplementaryFiles` returns **404**
  (that endpoint only serves CC-licensed articles; this is a NIHMS author manuscript, `license="none"`).
  `ftp.ncbi.nlm.nih.gov/pub/pmc/oa_package/...` paths from `oa.fcgi` are stale (404 over both https and ftp), and
  `pmc.ncbi.nlm.nih.gov/articles/.../bin/...` serves a **Google reCAPTCHA challenge**. Springer's
  `static-content.springer.com/esm/art%3A10.1038%2Fnchembio.2077/...` returns **403**. The supplement is a PDF
  anyway (`NIHMS767915-supplement-1.pdf`) — not machine-readable. **Manual click-through only; not pursued.**

### 3. Bennett et al. 2009 (Nat Chem Biol) — **ALREADY HELD VIA ECMDB; no separate download possible**

- **Paper** PMID **19561621**, PMCID **PMC2754216**, "Absolute metabolite concentrations and implied enzyme active
  site occupancy in Escherichia coli."
- **Status** this is *the* canonical E. coli absolute-concentration dataset, and it is **already in hand**: ECMDB's
  scraped rows cite `PMID: 19561621`, which the aggregation already prefers. Re-downloading
  it buys nothing.
- **Supplementary files are `NIHMS109101-supplement-1.pdf` and `NIHMS109101-supplement-2.doc`** (from the EPMC
  `fullTextXML` `xlink:href` list). A **PDF and a Word document — not machine-readable tables.**
- **Every automated route is blocked**, same as Park: EPMC `supplementaryFiles` → 404 (NIHMS manuscript);
  PMC OA `.tar.gz` → 404; `pmc.ncbi.nlm.nih.gov/.../bin/<file>` → **reCAPTCHA challenge page** returned as a 200.
  **Manual click-through only. Not circumvented, not pursued.** Directory left empty deliberately.
- **Absolute?** Yes — µM/mM intracellular, K-12 NCM3722, Gutnick minimal, glucose/glycerol/acetate, mid-log.
- **Overlap with ECMDB** 100 %. It *is* a large part of ECMDB.

### 4. MetaboLights (EBI) — **REJECT for concentrations; best identifier hygiene of any source surveyed**

- **Canonical URL** `https://www.ebi.ac.uk/metabolights/`
- **Commands that worked**
  ```
  curl -s "https://www.ebi.ac.uk/metabolights/ws/studies" -o ml_all_studies.json        # 3,316 public accessions
  curl -sG --data-urlencode "query=Escherichia coli" --data-urlencode "fields=id,name,organism,technology_type" \
       "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights?format=json&size=100&start=<n>"   # 7 pages, 1 s apart
  curl -s "https://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public/<MTBLSid>/"         # plain HTML dir listing
  curl -s "https://ftp.ebi.ac.uk/pub/databases/metabolights/studies/public/<MTBLSid>/<file>"   # ISA-Tab + MAF
  ```
  The MetaboLights `ws/studies` endpoint returns accessions only. Organism search goes through **EBI Search**, not
  the MetaboLights WS.
- **Access** REST + open FTP-over-HTTPS. No login for public studies. No rate limiting hit at 1 req/s.
- **Licence** MetaboLights public studies are **CC0** by submission terms. Redistributable.
- **Format / size** ISA-Tab (`i_Investigation.txt`, `s_*.txt`, `a_*.txt`) + MAF (`m_*_maf.tsv`). 6.1 MB downloaded
  across 3 studies.
- **E. coli subset** 696 free-text hits; **627 studies whose `organism` field names *Escherichia***. Accession list
  in `ml_ecoli_candidates.json`. Most are E. coli-as-expression-host, not E. coli physiology.
- **The structural problem, and it is fatal.** The MAF format **has no units column**. Its data columns are
  `smallmolecule_abundance_sub`, `smallmolecule_abundance_stdev_sub`, `smallmolecule_abundance_std_error_sub` plus
  one column per sample. Units, when stated at all, are free prose inside `i_Investigation.txt`. **MetaboLights
  therefore cannot be filtered for absolute quantification by any query** — you must open every study and read its
  protocol. That is the reason 627 candidates could not be swept exhaustively here.
- **Studies actually pulled and adjudicated**

  | Study | What it is | Values | Verdict |
  |---|---|---|---|
  | `MTBLS4249` | Radoš, Donati, Lempp, Rapp, **Link** 2022 iScience — *Homeostasis of the biosynthetic E. coli metabolome*. 101 primary metabolites × **19 conditions** (BW25113/MG1655/NCM3722, M9, 11 carbon sources, pH 6, 42 °C, NaCl, synthetic rich, stationary 1 d & 3 d) | ATP = 0.88–1.11, FBP = 0.08, citrate = 1.79 — these are **¹²C/¹³C peak-area ratios** against a U-¹³C E. coli internal standard (Guder/Link isotope-ratio LC-MS/MS), not molar | **RELATIVE — rejected** |
  | `MTBLS2795` | Bershtein lab, MSB 2021, DHFR point mutants | ATP = 141,000 — raw ion counts | **RELATIVE — rejected** |
  | `MTBLS2188` | Fluxotype of the E. coli y-ome, Metabolites 2021 | 16 + 3 metabolites, exometabolome for flux fitting | **rejected** (extracellular, tiny) |

- **Identifier namespaces — this is what MetaboLights is good for.** Every MAF carries, as its first five columns:
  `database_identifier`, `chemical_formula`, `smiles`, `inchi`, `metabolite_identification`. In all three studies
  pulled, `database_identifier` is **100 % ChEBI**. Real row (`MTBLS4249`, acidic MAF):
  ```
  database_identifier  chemical_formula  metabolite_identification  Glu_BW25113_1
  CHEBI:15422          C10H16N5O13P3     ATP                        0.882700833
  CHEBI:37736          C6H14O12P2        FBP                        0.08037715
  ```
  ChEBI + full InChI + SMILES is strictly the best-annotated source in this survey — `chebi:` is already one of the
  two prefixes the join used at survey time. **The identifiers are excellent and the numbers are
  worthless.** If MetaboLights ever deposits a molar E. coli study, it will join for free.
- **Growth conditions** yes, and unusually rich — `s_*.txt` carries per-sample factor values, and `i_Investigation.txt`
  carries strain, medium, temperature, growth phase and growth rate.
- **Overlap with ECMDB** the 101 metabolites of MTBLS4249 are primary metabolism — essentially inside ECMDB's 891.
- **Verdict** **delete the downloads, keep the finding.** Nothing molar. Keep `MTBLS4249`'s ChEBI list only if you
  want a reference vocabulary.

### 5. Radoš et al. 2022 (iScience) supplementary — **REJECT (fold-change), but see the derived option**

- **Paper** PMID **35754712**, PMCID **PMC9218372**, DOI 10.1016/j.isci.2022.104503. Licence **CC BY-NC-ND** —
  redistributable with attribution, **no derivatives and non-commercial**, which is a real constraint if the table
  is transformed and shipped.
- **Command that worked** (the EPMC endpoint the brief suggested — it *does* work, but only for CC-licensed articles)
  ```
  curl -sL "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC9218372/supplementaryFiles" -o PMC9218372_suppl.zip
  ```
  2.65 MB zip → `mmc1.pdf`, `mmc2.xlsx` (Table S1, conditions), `mmc3.xlsx` (Table S2, the data), plus figures.
- **Table S1** (`mmc2.xlsx`, 33×12) — the condition table: strain, medium, carbon source, pH, temperature, growth
  phase, growth rate per condition. Genuinely useful metadata.
- **Table S2** (`mmc3.xlsx`, 106×42) — **103 metabolites × 21 columns of "Fold change relative to glucose" and 21
  columns of standard deviation. There is no absolute column anywhere in the file.** Real row:
  ```
  KEGG identifier  Short Name  Metabolite name  Gluc_BW  Pyr       Fum        St3
  C00149           mal         Malate           1        0.70986   12.40864   0.041658
  ```
  **RELATIVE — rejected as a concentration source**, exactly per the brief.
- **Identifier namespace: KEGG compound, first column, clean `C#####`.** The single most joinable identifier column
  in this whole survey, attached to the single least usable set of numbers.
- **The derived option worth stating.** The `Gluc_BW` column is 1 by definition — every other column is a ratio to
  E. coli BW25113 on M9 glucose, exponential. Bennett/ECMDB give an *absolute* glucose-minimal exponential
  concentration for most of these same compounds, keyed by KEGG. Multiplying ECMDB's glucose anchor by this fold
  change would yield **absolute concentrations across 19 conditions including 11 carbon sources, acid, salt, heat
  and two stationary-phase timepoints** — a condition panel nothing else in this survey offers, joined purely on
  `kegg.compound:`. That is a derivation, not a measurement, and it would need to be labelled as one; the CC BY-NC-ND
  "no derivatives" term is also a question for whoever pins it. Flagging it, not doing it.

### 6. Gerosa et al. 2015 (Cell Systems) — Elsevier supplementary — **TOP KEEP, the only real condition panel**

- **Paper** PMID **27136056**, DOI 10.1016/j.cels.2015.09.008, PII **S2405471215001465**. Gerosa, van Rijsewijk,
  Christodoulou, Zampieri, Mannhardt, Sauer — "Pseudo-transition Analysis Identifies the Key Regulators of Dynamic
  Metabolic Adaptations from Steady-State Data."
- **Not in Europe PMC** (`isOpenAccess = N`, no PMCID, `supplementaryFiles` unavailable). Found instead through
  Crossref, which gives the Elsevier PII, and the PII gives the CDN path:
  ```
  curl -s "https://api.crossref.org/works/10.1016/j.cels.2015.09.008"        # -> alternative-id S2405471215001465
  curl -sL -A "Mozilla/5.0" "https://ars.els-cdn.com/content/image/1-s2.0-S2405471215001465-mmc2.xlsx" -o mmc2.xlsx
  # mmc1.pdf mmc2.xlsx mmc3.xlsx mmc4.xlsx mmc5.pdf mmc6.pdf all return 200; no login, no paywall on the CDN
  ```
- **Access** direct file download off the Elsevier CDN. No login. **This route works where Springer's (Park) 403s.**
- **Licence** Crossref reports `elsevier.com/open-access/userlicense/1.0/` — the Elsevier Open Access user licence:
  attribution required, **non-commercial**, text-and-data-mining permitted. Redistribution inside a git repo is
  permissible for non-commercial use but is *not* a blanket permissive licence like Park's MIT. Flag before pinning.
- **Format / size** 3 xlsx + 3 pdf, **4.9 MB**. Read with `mamba run -n msm python` (openpyxl; the `ecspr` env has
  no xlsx reader). Derived TSV: `gerosa2015_ecoli_concentrations.tsv`, 342 rows.
- **E. coli only** — single organism, no filtering needed.
- **Absolute? YES**, and it ships its own unit conversion. `mmc2.xlsx` sheet **"Metabolite concentrations"**
  (47×20) is **µmol · gCDW⁻¹** with a matching standard-deviation block. The **"Conversion Factors"** sheet gives
  **2.3 mL intracellular volume per gCDW (all conditions)** plus per-condition gCDW·OD₆₀₀⁻¹·L⁻¹ factors. So
  `mM = (µmol/gCDW) / 2.3`, published by the authors rather than assumed. The derived TSV carries both columns.
- **Number of distinct E. coli intracellular metabolites: 43**, each measured in **all 8 conditions** (342
  non-missing values). Two are not clean single compounds: `2pg+3pg` is a pooled channel naming two metabolites and
  cannot be assigned; `cAMP` is spelled off-BiGG (`camp`).
- **Identifier namespaces: BiGG SHORT NAMES ONLY.** `mmc4.xlsx` sheet `metabolites` declares columns
  `Metabolite KEGGID`, `Metabolite PubChemID`, `Metabolite CheBI ID`, `Metabolite Inchi String`, `Metabolite Smile`
  — and **all five are empty for all 80 rows**. Only `Metabolite name` (`13dpg[c]`, `mal-L[c]`) and a description are
  populated. Real row from the concentration table:
  ```
  bigg_short  name                          carbon_source  conc_umol_per_gCDW  conc_mM   sd_umol_per_gCDW
  fdp         D-Fructose-1-6-bisphosphate   Glucose        2.6702              1.1610    (from the SD block)
  g6p         D-Glucose-6-phosphate         Glucose        2.1831              0.9492
  atp         Adenosine triphosphate        Glucose        7.9330              3.4491
  ```
  Join is `bigg.metabolite:` into MetaNetX — **the same third namespace Park needs**, which is the one useful
  consequence of two independent sources having the same gap.
- **Growth conditions — the reason to keep it.** **Eight carbon sources**: acetate, fructose, galactose, glucose,
  glycerol, gluconate, pyruvate, succinate. E. coli BW25113, M9 minimal, aerobic batch, exponential phase.
  `mmc2.xlsx` sheet `Physiology` carries growth rate and uptake/secretion rate per condition; sheet
  `Thermodynamic potentials (ΔG)` carries per-reaction ΔG for 70 reactions per condition — directly comparable to
  what the direction lane computes. `mmc3.xlsx` adds a **time-resolved** series (20 metabolites × 19 timepoints,
  same µmol/gCDW units) across a diauxic transition.
- **Overlap with ECMDB** high but not total. All 43 are central carbon / energy / a few amino acids, so ECMDB's 891
  almost certainly covers them by identity. **Net new metabolites: ≈ 0. Net new *conditions*: 8, and that is the
  contribution** — ECMDB's spread comes from a handful of studies at glucose/glycerol/acetate; Gerosa measures the
  same compounds on eight carbon sources with a consistent method, which is exactly the width the concentration
  correction needs.
- **Cross-check against Bennett/ECMDB, stated plainly**: Gerosa's glucose ATP is **3.45 mM**, Bennett's is
  **9.6 mM**. Same metabolite, same nominal condition, factor of ~2.8 apart. Whichever way that is resolved, it is
  evidence that the *width* on a concentration is method-dependent and not merely condition-dependent.

### 7. BioNumbers — targeted scrape for the quantities no metabolomics DB holds — **KEEP (small, hand-curated)**

- **Canonical URL** `https://bionumbers.hms.harvard.edu/`
- **Commands that worked**
  ```
  https://bionumbers.hms.harvard.edu/search.aspx?task=searchbytrmorg&trm=<query>&pi=<page>   # 20 rows/page, server-side
  https://bionumbers.hms.harvard.edu/bionumber.aspx?id=<BNID>                                # the record
  ```
  **Pagination is `&pi=<n>`** — not documented on the page, recovered from the "next" button's href. Scraped at
  1.5 s/request; no rate limiting or blocking encountered.
- **Access** paginated HTML scrape only. No API, no bulk file.
- **Licence** not stated on the site; BioNumbers is an academic aggregator of *published* values. Every row carries a
  full citation, so the defensible form is to cite the BNID and the underlying paper, not to redistribute the table.
- **Format / size** raw search HTML + record HTML, **1.6 MB**. Two derived tables:
  `bionumbers_ecoli_physicochemical.tsv` — **175 E. coli / generic rows** from 42 targeted queries;
  `bionumbers_curated.tsv` — **37 hand-picked records** with full value, organism, citation and BNID.
- **Absolute?** Yes for the concentrations. The redox entries are potentials (mV) and ratios (unitless), which is the
  correct form for a poise.
- **Identifier namespaces: NONE.** Confirmed exactly as the brief said — description free text plus an organism
  string, no chemical identifier of any kind. **Machine joining is impossible; every row here is a hand-placed
  constant.** That is acceptable *because* these are single physicochemical constants, not a metabolite table.
- **What was captured, by quantity**

  | Quantity | BNIDs and values |
  |---|---|
  | **Cytoplasmic pH** | `105980` 7.5–7.6 · `107037` **7.85 ± 0.05** · `111346` 7.98 ± 0.18 · `106518` 7.2–7.8 (E. coli in gut, external pH 4.5–9) · `105982` 6.0 (after HCl shock) · `103387` ΔpH across membrane 0.18–0.3 |
  | **Dissolved O₂** | `108724` **0.264 mM — O₂ solubility in M9 at 37 °C** (the one to use) · `109182` ~200 µM air-saturated buffer 37 °C · `111065` 7 µM ≙ 0.6 % air · `111098` 24 µM ≙ 2 % air |
  | **Dissolved CO₂** | **no direct E. coli cytoplasmic value exists.** Nearest: `106223` 9.27 mmol·m⁻³ (= 9.27 µM) in freshwater at 35 Pa gas-phase CO₂ · `106206`/`106207`/`106208` Lange's solubility tables (0.759 v/v at 25 °C, 0.592 at 35 °C). **This is a real gap and must be computed from Henry's law + cytoplasmic pH, not looked up.** |
  | **NAD couple** | `104681` NAD⁺ **2.6 mM** · `104682` NADH **83 µM** · `105022` NAD⁺/NADH = **19** (Henry 2007 TMFA) · `101187` NAD 1256–2078 µM (Albe 1990, calculated not measured) · `105427` full table (Andersen & von Meyenburg) |
  | **NADP couple** | `104689` NADPH **120 µM** · `104692` NADP⁺ **2.1 µM** · `105023` NADP⁺/NADPH = **1.2** · `108045` NADPH/NADP⁺ ≈ 0.4. **These disagree**: 120/2.1 gives NADP⁺/NADPH ≈ 0.018, versus 1.2 and 0.4 from the other two. Pick one and say which. |
  | **Glutathione** | `104680` GSH **17 mM** · `104683` GSSG **2.4 mM** · `111453` E°′(GSSG/2GSH) = **−240 mV** |
  | **Thioredoxin** | `113978` reducing potential **−0.26 to −0.28 V** · `113257` Trx1/Grx abundances as % of cell protein |
  | **Quinone** | `104411` ubiquinone **+110 mV** · `104412` menaquinone **−80 mV** · `104410` quinone content vs membrane potential, aerobic/anaerobic on glucose (table) · `103343` **midpoint potentials of electron donor/acceptor couples**, Neidhardt *E. coli and Salmonella* — the reference table for every couple here |
  | **Ferredoxin** | `112901` E°′(Fd) ≈ **−400 mV** (generic, not E. coli-specific) |
  | **Flavin** | **nothing.** No E. coli FAD/FADH₂ or FMN/FMNH₂ poise in BioNumbers. Second real gap. |
  | **Unit conversion** | `107924` **0.47 ± 0.03 gDCW per L per OD₆₀₀ = 1.0** · `104678` total intracellular metabolite pool ~300 mM · `109705` Nöh/Wiechert pools in **mmol/L_cell** · `110378` Hiller steady-state concentrations in nmol/L |

- **Provenance warning worth stating**: `104673`/`104680`/`104681`/`104682`/`104689`/`104692`/`104683` all cite
  **Bennett 2009** — the same source as ECMDB. BioNumbers is not an independent measurement of these; it is a
  hand-curated index into papers you already have. **What it uniquely adds is the non-metabolite physics**: pH,
  O₂ solubility, redox midpoint potentials, and the OD→gDCW conversion. Nothing else here is new.
- **Overlap with ECMDB** the concentration rows overlap ~100 %; the pH / O₂ / potential rows overlap 0 % because
  ECMDB does not carry that class of quantity at all.

### 8. eQuilibrator / MDF default concentration bounds — **KEEP (a convention, not data)**

- **Where it is stated, verbatim.** Noor, Bar-Even, Flamholz, Lubling, Davidi, Milo (2014) PLoS Comput Biol
  10(2):e1003483, *"Pathway thermodynamics highlights kinetic obstacles in central metabolism"* — PMID **24586134**,
  PMCID **PMC3930492**, **CC BY**. Methods, § *Metabolite concentration range*:

  > "Throughout our analyses we used metabolite concentration bounds characteristic of cellular physiology, a lower
  > bound C_min = 1 µM and an upper bound C_max = 10 mM."

  And in Fig. 5's legend: *"default metabolite concentration range used throughout this study (1 µM–10 mM)"*.
- **Command that worked**
  ```
  curl -sL "https://www.ebi.ac.uk/europepmc/webservices/rest/PMC3930492/fullTextXML" -o noor2014_MDF_PMC3930492.xml
  ```
  181 KB XML, CC BY, freely redistributable.
- **The same paragraph fixes the cofactor poises**, and this is the more valuable half — it is the published
  convention for exactly the quantities BioNumbers could not supply:
  > "[ATP]/[ADP] = 10, [ADP]/[AMP] = 1, [NADH]/[NAD⁺] = 0.1, [NADPH]/[NADP⁺] = 10,
  > [Ferredoxin_reduced]/[Ferredoxin_oxidized] = 1 (corresponds to a reduction potential of −400 mV),
  > [orthophosphate] = 10 mM, [pyrophosphate] = 1 mM, [CoA] = 1 mM, [CO₂(aq)] = 10 µM (ambient conditions)."

  Note the rationale, which is worth carrying: *"Wherever possible, we constrained the cofactor ratios rather than
  their absolute concentrations, since the ratios are more conserved."*
- **Absolute or relative?** Neither — it is a **prior**, and must be labelled as one. The 1 µM–10 mM box is a
  statement about what is physiologically plausible, not a measurement of anything.
- **Identifier namespaces** none needed: these are per-compound constants keyed by name, applied by hand.
- **Overlap with ECMDB** none — this is the fallback for the ~metabolites ECMDB does *not* carry, which is the
  large majority of any reaction universe.
- **Cross-check against the other sources here**: the MDF's [NADPH]/[NADP⁺] = 10 sits between BioNumbers
  `104689`/`104692` (which imply ~57) and `105023` (0.83). Its [CO₂(aq)] = 10 µM matches BioNumbers `106223`'s
  9.27 µM freshwater equilibrium almost exactly. Its ferredoxin −400 mV is the same number as BioNumbers `112901`.
  **Three independent confirmations that the MDF convention is the right default for the couples nobody measured.**
- **Also downloaded for provenance**: eQuilibrator itself is PMID 22064852 / PMC3245061 (CC BY-NC) and eQuilibrator
  3.0 is PMID 34850162 / PMC8728285 (CC BY). Neither ships concentration measurements — they ship ΔfG° estimates.

### 9. BiGG / iML1515 — **NO concentrations (as expected), but it is the BiGG→MetaNetX crosswalk this survey needs**

- **Command that worked**
  ```
  curl -sL "http://bigg.ucsd.edu/static/models/iML1515.json" -o iML1515.json     # 3.06 MB, no login
  ```
- **Licence** BiGG models are distributed for academic use; iML1515 itself is from Monk et al. 2017 Nat Biotechnol.
  Check bigg.ucsd.edu's terms before redistributing the file; the *derived crosswalk* is a fact table.
- **Concentrations: none.** Confirmed by inspection — the JSON contains **zero** occurrences of "concentration",
  "conc", or "molar". 1,877 metabolites, 2,712 reactions, and every metabolite carries only id / name /
  compartment / charge / formula / annotation. **Expectation confirmed, as the brief predicted.**
- **What it does carry, and why it matters more than the concentrations would have.** Per-metabolite annotation
  coverage across all 1,877 metabolites:

  | namespace | metabolites annotated |
  |---|---|
  | `bigg.metabolite` | **1877** |
  | **`metanetx.chemical`** | **1804** |
  | `seed.compound` | 1602 |
  | `inchi_key` | 1507 |
  | `biocyc` | 1499 |
  | `chebi` | 1439 |
  | `kegg.compound` | 1350 |
  | `hmdb` | 1060 |
  | `sabiork` | 1097 |
  | `reactome.compound` | 674 |

  Real row:
  ```json
  {"id":"octapb_c","name":"Octanoate (protein bound)","compartment":"c","charge":1,"formula":"C8H15O",
   "annotation":{"bigg.metabolite":["octapb"],"metanetx.chemical":["MNXM147531"],"sbo":"SBO:0000247"}}
  ```
  **`bigg.metabolite` → `metanetx.chemical` for 1,804 of 1,877 metabolites in one 3 MB file.** Park 2016 and
  Gerosa 2015 — the two best concentration sources found — both carry BiGG ids and *nothing else*. This file is what
  makes either of them joinable to MNXM without going through MetaNetX's own `chem_xref`, and it resolves the
  compartment suffix at the same time.

### 10. SABIO-RK — **REJECT, kinetics not pools (confirmed)**

- `https://sabiork.h-its.org/sabioRestWebServices/searchKineticLaws/sbml?q=Organism:"Escherichia coli"` now
  **302-redirects to `/ui/404`** — the legacy REST paths have been retired. The `kineticlawsExportTsv` endpoint
  likewise returns the SPA shell, not TSV.
- Irrespective of the endpoint move, SABIO-RK holds **Km, kcat, Ki and reaction-condition parameters**, not
  intracellular pool sizes. It is the wrong quantity. Confirmed and dropped, per the brief. No download kept.

### 11. YMDB — **REJECT (yeast), and it does NOT have ECMDB's concentrations page**

- `https://www.ymdb.ca/` — same Wishart Research Group / TMIC infrastructure as ECMDB, same site chrome
  (ChemQuery, LC-MS/GC-MS/NMR search, `/downloads`, `/compounds`, `/proteins`, `/reactions`).
- **`https://ymdb.ca/concentrations` returns 404.** ECMDB's paginated concentration index has **no YMDB
  counterpart** — so if another organism is ever wanted, the ECMDB scraping approach does not transfer to YMDB as
  written. That is the useful negative here.
- **Licence** stated on `/downloads`: *"YMDB is offered to the public as a freely available resource. Use and
  re-distribution of the data, in whole or in part, for commercial purposes requires explicit permission of the
  authors and explicit acknowledgment of the source material (YMDB) and the original publication."* Same wording
  family as ECMDB's — non-commercial redistribution with attribution.
- Organism is *S. cerevisiae*. **Rejected per the E.-coli-only rule.**

### 12. HMDB — **REJECT without downloading, and the reason is the quantity, not the size**

- HMDB's bulk concentration data is **blood, serum, urine, CSF, saliva and feces** — biofluid concentrations of a
  human. A serum concentration and a cytoplasmic pool are **different physical quantities**: one is a whole-body
  extracellular equilibrium, the other is the activity a reaction quotient actually sees inside a cell. Substituting
  one for the other does not add noise, it adds a bias of unknown sign.
- It is also the wrong organism. **Not downloaded. Not pursued.** Exactly per the brief.

### 13. Ishii et al. 2007 (Science) — **manual only; already in ECMDB**

- PMID **17379776**, "Multiple high-throughput analyses monitor the response of E. coli to perturbations."
  Europe PMC: `isOpenAccess = N`, no PMCID, no supplementary files. **Science paywall — manual click-through or an
  institutional subscription. Not attempted.**
- **Already covered**: ECMDB's concentration rows cite PMID 17379776, so its E. coli measurements are in the
  baseline already.

### 14. Taymaz-Nikerel et al. 2009 — **manual only, nothing retrievable**

- Two 2009 papers, neither open access, neither with EPMC supplementary:
  PMID **19084496** (Anal Biochem, *"Development and application of a differential method for reliable metabolome
  analysis"* — the E. coli chemostat pool paper) and PMID **19685524** (Biotechnol Bioeng, aerobic→anaerobic shift).
  Both `isOpenAccess = N`, no PMCID. **Elsevier paywall. Not attempted.**
- The nearest machine-readable trace of this literature is **BioNumbers BNID `109705`** (Nöh/Wiechert, pools in
  **mmol/L_cell**) and BNID `110378` (Hiller, nmol/L) — both "Table - link" records, so the numbers sit in an
  attached table rather than in the row.


---

## What an aggregation actually buys — measured, not estimated

Two numbers, both computed against the real MetaNetX 4.5 files in the repo
(`data/fabfos/originals/metanetx/4.5/{chem_xref,chem_prop}.tsv`, read only) and the deployed lane's own output
(`research/fabfos/bake/work/conc_mnxm.tsv`). Script output in `aggregation_mnxm_coverage.json`.

### 1. Chemically, the panel adds almost nothing

Joining on **InChIKey connectivity skeleton** (ECMDB's `moldb_inchikey`, populated for **all 891** measured
metabolites, against iML1515's `inchi_key`):

| Source | metabolites | skeleton-matches an ECMDB measured metabolite | not matched |
|---|---|---|---|
| Park 2016 | 103 | 97 | **4** — 2-deoxy-D-ribose 5-phosphate, FMN, FAD, sedoheptulose 7-phosphate |
| Gerosa 2015 | 42 | 30 | **5** — KDPG, 1,3-bisphosphoglycerate, fructose 1-phosphate, isocitrate, sedoheptulose 7-phosphate |
| union | 120 | 103 | **8 distinct** |

**Park + Gerosa together add 8 chemicals to ECMDB's 891.** Everything else is a re-measurement. That is the honest
answer to "how much does the panel add" and it is not flattering.

### 2. The bottleneck is not the number of sources — it is ECMDB's own crosswalk

| route | distinct MNXM reached |
|---|---|
| ECMDB 891 measured, via `kegg.compound:` + `chebi:` (**what the lane does today**) | **232** |
| Park 2016 (103 metabolites) via `bigg.metabolite:` | **101** (2 unresolved: `acon-C`, `dad-2`) |
| Gerosa 2015 (42 metabolites) via `bigg.metabolite:` | **42** (0 unresolved) |
| Park ∪ Gerosa | 111, of which **104 already in ECMDB's 232** |
| **ECMDB ∪ Park ∪ Gerosa** | **239 — only +7, +3 %** |

The 7 genuinely new MNXM keys: `MNXM1104787` β-D-fructose 1-phosphate · `MNXM1364213` α-D-galactose 1-phosphate ·
`MNXM1364464` (2R)-3-phospho-glyceroyl phosphate · `MNXM649` KDPG · `MNXM733314` D-sedoheptulose 7-phosphate ·
`MNXM741175` L-ornithine · `MNXM89661` isocitrate(3−).

**ECMDB's 891 measured metabolites collapse to 232 MNXM because ECMDB's bulk crosswalk carries a KEGG id for only
1,291 of its 3,760 compounds and a ChEBI id for 968** — and the *measured* subset is worse than average: of the 891,
**181 carry a KEGG id and 166 a ChEBI id; 710 carry neither.** Adding sources does not fix this. **659 of ECMDB's
own measurements are unreachable, and that is ~3× the size of everything the panel could add.**

### 3. The InChIKey rescue does NOT work — tested and rejected

ECMDB carries `moldb_inchikey` for all 891, and MetaNetX `chem_prop` carries an `InChIKey` column for 1,231,714
rows, so the obvious fix is to join on it. **It fails**: of the 891, only **72 hit an exact 27-character InChIKey**
in `chem_prop`, yielding **75 MNXM — a third of what the KEGG/ChEBI route already reaches**. A further 155 match on
the 14-character skeleton only, and the skeleton is exactly the join the survey-time script already
documents as unsafe (`HXXFSFRBOHSIMQ` is a seventeen-accession hexose-phosphate bucket). ECMDB stores the neutral
acid form; MetaNetX stores a reference protonation state, so the third block diverges. **Stated as a negative
result: do not build the InChIKey route.**

---

## Identifier namespaces an aggregation must join on

Three, and no fewer:

1. **`kegg.compound:`** — ECMDB's primary crosswalk (181 of its 891 measured), and Radoš 2022's Table S2's only
   identifier column. Already used.
2. **`chebi:`** — ECMDB's secondary (166 of 891), and **every MetaboLights MAF's `database_identifier`, at 100 %
   coverage**. Already used. It is the namespace to demand of any future deposit.
3. **`bigg.metabolite:`** — **NEW, and required**: Park 2016 and Gerosa 2015, the two best-quality sources found,
   carry BiGG ids and *nothing else*. Both declare KEGG/ChEBI/PubChem/InChI fields and both leave them **entirely
   empty**. MetaNetX 4.5 `chem_xref` carries the `bigg.metabolite:` prefix natively (9,087 entries), so no extra
   crosswalk file is needed — but two normalisations are:
   - strip the compartment suffix: `glu-L[c]` → `glu-L`, `13dpg_c` → `13dpg`;
   - map old-style iAF1260/Gerosa stereo suffixes to modern BiGG: `-L` → `__L`, `-D` → `__D`, and bare
     `glu`/`asp`/`arg`/`asn`/`gln`/`phe`/`tyr` → `glu__L` etc. Without this, 7 of Gerosa's 42 and several of Park's
     silently vanish.
   Two Park ids resolve to nothing even so — `acon-C` (cis-aconitate) and `dad-2` (deoxyadenosine) — and must be
   hand-mapped or dropped.

**Do NOT join on names, and do NOT join on InChIKey skeleton.** Names fail on `Citrate` vs `Citric acid` and on
pooled channels (`2pg+3pg`, `Citrate or Isocitrate`, `Glucose 6-phosphate or fructose-6-phosphate` — three MW
channels and one Gerosa channel that each name two compounds). The skeleton fails as documented above.

---

## Ranked recommendation

### Pin into the repo

| # | Source | Why | Licence risk |
|---|---|---|---|
| 1 | **Park 2016** `ecoli_integrate.mat` (GitHub, MIT) | 103 measured cytoplasmic concentrations in **molar**, BiGG-keyed, plus the pinned physical state (**T = 310.15 K, cytoplasmic pH 7.7**, ionic strength, membrane potential) and thermodynamically-consistent LB/UB for ~850 unmeasured metabolites. **MIT — the only source here that is unambiguously redistributable.** | none |
| 2 | **Gerosa 2015** `mmc2.xlsx` (Elsevier CDN) | The **only real condition panel**: 42 metabolites × **8 carbon sources**, µmol/gCDW, *with the authors' own 2.3 mL/gCDW conversion to molar*. Also ships per-reaction ΔG and per-condition growth/uptake rates. This is the width the concentration correction needs. | Elsevier OA user licence — attribution + **non-commercial**. Check before shipping. |
| 3 | **eQuilibrator / MDF conventions** (Noor 2014, CC BY) | The 1 µM–10 mM box *and* the fixed cofactor ratios — ATP/ADP = 10, NADH/NAD⁺ = 0.1, NADPH/NADP⁺ = 10, Fd_red/Fd_ox = 1, Pi = 10 mM, PPi = 1 mM, CoA = 1 mM, **CO₂(aq) = 10 µM**. Independently corroborated by BioNumbers on CO₂ and ferredoxin. The defensible prior for everything unmeasured. | CC BY |
| 4 | **BioNumbers curated 37 rows** | The non-metabolite physics nothing else carries: cytoplasmic pH (7.5–7.98), **O₂ solubility in M9 at 37 °C = 0.264 mM**, ubiquinone +110 mV / menaquinone −80 mV, GSSG/2GSH −240 mV, thioredoxin −0.26 to −0.28 V, and the OD₆₀₀ → 0.47 gDCW/L conversion. Cite BNIDs, do not redistribute the table. | cite-only |
| 5 | **iML1515** `iML1515.json` (BiGG) | Not for concentrations — it has none. Keep it as the **compartment-aware BiGG vocabulary** and as a cross-check on MetaNetX's own `bigg.metabolite:` mapping, which disagreed with iML1515's `metanetx.chemical` annotations on ~60 accessions during this survey. Prefer MetaNetX's own xref; keep this to audit it. | academic use |

### Delete

- **Metabolomics Workbench** (3.0 MB) — 58 E. coli studies, 3 with a real absolute unit, ~40 metabolites, **names
  only**, ~100 % ECMDB overlap. The only thing worth extracting first is `ST001421`/`ST002076` if an **anaerobic**
  column is ever wanted; those two are the sole anaerobic absolute measurements found anywhere in this survey.
- **MetaboLights** (6.3 MB) — best identifiers in the survey (100 % ChEBI + InChI + SMILES), zero molar numbers.
  Keep the finding, delete the files.
- **Radoš 2022 supplement** (5.3 MB) — fold-change only. Keep `mmc2.xlsx` (Table S1, the 19-condition table) if the
  fold-change × ECMDB-anchor derivation is ever attempted; delete `mmc3.xlsx` otherwise. CC BY-NC-**ND** makes that
  derivation a licence question, not just a science one.
- **`_probes/`** — SABIO-RK and YMDB negative-result artefacts, kept only as evidence. Delete.

### Not obtainable (stated, not circumvented)

**Bennett 2009**, **Ishii 2007**, **Taymaz-Nikerel 2009**: paywall or reCAPTCHA on every automated route. Bennett
and Ishii are already inside ECMDB by PMID, so nothing is lost. Taymaz-Nikerel is genuinely absent — the nearest
machine-readable trace is BioNumbers BNID `109705`.


---

## Derived files worth reading first

| path | what |
|---|---|
| `park2016/park2016_ecoli_iAF1260_concentrations.tsv` | 1,668 rows; filter `compartment == c` and `conc_measured_M` non-empty for the 103 |
| `gerosa2015/gerosa2015_ecoli_concentrations.tsv` | 342 rows = 43 metabolites × 8 carbon sources, both µmol/gCDW and mM |
| `bionumbers/bionumbers_curated.tsv` | 37 records: pH, O₂, CO₂, redox potentials, unit conversions, with BNIDs |
| `bionumbers/bionumbers_ecoli_physicochemical.tsv` | the full 175-row sweep behind the curated set |
| `metabolomics_workbench/mw_ecoli_absolute_tidy.tsv` | 183 rows over the 5 MW studies with any absolute-looking unit |
| `metabolomics_workbench/mw_ecoli_studies.json` | the 58 E. coli studies with licence and analysis type |
| `metabolights/ml_ecoli_candidates.json` | the 627 MetaboLights accessions whose organism names *Escherichia* |
| `aggregation_mnxm_coverage.json` | the MNXM sets behind the 232 / 111 / 239 numbers above |
| `equilibrator_mdf/noor2014_MDF_PMC3930492.xml` | full text; the bounds and cofactor paragraph is in Methods |

**Environment note:** the `ecspr` env has no xlsx reader. Gerosa's and Radoš's `.xlsx` were read with
`mamba run -n msm python` (openpyxl 3.1.5). Everything else is `mamba run -n ecspr python`.
