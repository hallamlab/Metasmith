# Lung Status Analysis: TumorSide vs. Contralateral vs. Healthy

Analysis pipeline for comparing microbiome composition across:
- **TumorSide**: Lung samples from the cancerous side (cancer patients)
- **Contralateral**: Lung samples from the opposite side (cancer patients)
- **Healthy**: Lung samples from control patients (averaged left/right)

---

## Design Overview

### Three Planned Contrasts

**A. TumorSide vs. Contralateral** (paired, within cancer patients)
- Tests if microbiome differs between cancerous and contralateral lungs
- Uses patient-blocked PERMANOVA: `adonis2(..., how(blocks=patient_id))`
- Alpha: Paired Wilcoxon signed-rank test

**B. Contralateral vs. Healthy** (between-patient)
- Tests if contralateral lung microbiome differs from healthy controls
- Patient-level comparison (1 profile per patient)
- Alpha: Wilcoxon rank-sum test

**C. TumorSide vs. Healthy** (between-patient)
- Tests if tumor-side microbiome differs from healthy controls
- Patient-level comparison
- Alpha: Wilcoxon rank-sum test

### Sample Types

- **Primary**: Lung Brush (n=8 fully paired cancer patients)
- **Exploratory**: BAL (n=4 fully paired cancer patients)

### Multiple Testing

FDR correction across the 3 planned contrasts (per sample type).

---

## Pipeline Scripts

### 1. Data Preparation
**Script**: `prepare_lung_status_data.py`

- Assigns `lung_status` based on `Case`, `Cancer_Site`, and `lung_code`
- Filters to specified sample type (Lung Brush or BAL)
- Creates ASV table and metadata files

**Usage**:
```bash
python3 prepare_lung_status_data.py \
  --input data/supplementary_table_S2_ASV_master_long.tsv \
  --sample-type "Lung Brush" \
  --outdir lung_status_analysis/Lung_Brush/data
```

### 2. Statistical Analysis (R)
**Script**: `run_lung_status_analysis.R`

- Runs contrasts A/B/C with appropriate statistical tests
- PERMANOVA with patient blocking (contrast A)
- PERMDISP checks for dispersion homogeneity
- Alpha diversity tests (Shannon)
- FDR correction

**Usage**:
```bash
Rscript run_lung_status_analysis.R \
  lung_status_analysis/Lung_Brush/data/Lung_Brush_metadata.tsv \
  lung_status_analysis/Lung_Brush/data/Lung_Brush_ASV_table.tsv \
  lung_status_analysis/Lung_Brush/results
```

**R Dependencies**:
```r
install.packages(c("vegan", "permute", "dplyr", "tidyr", "tibble", "readr"))
```

**Critical**: The `permute` package is required for patient-blocked permutations in PERMANOVA.

### 3. Visualization (Python)
**Script**: `plot_lung_status_analysis.py`

Creates:
- **PCoA plot** (proper eigendecomposition) with paired connection lines
- **Alpha diversity boxplots** (sample-level and patient-level)
- **Bray-Curtis distance boxplots** for all three contrasts (A/B/C)
- **R² summary bar plot** for all contrasts with FDR-corrected q-values

**Usage**:
```bash
python3 plot_lung_status_analysis.py \
  --metadata lung_status_analysis/Lung_Brush/data/Lung_Brush_metadata.tsv \
  --patient-level lung_status_analysis/Lung_Brush/results/patient_level_metadata.tsv \
  --distances lung_status_analysis/Lung_Brush/results/patient_level_bray_distances.tsv \
  --summary lung_status_analysis/Lung_Brush/results/lung_status_contrasts_summary.tsv \
  --pairdist-a lung_status_analysis/Lung_Brush/results/contrast_A_pairwise_distances.tsv \
  --asv-table lung_status_analysis/Lung_Brush/data/Lung_Brush_ASV_table.tsv \
  --outdir lung_status_analysis/Lung_Brush/figures
```

**Python Dependencies**:
```bash
pip install pandas numpy matplotlib seaborn scipy scikit-bio
```

**Critical**: `scikit-bio` is required for proper PCoA (eigendecomposition of distance matrix). Do NOT use sklearn's MDS as a substitute.

### 4. Master Pipeline
The standalone wrapper `run_lung_status_pipeline.sh` has been removed.

Use one of these paths instead:
- Enable `lung_status_analysis.enabled: true` in the Nextflow workflow.
- Run the component scripts directly: `prepare_lung_status_data.py`, `run_lung_status_analysis.R`, and `plot_lung_status_analysis.py`.

---

## Output Structure

```
lung_status_analysis/
├── Lung_Brush/
│   ├── data/
│   │   ├── Lung_Brush_with_lung_status.tsv     # Full annotated data
│   │   ├── Lung_Brush_ASV_table.tsv             # Wide ASV table
│   │   └── Lung_Brush_metadata.tsv              # Sample metadata
│   ├── results/
│   │   ├── lung_status_contrasts_summary.tsv    # Main results table
│   │   ├── patient_level_metadata.tsv           # Patient-level metadata
│   │   ├── patient_level_bray_distances.tsv     # Distance matrix
│   │   └── contrast_A_pairwise_distances.tsv    # Paired distances
│   └── figures/
│       ├── PCoA_lung_status.pdf                 # PCoA with pairing
│       ├── Alpha_diversity_lung_status.pdf      # Shannon boxplots
│       ├── Distance_contrasts_ABC.pdf           # Combined A/B/C distances
│       └── PERMANOVA_R2_contrasts.pdf           # Effect sizes
└── BAL/                                          # (if --run-bal)
    └── [same structure]
```

---

## Key Results File

**`lung_status_contrasts_summary.tsv`** contains:

| Column | Description |
|--------|-------------|
| `contrast` | A/B/C identifier |
| `comparison_type` | "paired" or "between_patient" |
| `n_patients` | Number of patients |
| `permanova_R2` | Effect size |
| `permanova_F` | F-statistic |
| `permanova_p` | Raw p-value |
| `permanova_q` | FDR-corrected q-value |
| `permdisp_F` | Dispersion test F |
| `permdisp_p` | Dispersion test p-value |
| `alpha_p` | Alpha diversity p-value |
| `alpha_q` | Alpha diversity q-value |

---

## Interpretation Notes

### Contrast A (Paired)
- Direct test of cancer effect within patients
- Controls for patient-specific factors
- Most powerful for detecting tumor-associated changes

### Contrasts B & C (Between-Patient)
- Compare cancer patients to healthy controls
- B tests if contralateral lung is "normal"
- C tests if tumor lung differs from healthy
- Patient-level aggregation avoids pseudoreplication

### BAL Caveat
- Only n=4 fully paired cancer patients
- Label as "exploratory" in any publication
- May be underpowered to detect true effects

---

## References

Statistical approach based on:
- Anderson, M.J. (2001). *Permutational multivariate analysis of variance*. Austral Ecology 26: 32-46.
- Anderson, M.J. & Walsh, D.C.I. (2013). *PERMANOVA, ANOSIM, and the Mantel test in the face of heterogeneous dispersions*. Ecological Monographs 83: 557-574.

---

## Troubleshooting

### "No samples found for sample type 'Lung Brush'"
- The script uses the `type_group` column (not `Type`)
- Valid values: `"Lung Brush"`, `"BAL"`, `"Oral Rinse"`
- Check your input data: the script will print available types

### "could not find function 'how'" (R error)
- Install the `permute` package: `install.packages("permute")`
- The script now explicitly loads `library(permute)`

### "No overlapping samples between metadata and ASV table"
- Check that sample IDs match exactly between files
- The prepare script uses the `sample` column as the key

### PCoA plot shows unusual patterns
- Verify you have `scikit-bio` installed (not sklearn's MDS)
- Check PERMDISP results - significant dispersion differences can confound PERMANOVA

### Low power / non-significant results
- Lung Brush: n=8 paired patients (adequate for large effects)
- BAL: n=4 paired patients (exploratory only, likely underpowered)
- Focus on effect sizes (R²) not just p-values

---

## Questions?

Contact the analysis developer or refer to:
- `vegan` package documentation: https://CRAN.R-project.org/package=vegan
- PERMANOVA tutorial: https://archetypalecology.wordpress.com/2018/02/21/permutational-multivariate-analysis-of-variance-permanova-in-r-preliminary/
- scikit-bio PCoA: http://scikit-bio.org/docs/latest/generated/skbio.stats.ordination.pcoa.html
