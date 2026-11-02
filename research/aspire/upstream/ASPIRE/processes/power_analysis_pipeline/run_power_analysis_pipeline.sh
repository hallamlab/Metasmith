#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<USAGE
Run full power-analysis workflow:
  1) estimate_effects.py
  2) power_main.py
  3) plot_power_curves.py

Usage:
  $(basename "$0") \
    --data-long <ASV_master_long.tsv> \
    --data-wide <ASV_count_wide.tsv> \
    --outdir <power_output_root> \
    [--sample-col sample] \
    [--patient-col Participant_ID] \
    [--case-col Case] \
    [--type-col type_group] \
    [--sample-sizes-cancer 6,8,10,15,20,25,30] \
    [--sample-sizes-stype 10,15,20,25,30,40,50] \
    [--n-simulations 1000] [--n-perm 199] \
    [--alpha 0.05] [--seed 42] \
    [--skip-estimate] [--skip-plot] \
    [--transform none|rclr] \
    [--indicspecies-dir <main_analysis_output/indicspecies>] \
    [--keep-contralateral-in-cancer] [--contralateral-sample-types "Bronchial Brush,BAL"]
USAGE
}

DATA_LONG=""
DATA_WIDE=""
OUTDIR=""
SAMPLE_COL="sample"
PATIENT_COL="Participant_ID"
CASE_COL="Case"
TYPE_COL="type_group"
SAMPLE_SIZES_CANCER="6,8,10,15,20,25,30"
SAMPLE_SIZES_STYPE="10,15,20,25,30,40,50"
N_SIMULATIONS="1000"
N_PERM="199"
ALPHA="0.05"
SEED="42"
SKIP_ESTIMATE="false"
SKIP_PLOT="false"
TRANSFORM="none"
INDICSPECIES_DIR=""
EXCLUDE_CONTRALATERAL="true"
CONTRALATERAL_SAMPLE_TYPES="Bronchial Brush,BAL"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-long) DATA_LONG="$2"; shift 2 ;;
    --data-wide) DATA_WIDE="$2"; shift 2 ;;
    --outdir) OUTDIR="$2"; shift 2 ;;
    --sample-col) SAMPLE_COL="$2"; shift 2 ;;
    --patient-col) PATIENT_COL="$2"; shift 2 ;;
    --case-col) CASE_COL="$2"; shift 2 ;;
    --type-col) TYPE_COL="$2"; shift 2 ;;
    --sample-sizes-cancer) SAMPLE_SIZES_CANCER="$2"; shift 2 ;;
    --sample-sizes-stype) SAMPLE_SIZES_STYPE="$2"; shift 2 ;;
    --n-simulations) N_SIMULATIONS="$2"; shift 2 ;;
    --n-perm) N_PERM="$2"; shift 2 ;;
    --alpha) ALPHA="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --skip-estimate) SKIP_ESTIMATE="true"; shift ;;
    --skip-plot) SKIP_PLOT="true"; shift ;;
    --transform) TRANSFORM="$2"; shift 2 ;;
    --indicspecies-dir) INDICSPECIES_DIR="$2"; shift 2 ;;
    --keep-contralateral-in-cancer) EXCLUDE_CONTRALATERAL="false"; shift ;;
    --contralateral-sample-types) CONTRALATERAL_SAMPLE_TYPES="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 2 ;;
  esac
done

[[ -n "$DATA_LONG" ]] || { echo "ERROR: --data-long is required"; exit 2; }
[[ -n "$DATA_WIDE" ]] || { echo "ERROR: --data-wide is required"; exit 2; }
[[ -n "$OUTDIR" ]] || { echo "ERROR: --outdir is required"; exit 2; }
[[ -f "$DATA_LONG" ]] || { echo "ERROR: data-long not found: $DATA_LONG"; exit 2; }
[[ -f "$DATA_WIDE" ]] || { echo "ERROR: data-wide not found: $DATA_WIDE"; exit 2; }
[[ "$TRANSFORM" == "none" || "$TRANSFORM" == "rclr" ]] || { echo "ERROR: --transform must be one of: none, rclr"; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$OUTDIR"

EFFECT_DIR="$OUTDIR/effect_sizes"
RESULTS_DIR="$OUTDIR/results"
PLOTS_DIR="$OUTDIR/plots"
mkdir -p "$EFFECT_DIR" "$RESULTS_DIR" "$PLOTS_DIR"

log() { printf "[%(%F %T)T] %s\n" -1 "$*"; }

if [[ "$SKIP_ESTIMATE" == "false" ]]; then
  log "Estimating observed effect sizes"
  python3 "$SCRIPT_DIR/estimate_effects.py" \
    --data-wide "$DATA_WIDE" \
    --data-long "$DATA_LONG" \
    --sample-col "$SAMPLE_COL" \
    --patient-col "$PATIENT_COL" \
    --case-col "$CASE_COL" \
    --type-col "$TYPE_COL" \
    --outdir "$EFFECT_DIR" \
    --transform "$TRANSFORM" \
    --exclude-contralateral-in-cancer "$EXCLUDE_CONTRALATERAL" \
    --contralateral-sample-types "$CONTRALATERAL_SAMPLE_TYPES"
fi

log "Running power_main.py"
python3 "$SCRIPT_DIR/power_main.py" \
  --data-wide "$DATA_WIDE" \
  --data-long "$DATA_LONG" \
  --effect-sizes-dir "$EFFECT_DIR" \
  --sample-col "$SAMPLE_COL" \
  --patient-col "$PATIENT_COL" \
  --case-col "$CASE_COL" \
  --type-col "$TYPE_COL" \
  --sample-sizes-cancer "$SAMPLE_SIZES_CANCER" \
  --sample-sizes-stype "$SAMPLE_SIZES_STYPE" \
  --n-simulations "$N_SIMULATIONS" \
  --n-perm "$N_PERM" \
  --alpha "$ALPHA" \
  --seed "$SEED" \
  --outdir "$RESULTS_DIR" \
  --transform "$TRANSFORM" \
  --exclude-contralateral-in-cancer "$EXCLUDE_CONTRALATERAL" \
  --contralateral-sample-types "$CONTRALATERAL_SAMPLE_TYPES"

if [[ "$SKIP_PLOT" == "false" ]]; then
  log "Plotting power curves"
  python3 "$SCRIPT_DIR/plot_power_curves.py" \
    --results-dir "$RESULTS_DIR" \
    --outdir "$PLOTS_DIR"

  # ISA power/aligned plotting (includes ISA power curves from indicspecies_power_results.tsv).
  ISA_POWER_RESULTS="$RESULTS_DIR/indicspecies_power_results.tsv"
  if [[ -f "$ISA_POWER_RESULTS" ]]; then
    ISA_PLOT_DIR="$PLOTS_DIR/indicspecies_aligned"

    # Auto-discover main analysis indicspecies outputs if user didn't pass --indicspecies-dir.
    if [[ -z "$INDICSPECIES_DIR" ]]; then
      CANDIDATE_ISA_DIR="$(cd "$OUTDIR/.." && pwd)/main_analysis_output/indicspecies"
      if [[ -d "$CANDIDATE_ISA_DIR" ]]; then
        INDICSPECIES_DIR="$CANDIDATE_ISA_DIR"
      fi
    fi

    ISA_PLOT_CMD=(
      python3 "$SCRIPT_DIR/../indicspecies_aligned_plots/plot_indicspecies_aligned.py"
      --power-results "$ISA_POWER_RESULTS"
      --outdir "$ISA_PLOT_DIR"
    )
    if [[ -n "$INDICSPECIES_DIR" ]]; then
      ISA_PLOT_CMD+=(--indicspecies-dir "$INDICSPECIES_DIR")
    fi

    log "Plotting ISA power/aligned outputs"
    "${ISA_PLOT_CMD[@]}"
  else
    log "Skipping ISA power/aligned plots (no file: $ISA_POWER_RESULTS)"
  fi
fi

log "Power pipeline complete"
log "Effect sizes: $EFFECT_DIR"
log "Results: $RESULTS_DIR"
log "Plots: $PLOTS_DIR"
