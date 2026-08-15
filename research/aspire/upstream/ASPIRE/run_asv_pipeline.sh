#!/usr/bin/env bash
set -euo pipefail

export NXF_VER="${NXF_VER:-25.10.0}"
export NXF_SYNTAX_PARSER="${NXF_SYNTAX_PARSER:-v1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="${SCRIPT_DIR}/$(basename "$0")"
CONTROL_ENV_DIR="${CONTROL_ENV_DIR:-${SCRIPT_DIR}/.controller_env}"
ORIGINAL_ARGS=("$@")

PROCESS_ORDER=(
  FASTP_QC
  MERGE_READS
  FILTER_READS
  RELABEL_FILTERED
  CONCAT_FASTAS
  DEREPLICATE
  DENOISE
  CHIMERA_CHECK
  CREATE_COUNT_MATRIX
  FILTER_TABLE
  SINA_TRIM
  TAXONOMY
  PREPARE_BLAST_DATABASES
  MITOMASTER
  MITO_DECONTAM
  FILTER_COUNTS
  GENERAL_STATS
  PLOT_METADATA
  GROUPING_DIAGNOSTICS
  GROUP_LABEL_AUGMENTATION
  PLOT_UPSET
  ASV_BATCH_CORRECTION
  ASV_META_FROM_CORRECTED
  BUBBLEPLOTTER
  UMAP_CLUSTERING
  OUTLIER_CHECKER
  COLLECTORS_CURVE
  DIVERSITY_ANALYSIS
  INDICSPECIES
  INDICSPECIES_PLOTS
  INDICSPECIES_ALIGNED_PLOTS
  VOC_CORRELATION
  MEASUREMENT_ASSOCIATION
  CLUSTERMAPS
  GROUP_POWER_ANALYSIS
  TAXONOMY_GROUP_ASSOCIATION
  PAIRED_GROUP_CONTRAST
  SPIECEASI
  NETWORK_MODULES
  ASV_MAG_LINK
  ASV_MAG_NETWORK
  GRAPH_NETWORK
  MODULE_MAG_ANCHORS
  SANKEY
  MASTER_SUMMARY
)

declare -A PROCESS_ALIASES=(
  [POWER_ANALYSIS_PIPELINE]=GROUP_POWER_ANALYSIS
  [TAXONOMY_PATIENT_AWARE]=TAXONOMY_GROUP_ASSOCIATION
  [LUNG_STATUS_ANALYSIS]=PAIRED_GROUP_CONTRAST
)

usage() {
  cat <<'EOF'
Usage:
  run_asv_pipeline.sh [CONFIG_FILE] [--rerun-from PROCESS_NAME] [--resume-run RUN_NAME] [--resume-policy POLICY] [--no-resume] [--list-stages] [-- NEXTFLOW_ARGS...]

Options:
  --rerun-from PROCESS_NAME  Force rerun starting at PROCESS_NAME and all later processes in controller order, while preserving cacheability for future resumes.
  --resume-run RUN_NAME      Resume from a specific Nextflow run name/id (from `nextflow log -q`).
  --resume-policy POLICY     Resume baseline selection when --resume-run is not set.
                             Allowed: last-with-tasks (default), latest
  --no-resume                Disable resume for this run (cold execution).
  --list-stages              Print known process names for --rerun-from and exit.
  --help, -h                 Show this help.

Examples:
  run_asv_pipeline.sh asv_pipeline_nextflow.yml --rerun-from FILTER_COUNTS
  run_asv_pipeline.sh --resume-run lethal_poisson
  run_asv_pipeline.sh --resume-policy latest
  run_asv_pipeline.sh --rerun-from PLOT_METADATA -- -with-report report.html
EOF
}

if [[ -z "${IN_CONTROLLER_ENV:-}" ]]; then
  if ! command -v mamba >/dev/null 2>&1; then
    echo "mamba is required to bootstrap the controller environment." >&2
    exit 1
  fi
  exec 8>"${TMPDIR:-/tmp}/aspire-controller-${UID}.lock"
  if ! flock -w 7200 8; then
    echo "Timed out waiting for another ASPIRE controller environment update." >&2
    exit 1
  fi
  if [[ ! -d "$CONTROL_ENV_DIR" ]]; then
    echo "[controller] Creating mamba env at $CONTROL_ENV_DIR"
    mamba env create --yes --prefix "$CONTROL_ENV_DIR" --file "${SCRIPT_DIR}/processes/controller/env.yml"
  elif [[ ! -x "$CONTROL_ENV_DIR/bin/mamba" ]]; then
    echo "[controller] Updating controller environment to provide an isolated mamba executable."
    mamba env update --prefix "$CONTROL_ENV_DIR" --file "${SCRIPT_DIR}/processes/controller/env.yml"
  fi
  flock -u 8
  exec env IN_CONTROLLER_ENV=1 CONTROL_ENV_DIR="$CONTROL_ENV_DIR" conda run --no-capture-output -p "$CONTROL_ENV_DIR" "$SCRIPT_PATH" "$@"
fi

CONFIG_FILE="asv_pipeline_nextflow.yml"
CONFIG_SET=0
LIST_STAGES=0
RERUN_FROM=""
NEXTFLOW_ARGS=()
RESUME_ENABLED=1
RESUME_POLICY="last-with-tasks"
RESUME_RUN=""
RERUN_CONFIG_FILE=""
declare -A RERUN_STAGE_SET=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --list-stages)
      LIST_STAGES=1
      shift
      ;;
    --rerun-from)
      if [[ $# -lt 2 ]]; then
        echo "--rerun-from requires a process name" >&2
        exit 1
      fi
      RERUN_FROM="$2"
      shift 2
      ;;
    --rerun-from=*)
      RERUN_FROM="${1#*=}"
      shift
      ;;
    --resume-run)
      if [[ $# -lt 2 ]]; then
        echo "--resume-run requires a run name or id" >&2
        exit 1
      fi
      RESUME_RUN="$2"
      shift 2
      ;;
    --resume-run=*)
      RESUME_RUN="${1#*=}"
      shift
      ;;
    --resume-policy)
      if [[ $# -lt 2 ]]; then
        echo "--resume-policy requires a value: last-with-tasks|latest" >&2
        exit 1
      fi
      RESUME_POLICY="$2"
      shift 2
      ;;
    --resume-policy=*)
      RESUME_POLICY="${1#*=}"
      shift
      ;;
    --no-resume)
      RESUME_ENABLED=0
      shift
      ;;
    --)
      shift
      NEXTFLOW_ARGS+=("$@")
      break
      ;;
    -*)
      NEXTFLOW_ARGS+=("$1")
      shift
      ;;
    *)
      if [[ $CONFIG_SET -eq 0 ]]; then
        CONFIG_FILE="$1"
        CONFIG_SET=1
      else
        NEXTFLOW_ARGS+=("$1")
      fi
      shift
      ;;
  esac
done

if [[ "$LIST_STAGES" -eq 1 ]]; then
  printf '%s\n' "${PROCESS_ORDER[@]}"
  if [[ ${#PROCESS_ALIASES[@]} -gt 0 ]]; then
    echo "# aliases"
    for alias_name in "${!PROCESS_ALIASES[@]}"; do
      printf '%s -> %s\n' "$alias_name" "${PROCESS_ALIASES[$alias_name]}"
    done | sort
  fi
  exit 0
fi

if [[ "$RESUME_POLICY" != "last-with-tasks" && "$RESUME_POLICY" != "latest" ]]; then
  echo "Invalid --resume-policy '${RESUME_POLICY}'. Allowed: last-with-tasks, latest" >&2
  exit 1
fi

if [[ "$RESUME_ENABLED" -eq 0 && -n "$RESUME_RUN" ]]; then
  echo "--no-resume cannot be combined with --resume-run" >&2
  exit 1
fi

sanitize_nextflow_args() {
  local sanitized=()
  local skip_next=0
  local i arg next_arg
  for ((i=0; i<${#NEXTFLOW_ARGS[@]}; i++)); do
    if [[ $skip_next -eq 1 ]]; then
      skip_next=0
      continue
    fi
    arg="${NEXTFLOW_ARGS[$i]}"
    case "$arg" in
      -resume)
        if (( i + 1 < ${#NEXTFLOW_ARGS[@]} )); then
          next_arg="${NEXTFLOW_ARGS[$((i + 1))]}"
          if [[ "$next_arg" != -* ]]; then
            skip_next=1
          fi
        fi
        echo "[controller] Ignoring passthrough '-resume' argument; use --resume-run/--resume-policy/--no-resume." >&2
        ;;
      -resume=*)
        echo "[controller] Ignoring passthrough '-resume=...' argument; use --resume-run/--resume-policy/--no-resume." >&2
        ;;
      *)
        sanitized+=("$arg")
        ;;
    esac
  done
  NEXTFLOW_ARGS=("${sanitized[@]}")
}

has_tasks_for_run() {
  local run_name="$1"
  local rows rc process_name workdir _status
  set +e
  rows="$(nextflow log "$run_name" -f 'process,workdir,status' 2>&1)"
  rc=$?
  set -e
  [[ $rc -eq 0 ]] || return 1
  while IFS=$'\t' read -r process_name workdir _status; do
    [[ -n "$process_name" ]] || continue
    [[ "$process_name" == "process" && "$workdir" == "workdir" ]] && continue
    return 0
  done <<< "$rows"
  return 1
}

select_baseline_run() {
  local selected=""
  local latest=""
  local runs=()
  local i run_name

  if [[ -n "$RESUME_RUN" ]]; then
    if ! nextflow log "$RESUME_RUN" >/dev/null 2>&1; then
      echo "Specified --resume-run not found in Nextflow history: ${RESUME_RUN}" >&2
      exit 1
    fi
    selected="$RESUME_RUN"
    echo "$selected"
    return 0
  fi

  mapfile -t runs < <(nextflow log -q 2>/dev/null | sed '/^[[:space:]]*$/d')
  if [[ ${#runs[@]} -eq 0 ]]; then
    echo ""
    return 0
  fi
  latest="${runs[$(( ${#runs[@]} - 1 ))]}"

  if [[ "$RESUME_POLICY" == "latest" ]]; then
    echo "$latest"
    return 0
  fi

  for ((i=${#runs[@]}-1; i>=0; i--)); do
    run_name="${runs[$i]}"
    if has_tasks_for_run "$run_name"; then
      selected="$run_name"
      break
    fi
  done

  if [[ -n "$selected" ]]; then
    echo "$selected"
  else
    echo "$latest"
  fi
}

sanitize_nextflow_args

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Config file not found: $CONFIG_FILE" >&2
  exit 1
fi
CONFIG_FILE="$(realpath "$CONFIG_FILE")"
CONFIG_DIR="$(dirname "$CONFIG_FILE")"

resolve_config_path() {
  local value="$1"
  if [[ "$value" = /* ]]; then
    realpath -m "$value"
  else
    realpath -m "${CONFIG_DIR}/${value}"
  fi
}

OUTPUT_DIR=$(yq -r '.paths.output_dir // empty' "$CONFIG_FILE")
RUNTIME_DIR=$(yq -r '.paths.runtime_dir // empty' "$CONFIG_FILE")
KEEP_RUNTIME_DIR=$(yq -r '.paths.keep_runtime_dir // true' "$CONFIG_FILE")
WORK_DIR=$(yq -r '.paths.work_dir // empty' "$CONFIG_FILE")
CONDA_CACHE_DIR=$(yq -r '.paths.conda_cache_dir // empty' "$CONFIG_FILE")

if [[ -z "$OUTPUT_DIR" ]]; then
  echo "paths.output_dir must be set in $CONFIG_FILE" >&2
  exit 1
fi

OUTPUT_DIR="$(resolve_config_path "$OUTPUT_DIR")"
if [[ -z "$RUNTIME_DIR" || "$RUNTIME_DIR" == "null" ]]; then
  RUNTIME_DIR="${OUTPUT_DIR}/.aspire"
else
  RUNTIME_DIR="$(resolve_config_path "$RUNTIME_DIR")"
fi
if [[ -z "$WORK_DIR" || "$WORK_DIR" == "null" ]]; then
  WORK_DIR="${RUNTIME_DIR}/nf_work"
else
  WORK_DIR="$(resolve_config_path "$WORK_DIR")"
fi
if [[ -z "$CONDA_CACHE_DIR" || "$CONDA_CACHE_DIR" == "null" ]]; then
  CONDA_CACHE_DIR="${RUNTIME_DIR}/conda_cache"
else
  CONDA_CACHE_DIR="$(resolve_config_path "$CONDA_CACHE_DIR")"
fi
PUBLICATION_STAGING_DIR="${RUNTIME_DIR}/publication_staging"

case "${KEEP_RUNTIME_DIR,,}" in
  true|false) ;;
  *)
    echo "paths.keep_runtime_dir must be true or false: ${KEEP_RUNTIME_DIR}" >&2
    exit 1
    ;;
esac

mkdir -p "$WORK_DIR"
mkdir -p "$CONDA_CACHE_DIR"
mkdir -p "${CONDA_CACHE_DIR}/pkgs"
mkdir -p \
  "${OUTPUT_DIR}/modules" \
  "${OUTPUT_DIR}/intermediates" \
  "${OUTPUT_DIR}/references" \
  "${OUTPUT_DIR}/summary" \
  "${OUTPUT_DIR}/logs"

# One workflow may own a Conda cache at a time. This makes orphaned Nextflow
# environment markers safe to remove after an interrupted run.
exec 7>"${CONDA_CACHE_DIR}/.aspire-run.lock"
if ! flock -n 7; then
  echo "Another ASPIRE run is actively using Conda cache: ${CONDA_CACHE_DIR}" >&2
  echo "Wait for that run to finish or configure a different paths.conda_cache_dir." >&2
  exit 1
fi
stale_env_locks=()
while IFS= read -r -d '' stale_lock; do
  stale_env_locks+=("$stale_lock")
done < <(find "$CONDA_CACHE_DIR" -maxdepth 1 -type f -name '.env-*.lock' -print0)
if (( ${#stale_env_locks[@]} > 0 )); then
  rm -f "${stale_env_locks[@]}"
  echo "[controller] Removed ${#stale_env_locks[@]} stale Nextflow Conda environment lock marker(s)."
fi
if [[ "$RESUME_ENABLED" -eq 0 && -d "$PUBLICATION_STAGING_DIR" ]]; then
  publication_staging_real="$(realpath -m "$PUBLICATION_STAGING_DIR")"
  runtime_dir_real="$(realpath -m "$RUNTIME_DIR")"
  if [[ "$publication_staging_real" == "$runtime_dir_real"/* ]]; then
    rm -rf "$publication_staging_real"
  else
    echo "Refusing to clear publication staging outside runtime directory: ${publication_staging_real}" >&2
    exit 1
  fi
fi
mkdir -p "$PUBLICATION_STAGING_DIR"
mkdir -p "${PUBLICATION_STAGING_DIR}/logs"
echo "[controller] Runtime directory: ${RUNTIME_DIR}"
echo "[controller] Publication staging directory: ${PUBLICATION_STAGING_DIR}"

has_nextflow_arg() {
  local expected="$1"
  local arg
  for arg in "${NEXTFLOW_ARGS[@]}"; do
    if [[ "$arg" == "$expected" || "$arg" == "${expected}="* ]]; then
      return 0
    fi
  done
  return 1
}

DEFAULT_NEXTFLOW_REPORT_ARGS=()
if ! has_nextflow_arg -with-report; then
  DEFAULT_NEXTFLOW_REPORT_ARGS+=(-with-report "${PUBLICATION_STAGING_DIR}/logs/nextflow_report.html")
fi
if ! has_nextflow_arg -with-timeline; then
  DEFAULT_NEXTFLOW_REPORT_ARGS+=(-with-timeline "${PUBLICATION_STAGING_DIR}/logs/nextflow_timeline.html")
fi
if ! has_nextflow_arg -with-trace; then
  DEFAULT_NEXTFLOW_REPORT_ARGS+=(-with-trace "${PUBLICATION_STAGING_DIR}/logs/nextflow_trace.tsv")
fi
if ! has_nextflow_arg -with-dag; then
  DEFAULT_NEXTFLOW_REPORT_ARGS+=(-with-dag "${PUBLICATION_STAGING_DIR}/logs/nextflow_dag.html")
fi

# Avoid collisions when reusing staging after a failed or retained run.
rm -f \
  "${PUBLICATION_STAGING_DIR}/logs/nextflow_report.html" \
  "${PUBLICATION_STAGING_DIR}/logs/nextflow_timeline.html" \
  "${PUBLICATION_STAGING_DIR}/logs/nextflow_trace.tsv" \
  "${PUBLICATION_STAGING_DIR}/logs/nextflow_dag.html"

printf '%q ' "$SCRIPT_PATH" "${ORIGINAL_ARGS[@]}" \
  > "${PUBLICATION_STAGING_DIR}/logs/launch_command.txt"
printf '\n' >> "${PUBLICATION_STAGING_DIR}/logs/launch_command.txt"
nextflow -version > "${PUBLICATION_STAGING_DIR}/logs/nextflow_version.txt" 2>&1

export NXF_WORK="$WORK_DIR"
export NXF_CONDA_CACHEDIR="$CONDA_CACHE_DIR"
export CONDA_PKGS_DIRS="${CONDA_CACHE_DIR}/pkgs"
export CONDA_REMOTE_MAX_RETRIES="${CONDA_REMOTE_MAX_RETRIES:-5}"
export CONDA_REMOTE_BACKOFF_FACTOR="${CONDA_REMOTE_BACKOFF_FACTOR:-2}"
export CONDA_REMOTE_CONNECT_TIMEOUT_SECS="${CONDA_REMOTE_CONNECT_TIMEOUT_SECS:-20}"
export CONDA_REMOTE_READ_TIMEOUT_SECS="${CONDA_REMOTE_READ_TIMEOUT_SECS:-120}"

ASPIRE_REAL_MAMBA="$(command -v mamba)"
if [[ -z "$ASPIRE_REAL_MAMBA" || ! -x "$ASPIRE_REAL_MAMBA" ]]; then
  echo "Controller environment does not provide a usable mamba executable." >&2
  exit 1
fi
export ASPIRE_REAL_MAMBA
export ASPIRE_MAMBA_LOCK_FILE="${TMPDIR:-/tmp}/aspire-mamba-${UID}.lock"
export ASPIRE_MAMBA_LOCK_TIMEOUT="${ASPIRE_MAMBA_LOCK_TIMEOUT:-1800}"
export ASPIRE_MAMBA_BUILD_TIMEOUT="${ASPIRE_MAMBA_BUILD_TIMEOUT:-900}"
export PATH="${SCRIPT_DIR}/processes/controller/bin:${PATH}"

BASELINE_RUN="$(select_baseline_run)"
if [[ -n "$BASELINE_RUN" ]]; then
  echo "[controller] Baseline run for cache/history lookup: ${BASELINE_RUN} (policy: ${RESUME_POLICY})"
else
  echo "[controller] No prior Nextflow run history found."
fi

if [[ -n "$RERUN_FROM" ]]; then
  RERUN_FROM_UPPER="$(printf '%s' "$RERUN_FROM" | tr '[:lower:]' '[:upper:]')"
  RERUN_FROM_CANONICAL="${PROCESS_ALIASES[$RERUN_FROM_UPPER]:-$RERUN_FROM_UPPER}"
  start_idx=-1
  for i in "${!PROCESS_ORDER[@]}"; do
    if [[ "${PROCESS_ORDER[$i]}" == "$RERUN_FROM_CANONICAL" ]]; then
      start_idx=$i
      break
    fi
  done

  if [[ $start_idx -lt 0 ]]; then
    echo "Unknown process for --rerun-from: $RERUN_FROM" >&2
    echo "Use --list-stages to see valid process names." >&2
    exit 1
  fi

  declare -A RERUN_STAGE_SET=()
  for ((i=start_idx; i<${#PROCESS_ORDER[@]}; i++)); do
    RERUN_STAGE_SET["${PROCESS_ORDER[$i]}"]=1
  done
  for alias_name in "${!PROCESS_ALIASES[@]}"; do
    canonical_name="${PROCESS_ALIASES[$alias_name]}"
    if [[ -n "${RERUN_STAGE_SET[$canonical_name]:-}" ]]; then
      RERUN_STAGE_SET["$alias_name"]=1
    fi
  done

  if [[ -z "$BASELINE_RUN" ]]; then
    echo "[controller] --rerun-from ${RERUN_FROM_UPPER} (${RERUN_FROM_CANONICAL}): no previous Nextflow run history found; proceeding without cache cleanup"
  else
    set +e
    task_rows="$(nextflow log "$BASELINE_RUN" -f 'process,workdir,status' 2>&1)"
    task_rows_rc=$?
    set -e
    if [[ $task_rows_rc -ne 0 ]]; then
      echo "[controller] Failed to inspect prior run tasks for cache cleanup (run: ${BASELINE_RUN})." >&2
      echo "[controller] nextflow log error:" >&2
      echo "${task_rows}" >&2
      echo "[controller] If another pipeline is currently running in this project, stop it and retry." >&2
      exit 1
    fi

    mapfile -t candidate_workdirs < <(
      while IFS=$'\t' read -r process_name workdir _status; do
        [[ -n "$process_name" ]] || continue
        process_key="$process_name"
        if [[ "$process_key" == *:* ]]; then
          process_key="${process_key##*:}"
        fi
        [[ -n "${RERUN_STAGE_SET[$process_name]:-}" || -n "${RERUN_STAGE_SET[$process_key]:-}" ]] || continue
        [[ -n "$workdir" && "$workdir" != "-" ]] || continue
        printf '%s\n' "$workdir"
      done <<< "$task_rows" | sort -u
    )

    if [[ ${#candidate_workdirs[@]} -eq 0 ]]; then
      echo "[controller] --rerun-from ${RERUN_FROM_UPPER} (${RERUN_FROM_CANONICAL}): no prior work directories found to invalidate"
    else
      work_root_real="$(realpath "$WORK_DIR")"
      removed_count=0
      for workdir in "${candidate_workdirs[@]}"; do
        workdir_real="$(realpath -m "$workdir")"
        if [[ "$workdir_real" == "$work_root_real"/* ]]; then
          rm -rf "$workdir_real"
          removed_count=$((removed_count + 1))
        else
          echo "[controller] Skipping unsafe work directory outside work root: ${workdir_real}" >&2
        fi
      done
      echo "[controller] Forcing rerun from ${RERUN_FROM_UPPER} (${RERUN_FROM_CANONICAL}) by invalidating ${removed_count} prior task work directories"
      echo "[controller] Cache remains enabled; successful rerun tasks will be reusable on future -resume runs"
    fi
  fi
fi

if [[ -n "$RERUN_FROM" ]]; then
  RERUN_CONFIG_FILE="$(mktemp /tmp/aspire-rerun-cache-XXXXXX.config)"
  {
    echo "process {"
    for stage_name in "${PROCESS_ORDER[@]}"; do
      [[ -n "${RERUN_STAGE_SET[$stage_name]:-}" ]] || continue
      printf "  withName: /(^|.*:)%s\$/ { cache = false }\n" "$stage_name"
    done
    echo "}"
  } > "$RERUN_CONFIG_FILE"
  echo "[controller] Generated temporary Nextflow config to disable cache from ${RERUN_FROM_UPPER} (${RERUN_FROM_CANONICAL}) onward: ${RERUN_CONFIG_FILE}"
fi

NEXTFLOW_RERUN_ARGS=()
if [[ -n "$RERUN_CONFIG_FILE" ]]; then
  NEXTFLOW_RERUN_ARGS=(-c "$RERUN_CONFIG_FILE")
fi

if [[ "$RESUME_ENABLED" -eq 1 ]]; then
  if [[ -n "$BASELINE_RUN" ]]; then
    RESUME_ARGS=(-resume "$BASELINE_RUN")
  else
    RESUME_ARGS=(-resume)
  fi
else
  RESUME_ARGS=()
  echo "[controller] Resume disabled for this run (--no-resume)."
fi

ASPIRE_PIPELINE_CONFIG="$CONFIG_FILE" \
SPARK_PIPELINE_CONFIG="$CONFIG_FILE" \
nextflow run "${SCRIPT_DIR}/asv_pipeline.nf" \
  --params-file "$CONFIG_FILE" \
  --pipeline_config "$CONFIG_FILE" \
  "${RESUME_ARGS[@]}" \
  "${NEXTFLOW_RERUN_ARGS[@]}" \
  -w "$NXF_WORK" \
  "${DEFAULT_NEXTFLOW_REPORT_ARGS[@]}" \
  "${NEXTFLOW_ARGS[@]}" 2>&1 | tee "${PUBLICATION_STAGING_DIR}/logs/controller.log"

COMPLETED_RUN="$(nextflow log -q 2>/dev/null | tail -n 1 || true)"
if [[ -n "$COMPLETED_RUN" ]]; then
  TASK_LOG_DIR="${PUBLICATION_STAGING_DIR}/logs/tasks"
  mkdir -p "$TASK_LOG_DIR"
  nextflow log "$COMPLETED_RUN" -f 'process,hash,workdir,status,exit,duration' \
    > "${PUBLICATION_STAGING_DIR}/logs/task_execution.tsv"
  while IFS=$'\t' read -r process_name task_hash task_workdir task_status task_exit task_duration; do
    [[ -n "$process_name" && -d "$task_workdir" ]] || continue
    safe_process="$(printf '%s' "$process_name" | sed 's/[^A-Za-z0-9._-]/_/g')"
    safe_hash="$(printf '%s' "$task_hash" | sed 's/[^A-Za-z0-9._-]/_/g')"
    destination="${TASK_LOG_DIR}/${safe_process}/${safe_hash}"
    mkdir -p "$destination"
    for log_name in .command.sh .command.out .command.err .command.log .command.trace .exitcode; do
      [[ -f "${task_workdir}/${log_name}" ]] || continue
      cp "${task_workdir}/${log_name}" "${destination}/${log_name#.}"
    done
  done < "${PUBLICATION_STAGING_DIR}/logs/task_execution.tsv"
fi

if [[ -f "${SCRIPT_DIR}/.nextflow.log" ]]; then
  cp "${SCRIPT_DIR}/.nextflow.log" "${PUBLICATION_STAGING_DIR}/logs/nextflow.log"
fi

python "${SCRIPT_DIR}/processes/output_layout/organize_outputs.py" \
  --staging-dir "$PUBLICATION_STAGING_DIR" \
  --output-dir "$OUTPUT_DIR" \
  --config "$CONFIG_FILE"

if [[ "${KEEP_RUNTIME_DIR,,}" == "true" ]]; then
  echo "[controller] Retaining Nextflow resume state: ${RUNTIME_DIR}"
else
  runtime_dir_real="$(realpath -m "$RUNTIME_DIR")"
  output_dir_real="$(realpath -m "$OUTPUT_DIR")"
  script_dir_real="$(realpath -m "$SCRIPT_DIR")"
  if [[ "$runtime_dir_real" == "/" ||
        "$output_dir_real" == "$runtime_dir_real" ||
        "$output_dir_real" == "$runtime_dir_real"/* ||
        "$script_dir_real" == "$runtime_dir_real" ||
        "$script_dir_real" == "$runtime_dir_real"/* ]]; then
    echo "Refusing to remove unsafe runtime directory: ${runtime_dir_real}" >&2
    exit 1
  fi
  rm -rf "$runtime_dir_real"
  echo "[controller] Removed runtime directory after successful publication: ${runtime_dir_real}"
fi
