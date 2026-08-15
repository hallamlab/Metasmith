#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTROL_ENV_DIR="${CONTROL_ENV_DIR:-${SCRIPT_DIR}/.controller_env}"

if ! command -v mamba >/dev/null 2>&1; then
  echo "mamba is required to create the ASPIRE controller environment." >&2
  exit 1
fi
if [[ ! -x "${CONTROL_ENV_DIR}/bin/python" ]]; then
  mamba env create --yes --prefix "${CONTROL_ENV_DIR}" \
    --file "${SCRIPT_DIR}/processes/controller/env.yml"
elif ! "${CONTROL_ENV_DIR}/bin/python" -c 'import pandas, yaml' >/dev/null 2>&1; then
  mamba env update --prune --prefix "${CONTROL_ENV_DIR}" \
    --file "${SCRIPT_DIR}/processes/controller/env.yml"
fi
exec conda run --no-capture-output -p "${CONTROL_ENV_DIR}" \
  python "${SCRIPT_DIR}/examples/mock_test/configure_mock_run.py" "$@"
