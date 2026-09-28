#!/bin/bash
# Copy the private CPLEX Python runtime to node-local scratch once per job,
# so `carveme_from_orfs_cplex.py` reads cplex/docplex's .so and .py files off
# the node's own disk instead of Lustre on every gapfill call -- the same
# problem COMEBin's GPU arm solves for its own inputs in
# `research/cami/run_cami_metag.py`'s `make_slurm_config` (a `beforeScript`
# appended to that process's Nextflow config, run on the compute node before
# the container starts).
#
# Usage, as a `beforeScript` body (a driver wires this in; nothing here does):
#
#   beforeScript = 'bash <this script> <lustre_cplex_installation_dir>'
#
# and the driver then registers `modelling::cplex_installation` at
# `$SLURM_TMPDIR/cplex_installation` (or `$TMPDIR/cplex_installation` off
# the login node) rather than at the Lustre path directly, so the bind
# `carveme_from_orfs_cplex.py` sets up points at the node-local copy.
#
# rsync, not cp -r: idempotent across `--tries` retries on the same node, and
# the CPLEX runtime is ~40 MB (cplex + docplex), so re-running this costs
# nothing when the copy is already current.
set -euo pipefail

SRC=$1
DEST_ROOT=${SLURM_TMPDIR:-${TMPDIR:-/tmp}}
DEST="$DEST_ROOT/cplex_installation"

mkdir -p "$DEST"
rsync -a --delete "$SRC"/ "$DEST"/
echo "$DEST"
