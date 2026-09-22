# Sourced by submit_driver.sbatch: the client's Python (make_client_env.sh builds it).
# CAUTION PYTHONNOUSERSITE keeps ~/.local's packages from shadowing the env's.
module load python/3.12.4 apptainer/1.3.5
export BENCH_PYTHON="${BENCH_CLIENT_ENV:-$HOME/bench_client_env}/bin/python"
export PYTHONNOUSERSITE=1
