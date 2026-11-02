import os
import sys
from concurrent.futures import FIRST_EXCEPTION, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import get_context

STATE_FILE = "leiden_sweep.state"
RUNNING, COMPLETE, ERROR = "running", "complete", "error"

# One run_leiden call peaks at about WORKER_BASE plus BYTES_PER_CANDIDATE_EDGE for each of its contigs x max_edges
# neighbour pairs, measured on fir at 8K and 97K contigs. A spawned worker re-imports main.py, torch included.
WORKER_BASE = 320 << 20
BYTES_PER_CANDIDATE_EDGE = 170


def _log(message):
    print(f"comebin_leiden_sweep: {message}", file=sys.stderr, flush=True)


def _state_path():
    if "--output_path" not in sys.argv:
        return None
    return os.path.join(sys.argv[sys.argv.index("--output_path") + 1], STATE_FILE)


def _write_state(state):
    path = _state_path()
    if path:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path + ".tmp", "w") as f:
            f.write(state + "\n")
        os.replace(path + ".tmp", path)


def _read_state():
    path = _state_path()
    if path and os.path.exists(path):
        with open(path) as f:
            return f.read().strip()
    return None


def _memory_limit():
    limits = []
    try:
        with open("/proc/self/cgroup") as f:
            entries = [line.rstrip("\n").split(":", 2) for line in f]
    except OSError:
        entries = []
    for hierarchy, controllers, path in entries:
        if hierarchy == "0":
            root, name = "/sys/fs/cgroup", "memory.max"
        elif "memory" in controllers.split(","):
            root, name = "/sys/fs/cgroup/memory", "memory.limit_in_bytes"
        else:
            continue
        while True:
            try:
                with open(f"{root}{path.rstrip('/')}/{name}") as f:
                    value = f.read().strip()
                if value.isdigit():
                    limits.append(int(value))
            except OSError:
                pass
            if path in ("", "/"):
                break
            path = os.path.dirname(path)
    slurm = os.environ.get("SLURM_MEM_PER_NODE", "")
    if slurm.isdigit():
        limits.append(int(slurm) << 20)
    return min(limits) if limits else None


def _rss():
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) << 10
    return 0


# Under memory pressure the cgroup's OOM killer should take a worker, whose runs are redone, not the parent.
def _volunteer_for_oom_killer():
    try:
        with open("/proc/self/oom_score_adj", "w") as f:
            f.write("500")
    except OSError:
        pass


# Stands in for the multiprocessing.Pool that cluster.cluster runs its Leiden sweep through. A Pool whose worker
# is killed, as the cgroup OOM killer does, replaces the worker but never resolves the task it held, so join()
# waits forever. A ProcessPoolExecutor fails every outstanding future instead. The runs that did not finish are
# redone on half the workers, and a sweep that breaks even on one worker exits non-zero. Each run is independent
# and writes only its own file, so neither the worker count nor a rerun changes any output. Workers are spawned,
# not forked: k-means and hnswlib have started threads by then, and a child forked while one of them held a lock
# waits on it forever.
class LeidenSweep:
    def __init__(self, processes=None, *args, **kwargs):
        self._requested = processes or os.cpu_count()
        self._calls = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def apply_async(self, func, args=(), kwds=None, *rest, **kwrest):
        self._calls.append((func, tuple(args), dict(kwds or {})))

    def close(self):
        pass

    def terminate(self):
        pass

    def _initial_workers(self):
        if not self._calls:
            return self._requested
        limit = _memory_limit()
        if limit is None:
            _log(f"{len(self._calls)} runs on {self._requested} workers: no memory limit found")
            return self._requested
        _, args, _ = self._calls[0]
        contigs, max_edges = len(args[1]), args[5]
        per_worker = WORKER_BASE + BYTES_PER_CANDIDATE_EDGE * contigs * max_edges
        parent = _rss()
        spare = limit - parent - limit // 10
        workers = max(1, min(self._requested, spare // per_worker))
        _log(f"{len(self._calls)} runs on {workers} of {self._requested} workers: limit {limit / 2**30:.1f} GB, "
             f"parent {parent / 2**30:.1f} GB, ~{per_worker / 2**30:.2f} GB per worker for {contigs} contigs")
        return workers

    def join(self):
        remaining = self._calls
        workers = self._initial_workers()
        while remaining:
            with ProcessPoolExecutor(workers, mp_context=get_context("spawn"), initializer=_volunteer_for_oom_killer) as pool:
                futures = {pool.submit(func, *args, **kwds): (func, args, kwds) for func, args, kwds in remaining}
                wait(futures, return_when=FIRST_EXCEPTION)
                failure = next((f.exception() for f in futures if f.done() and f.exception() is not None
                                and not isinstance(f.exception(), BrokenProcessPool)), None)
                if failure is not None:
                    for f in futures:
                        f.cancel()
                    raise failure
                wait(futures)
            lost = [call for f, call in futures.items() if f.exception() is not None]
            if lost and workers == 1:
                raise BrokenProcessPool(f"a Leiden worker died running alone; {len(lost)} runs unfinished")
            if lost:
                workers = max(1, workers // 2)
                _log(f"a worker died; redoing {len(lost)} unfinished runs on {workers} workers")
            remaining = lost
        _write_state(COMPLETE)


def _record_error(exc_type, exc, tb):
    if not isinstance(exc, BrokenProcessPool):
        _write_state(ERROR)
    sys.__excepthook__(exc_type, exc, tb)


# The state file tells what became of the sweep: "running" means the bin step was killed or ran out of memory,
# which more memory may cure, while "error" is an exception that would recur.
def apply_to_bin():
    import multiprocessing

    _write_state(RUNNING)
    sys.excepthook = _record_error
    multiprocessing.Pool = LeidenSweep


# run_comebin.sh runs get_result whatever the bin step's exit status, and get_result would pick the best of an
# unfinished sweep's results without a word.
def require_finished_sweep():
    state = _read_state()
    if state not in (None, COMPLETE):
        _log(f"the bin step's Leiden sweep is {state}, not complete; refusing to pick a result from it")
        sys.stderr.flush()
        os._exit(1)
