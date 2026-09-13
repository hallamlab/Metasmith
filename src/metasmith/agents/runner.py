from __future__ import annotations

import json
import os
import re
import shutil
import time
import threading
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

from ..constants import AgentPaths, MODULE_PATH, VERSION
from ..coms.terminals import LiveShell
from ..env import Environment, Rootfs
from ..logging import Log
from ..models.libraries import DataInstanceLibrary
from ..models.paths import PathMap
from ..models.remote import GlobusSource, Logistics, Source
from ..models.workflow import (
    NextflowGenContext, NextflowProcessName, WorkflowTask, restat_leaf_ids,
)
from ..serialization import StdTime
from .agent import Agent
from .collect import CollectResults

def _rewrite_staged_plan(task_path: Path, task: WorkflowTask):
    doc_path = task_path/"task.yml"
    with open(doc_path) as f:
        doc = yaml.safe_load(f)
    doc["plan"] = task.plan.Pack()
    tmp = doc_path.with_name(f"{doc_path.name}.{os.getpid()}.part")
    with open(tmp, "w") as f:
        yaml.dump(doc, f)
    os.replace(tmp, doc_path)


# TERM -> KILL window for nextflow's own process group when a run is cancelled.
# Nextflow's shutdown hook is what reaches `bin/scancel` for grid jobs, so this
# has to outlast a JVM draining a full submission queue, not merely outlast
# process exit.
NXF_SHUTDOWN_GRACE_S = 60

def RenderLauncher(
    task_key: str, setup_commands: list[str], binds: str, background: bool = True,
    workdir: str | None = None,
) -> str:
    # start.sh: the root of a run. Everything the run consists of descends from
    # the process it backgrounds, and carries the token it exports.
    #
    # `background=False` is for a caller that is itself already a durable host
    # for the run -- a Slurm batch job's own script, not a login-node SSH
    # session -- so there is nothing to free by backgrounding and returning.
    # It runs the driver in the foreground instead, so the job's own wall is
    # what keeps it alive rather than a login-node session that supervises
    # nothing and records nothing. `$$` under `set -m` is this script's own
    # pgid, the same handle a backgrounded job's `$!` would have given, so
    # RUN.pgid still names something signalable on whichever host ran it.
    #
    # Note RUN.pgid is not sufficient on its own: a run's JVM gets reparented
    # to init and then survives a kill of that process group, still holding its
    # heap and still submitting work. An empty `squeue` does not mean a run has
    # stopped either. RUN.slurmjob, written by the caller, is the handle that
    # actually answers whether the driver is alive.
    # Foreground mode writes RUN.pgid and the token/pgid echo *before* the
    # (possibly days-long) run, since there is no backgrounding step to hand
    # back a `$!` afterwards -- written after, both would sit undone for the
    # run's whole duration, which is exactly the file the recovery path reads.
    if background:
        tail = [
            f'nohup ../../msm api run_workflow -a key={task_key} host=$(hostname) log_dir=$LOG_DIR stub_delay=${{1:-0}} </dev/null >$LOG_DIR/agent.log 2>&1 &',
            f'RUN_PGID=$!',
            f'set +m',
            f'echo "$RUN_PGID" > ./{AgentPaths.RUN_PGID_FILE}',
            f'echo "run token is [${AgentPaths.RUN_TOKEN_ENV}], run pgid is [$RUN_PGID]"',
        ]
    else:
        tail = [
            f'RUN_PGID=$$',
            f'set +m',
            f'echo "$RUN_PGID" > ./{AgentPaths.RUN_PGID_FILE}',
            f'echo "run token is [${AgentPaths.RUN_TOKEN_ENV}], run pgid is [$RUN_PGID]"',
            f'../../msm api run_workflow -a key={task_key} host=$(hostname) log_dir=$LOG_DIR stub_delay=${{1:-0}} >$LOG_DIR/agent.log 2>&1',
        ]
    # `$BASH_SOURCE`-relative `cd` resolves the *staged* script's own path --
    # right for an interactive exec, wrong under sbatch, which copies the
    # script to a per-job spool directory first (the same trap recorded
    # against `verify.sbatch`: `$(dirname "$0")` resolves to nothing useful
    # there). `--chdir` on the submission already lands the job in the run's
    # workspace, so the foreground variant takes that literal path instead of
    # re-deriving it from a location that is about to be someone else's spool
    # dir.
    cd_line = (
        'cd $( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )'
        if background else
        f'cd "{workdir}"'
    )
    return "\n".join([
        f'#!/bin/bash',
        cd_line,
        "# >>> agent setup commands",
    ]+list(setup_commands)+[
        "# <<<",
        f'TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")',
        f'LOG_DIR="./{AgentPaths.INTERNALS}/logs.$TIMESTAMP"',
        f'LOG_LATEST="./{AgentPaths.INTERNALS}/logs.latest"',
        f'mkdir -p $LOG_DIR',
        f'[ -e $LOG_LATEST ] && rm "$LOG_LATEST"; ln -s "./logs.$TIMESTAMP" "$LOG_LATEST"',
        f"[ -e {AgentPaths.NXF_PARAMS} ] || echo '{{}}' > {AgentPaths.NXF_PARAMS}",
        f'[ -e {AgentPaths.NXF_CONFIG} ] || touch {AgentPaths.NXF_CONFIG}',
        f'echo "start time was [$TIMESTAMP]"',
        f'export BINDS="{binds}"',
        f'export OPENBLAS_NUM_THREADS=1',
        f'export OMP_NUM_THREADS=1',
        # The run's identity, inherited by every descendant. `set -m` gives the
        # backgrounded driver a fresh process group -- the portable way, since
        # setsid(1) is util-linux and absent on macOS -- so $! is the run's pgid.
        # The token is the backstop for anything that later setsid()s out of it.
        # The timestamp is the one already naming logs.$TIMESTAMP, so a leak
        # traces back to a single run directory.
        f'export {AgentPaths.RUN_TOKEN_ENV}="{task_key}.$TIMESTAMP"',
        f'echo "${AgentPaths.RUN_TOKEN_ENV}" > ./{AgentPaths.RUN_TOKEN_FILE}',
        f'set -m',
    ]+tail)


def RenderNextflowScript(
    *, workspace, log_dir, host: str, results_folder: str,
    nxf_report, nxf_dag, stub_param: str,
) -> str:
    # Nextflow, plus the supervisor that stops it. PID.lock holds nextflow's own
    # pgid (see `set -m` below), so removing the lock file stops the whole run
    # and leaves this driver alive to snapshot logs and promote the cache.
    return f"""
            cd {workspace}
            PIDF=./{AgentPaths.PID_LOCK_FILE}
            stop() {{
                [[ -e "$PIDF" ]] && rm $PIDF
                [ -e squeue.log ] && mv squeue.log {log_dir}
                [ -e scancel.log ] && mv scancel.log {log_dir}
                [ -e {AgentPaths.NXF_WORKFLOW} ] && cp {AgentPaths.NXF_WORKFLOW} {log_dir}
                [ -e {AgentPaths.NXF_CONFIG} ] && cp {AgentPaths.NXF_CONFIG} {log_dir}
                [ -e {AgentPaths.NXF_RES} ] && cp {AgentPaths.NXF_RES} {log_dir}
                [ -e {AgentPaths.NXF_PARAMS} ] && cp {AgentPaths.NXF_PARAMS} {log_dir}
                if [ -e {nxf_dag} ]; then
                    dot -Tsvg {nxf_dag} -o {nxf_dag.stem}.svg
                    rm {nxf_dag}
                fi
                exit 0
            }}
            trap stop EXIT

            export NXF_HOME=./.nextflow
            export NXF_ENABLE_VIRTUAL_THREADS=true
            export NXF_OFFLINE=TRUE # don't go online and search for latest version
            export OPENBLAS_NUM_THREADS=1
            export OMP_NUM_THREADS=1
            export NXF_OPTS="-Xms2g -Xmx10g -XX:ActiveProcessorCount=1 -Djdk.virtualThreadScheduler.maxPoolSize=512"
            set -m
            nextflow \
                -config ./{AgentPaths.NXF_RES} \
                -config ./{AgentPaths.NXF_CONFIG} \
                -log {log_dir}/nxf.log \
                run ./{AgentPaths.NXF_WORKFLOW} \
                -params-file ./{AgentPaths.NXF_PARAMS} \
                --hostName "{host}" \
                --output "{results_folder}" \
                -with-report {nxf_report} \
                -with-dag {nxf_dag} \
                -with-timeline {log_dir}/nxf_timeline.html \
                -with-trace {log_dir}/{AgentPaths.NXF_TRACE_FILE} \
                {stub_param} \
                -lib ./lib \
                -ansi-log false \
                -resume \
                -work-dir {workspace}/nxf_work &
            PID=$!
            set +m
            echo "nextflow PID is [$PID]"
            echo $PID >$PIDF
            while true; do
                if ! [[ -d "/proc/$PID" ]]; then
                    break
                fi
                if ! [[ -e "$PIDF" ]]; then
                    # $PID is a pgid: `set -m` above put nextflow in its own group,
                    # so this reaches the tools it spawned and not this supervisor.
                    # TERM first and wait -- the shutdown hook is what scancels grid
                    # jobs -- then KILL whatever is left of the group.
                    kill -TERM -$PID 2>/dev/null
                    for _ in $(seq {NXF_SHUTDOWN_GRACE_S}); do
                        [[ -d "/proc/$PID" ]] || break
                        sleep 1
                    done
                    kill -KILL -$PID 2>/dev/null
                    wait $PID 2>/dev/null
                    break
                fi
                sleep 1
            done
            """


def StageWorkflow(task_key: str, verify: bool, host: str, rootfs: Rootfs|None = None):
    agent = Agent.Load(AgentPaths.HOME_ROOT/"lib/agent.yml")
    task_path = agent.home.GetPath()/AgentPaths.to_task(task_key)
    assert task_path.exists(), f"task dir not found [{task_path}]"
    task = WorkflowTask.Load(task_path)
    Log.Info(f"staging workflow [{task._key}] with:")
    Log.Info(f"  [{len(task.data_libraries)}] data libraries")
    Log.Info(f"  [{len(task.transform_libraries)}] transform libraries")
    Log.Info(f"  [{len(task.plan.steps)}] total steps")

    work_relative = AgentPaths.STAGED/task_key
    work_dir = AgentPaths.WORK_ROOT/work_relative
    work_internals = work_dir/AgentPaths.INTERNALS
    data_dir = AgentPaths.to_data()
    data_dir.mkdir(parents=True, exist_ok=True)
    work_internals.mkdir(parents=True, exist_ok=True)
    _agent_env = Environment(image=agent.container, runtime=agent.runtime, native=agent.native)
    with _agent_env.ConnectShell(AgentPaths.to_local_relay_coms(host=host)) as extern_shell:
        extern_root = agent.real_path
        assert extern_root is not None
        path_map = PathMap(extern_home=Path(str(extern_root)), task_key=task._key)
        extern_work = path_map.extern_work
        workspace_str = f"{{AGENT_HOME}}/{path_map.extern_work.relative_to(extern_root)}"

        if not verify:
            Log.Info(f"skipping verification of external inputs paths")
        else:
            given_paths = [inst.ResolvePath() for inst in task.plan.given]
            # Container-internal paths cannot be stat'd from the external shell,
            # so they are skipped. Without a container boundary the home root IS
            # a real host path and every given path under it is verifiable --
            # applying the filter there would silently narrow verification.
            if _agent_env.needs_relay:
                given_paths = [p for p in given_paths if not p.is_relative_to(AgentPaths.HOME_ROOT)]
            def batchify(iterable: Iterable, n):
                batch: list[str] = []
                for x in iterable:
                    if len(batch) >= n:
                        yield batch
                        batch = []
                    batch.append(x)
                if len(batch) > 0: yield batch
            cmd = [
                f'[ -e "{p}" ] && echo "{p}"'
                for p in given_paths
            ]
            found = set()
            bs = 100
            batches = list(batchify(cmd, bs))
            for i, _batch in enumerate(batches):
                Log.Info(f"verifying [{(i*bs)+len(_batch)} of {len(cmd)}] external input paths")
                res = extern_shell.Exec(
                    cmd="\n".join(_batch),
                    history=True
                )
                found |= {Path(p) for p in res.out}
            missing_paths = [p for p in given_paths if p not in found]
            if len(missing_paths)>0:
                Log.Error(f"missing [{len(missing_paths)}] given data:")
                for p in missing_paths:
                    Log.Error(f"    {p}")
                Log.Error(f"staging failed, partial progress at [{workspace_str}]")
                return

    Log.Info(f"work [{work_dir}]")
    Log.Info(f"data [{data_dir}]")
    extern_data = extern_root/data_dir.name
    Log.Info(f"external work [{extern_work}]")
    Log.Info(f"external data [{extern_data}]")

    def move_remote_libs(libs: list[DataInstanceLibrary], dest: Path):
        processed_libs: list[DataInstanceLibrary] = []
        mover = Logistics()
        expected: list[Source] = []
        to_pull = [lib for lib in libs if lib.remote_src is not None]
        if len(to_pull)==0: return libs
        Log.Info(f"pulling [{len(to_pull)}] remote data libraries to [{data_dir}]")
        for lib in libs:
            if lib.remote_src is not None:
                lib_dest = dest/lib.location.name
                if not lib_dest.exists():
                    _dest = Source.FromLocal(lib_dest)
                    lib.PrepTransfer(_dest, mover=mover)
                    expected.append(_dest)
                lib.location = lib_dest
            processed_libs.append(lib)
        res = mover.ExecuteTransfers()
        _completed = {b.address for a, b in res.completed}
        for x in expected:
            assert x.address in _completed, f"failed to transfer [{x.address}]"
        return processed_libs
    task.data_libraries = move_remote_libs(task.data_libraries, data_dir)

    # This host owns the files; the client that minted their ids did not. Settle
    # identity here, and write it back so the plan on disk agrees with the ids
    # the codegen below is about to bake into the cache keys.
    restat_leaf_ids(task)
    _rewrite_staged_plan(task_path, task)

    Log.Info(f"compiling nextflow script")
    task.PrepareNextflow(NextflowGenContext(
        workflow_file=AgentPaths.NXF_WORKFLOW,
        work_dir=work_dir,
        external_work=extern_work,
        home_dir=AgentPaths.HOME_ROOT,
        external_home=agent.home.GetPath(),
        runtime=agent.runtime,
        resources_file=AgentPaths.NXF_RES,
        rootfs=rootfs,
    ))
    # Codegen's cache-decision pass stamps deterministic lineage ids onto the
    # plan's produce/require instances -- the ids baked into every .nf/.meta
    # file. Write the plan again so task.yml agrees with what execution will
    # actually see; otherwise a downstream step's dependency_map still carries
    # the pre-stamp id and lookups against the .meta payload miss.
    _rewrite_staged_plan(task_path, task)
    nxflib_dir = work_dir/"lib"
    nxflib_dir.mkdir(parents=True, exist_ok=True)
    orchestrator_lib = MODULE_PATH/"nextflow_config/Orchestrator.groovy"
    shutil.copy(orchestrator_lib, nxflib_dir/orchestrator_lib.name)

    launcher_path = work_dir/AgentPaths.LAUNCHER_FILE
    Log.Info(f"creating launcher script at [{launcher_path}]")
    mock = agent._get_mock_container(task)
    binds = mock.MakeBindsParam()
    if len(mock.container.binds)>0:
        Log.Info(f"external binds {[a for a, b in mock.container.binds]}")
    with open(launcher_path, "w") as f:
        f.write(RenderLauncher(task_key, agent.setup_commands, binds))
    os.chmod(launcher_path, 0o754)

    Log.Info(f"drawing DAG")
    task.plan.RenderDAG(f"{work_dir}/workflow.dag.svg")
    Log.Info(f"[{task._key}] staged to [{workspace_str}]")
        

def _extract_nxf_task_metadata(log_dir_abs: Path) -> "pd.DataFrame | None":
    tsv = log_dir_abs/AgentPaths.NXF_TRACE_FILE
    if tsv.exists():
        try:
            return pd.read_csv(tsv, sep="\t")
        except Exception as e:
            Log.Warn(f"failed to read [{tsv}] [{e}], falling back to HTML report")

    html = log_dir_abs/"nxf_report.html"
    if not html.exists():
        return None
    try:
        raw = None
        with open(html) as f:
            found = False
            for l in f:
                if l.strip().startswith('window.data = { "trace":['):
                    found = True
                    continue
                if not found:
                    continue
                # Nextflow embeds the trace as a JS object literal.
                # Single quotes in `.command.sh` arrive here as `\'`,
                # which json.loads rejects. Stripping the backslash
                # yields a valid JSON string (single quotes don't need
                # escaping in JSON).
                sanitized = l[:-2].replace("\\'", "'")
                raw = json.loads('{ "trace":[' + sanitized).get("trace")
                break
        if raw is None:
            return None
        return pd.DataFrame(raw)
    except Exception as e:
        Log.Warn(f"failed to parse task metadata from [{html}] [{e}]")
        return None


# How long a run has to be silent before the driver says anything, and how much
# longer each time after that. Cumulative, so a run that never speaks beats at
# 5, 20, 50, 110 and then 170 minutes and hourly after -- about fifteen lines
# over a twelve-hour InterProScan step, not one every five minutes.
HEARTBEAT_GAPS_S = (5 * 60, 15 * 60, 30 * 60, 60 * 60)
_HEARTBEAT_POLL_S = 20.0


def heartbeat_marks(silence_s: float, gaps=HEARTBEAT_GAPS_S) -> list[float]:
    """The seconds-of-silence at which a run silent for `silence_s` speaks."""
    marks: list[float] = []
    at, i = gaps[0], 0
    while at <= silence_s:
        marks.append(at)
        i = min(i + 1, len(gaps) - 1)
        at += gaps[i]
    return marks


def _running_tasks(workspace: Path, task) -> list[tuple[str, float]]:
    # Nextflow's own trace file only lands a row when a task finishes, so it
    # cannot answer "what is running". The work directory can: a task that has
    # begun and has no exit code yet is running, and its `.command.begin` is
    # when it started.
    now = time.time()
    found: list[tuple[str, float]] = []
    for begun in (workspace/"nxf_work").glob("*/*/.command.begin"):
        d = begun.parent
        if (d/".exitcode").exists(): continue
        name = d.name[:6]
        try:
            with open(d/".command.log") as f:
                first = f.readline()
            order = [int(x) for x in re.findall(r"\d+", first)][0]
            name = NextflowProcessName(order, task.plan.steps[order-1].transform.name)
        except Exception:
            pass
        try:
            found.append((name, max(0.0, now - begun.stat().st_mtime)))
        except OSError:
            continue
    return sorted(found, key=lambda kv: -kv[1])


def _heartbeat_line(workspace: Path, task, silent_s: float) -> str:
    running = _running_tasks(workspace, task)
    quiet = f"nextflow has said nothing for {silent_s/60:.0f} min"
    if not running:
        return f"{quiet}, and no step is running"
    names = ", ".join(n for n, _ in running[:4])
    if len(running) > 4:
        names += f" (+{len(running)-4} more)"
    return (
        f"{quiet}; [{len(running)}] step(s) still running, longest "
        f"{running[0][1]/60:.0f} min: {names}"
    )


class _Heartbeat:
    """Says whether a quiet run is still working, and does it rarely.

    Silence-triggered rather than periodic: a run nextflow is narrating emits
    nothing at all, because there is nothing to add. The reporter watched a
    ninety-five minute run print nothing, because `wget -q` says nothing and
    the agent log is exactly as chatty as nextflow is.
    """

    def __init__(self, shell, workspace: Path, task):
        self._shell = shell
        self._workspace = workspace
        self._task = task
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self):
        self._thread = threading.Thread(
            target=self._loop, name="msm-run-heartbeat", daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        return False

    def _loop(self):
        gap_i, next_at, seen, last = 0, HEARTBEAT_GAPS_S[0], 0.0, None
        while not self._stop.wait(_HEARTBEAT_POLL_S):
            try:
                silent = self._shell.SecondsSinceRead()
            except Exception:
                return
            if silent < seen:
                # nextflow spoke; the next silence starts from the shortest gap
                gap_i, next_at = 0, HEARTBEAT_GAPS_S[0]
            seen = silent
            if silent < next_at:
                continue
            try:
                line = _heartbeat_line(self._workspace, self._task, silent)
            except Exception:
                line = None
            if line and line != last:
                Log.Info(line)
                last = line
            gap_i = min(gap_i + 1, len(HEARTBEAT_GAPS_S) - 1)
            next_at = silent + HEARTBEAT_GAPS_S[gap_i]


_LINEAGE_POLL_S = 30.0


class _LineageReport:
    """Keeps the lineage report current while nextflow runs.

    Everything the run writes after nextflow exits is lost when the driver is
    killed first; a report rewritten on a tick keeps what the run reached. It
    only reads, and nothing it raises reaches the run.
    """

    def __init__(self, workspace: Path, task, *, extern_home: Path, out_dir: Path):
        self._args = dict(workspace=workspace, plan=task.plan, out_dir=out_dir)
        self._extern_home = extern_home
        self._report = None
        self._seen = None
        self._warned: set[str] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _warn(self, msg: str):
        if msg not in self._warned:
            self._warned.add(msg)
            Log.Warn(f"lineage report: {msg}")

    def _guarded(self, fn):
        try:
            return fn()
        except Exception as e:
            self._warn(f"{type(e).__name__}: {e}")
            return None

    def _open(self):
        from ..caching.layout import default_cache_root
        from .lineage_report import LineageReport

        self._report = LineageReport(
            cache_root=default_cache_root(self._extern_home), **self._args,
        )
        self._report.write_parents()

    def refresh(self, force: bool = False):
        if self._report is None:
            return None
        with self._lock:
            # Taken before the rebuild, so a task finishing mid-rebuild still
            # counts as a change on the next tick.
            fingerprint = self._report.fingerprint()
            if not force and fingerprint == self._seen:
                return None
            n_rows, truncated = self._report.rebuild()
            self._seen = fingerprint
        if truncated:
            self._warn(f"truncated at [{n_rows}] rows")
        return n_rows

    def __enter__(self):
        self._guarded(self._open)
        self._guarded(lambda: self.refresh(force=True))
        self._thread = threading.Thread(
            target=self._loop, name="msm-run-lineage", daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        n_rows = self._guarded(lambda: self.refresh(force=True))
        if n_rows is not None:
            Log.Info(f"lineage report [{n_rows}] row(s) in [{self._args['out_dir']}]")
        return False

    def _loop(self):
        while not self._stop.wait(_LINEAGE_POLL_S):
            self._guarded(self.refresh)


def _post_run_step(name: str, failures: list[str], fn):
    # One broken post-run step must not cost the run the steps after it, nor
    # the sentinel that says how the run ended.
    try:
        return fn()
    except Exception as e:
        Log.Error(f"post-run step [{name}] failed: {type(e).__name__}: {e}")
        failures.append(name)
        return None


_FAILED_STATES = {"FAILED", "ABORTED"}


def _failed_steps(df_tasks: "pd.DataFrame | None") -> list[str]:
    # A process is failed if nextflow said so, or if it "completed" with a
    # non-zero code -- the shipped presets end their errorStrategy in `ignore`,
    # which leaves the row behind and carries on.
    if df_tasks is None or len(df_tasks) == 0:
        return []
    names: list[str] = []
    for _, row in df_tasks.iterrows():
        status = str(row.get("status", "")).strip().upper()
        try:
            code = int(str(row.get("exit", "")).strip())
        except (TypeError, ValueError):
            code = 0
        if status in _FAILED_STATES or code != 0:
            name = str(row.get("name", "")).strip()
            if name and name not in names:
                names.append(name)
    return names


def RunWorkflow(key: str, log_dir: Path, host: str, stub_delay: float):
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}]"

    task = WorkflowTask.Load(task_path, alt_data_paths=[AgentPaths.to_data()])
    start_time = log_dir.name.split(".")[-1]
    Log.Info(f"Metasmith version [{VERSION}]")
    Log.Info(f"running workflow [{task._key}]")
    Log.Info(f"start time was [{start_time}]")

    Log.Info(f"loading agent metadata")
    agent = Agent.Load(AgentPaths.to_definition())
    extern_home = agent.home.GetPath()
    path_map = PathMap(extern_home=Path(str(extern_home)), task_key=key)
    extern_workspace = path_map.extern_work
    (workspace/log_dir).mkdir(parents=True, exist_ok=True)
    MAIN_LOG = workspace/log_dir/AgentPaths.MAIN_LOG_FILE
    Log.AddLogFile(MAIN_LOG)

    Log.Info(f"workspace [{workspace}]")
    Log.Info(f"external workspace [{extern_workspace}]")
    Log.Info(f"workflow steps [{len(task.plan.steps)}]")
    samples = max(len(instances) for step in task.plan.steps for instances in step.dependency_map.values())
    Log.Info(f"samples estimate [{samples}]")

    if agent.globus_uuid is not None:
        Log.Info(f"locating input data with agent's globus endpoint [{agent.globus_uuid}]")
        dest_base = GlobusSource(endpoint=agent.globus_uuid, path=Path("/")).AsSource()
    else:
        Log.Info(f"locating input data with personal globus endpoint")
        dest_base = Source.FromLocal("/")
    for lib in task.transform_libraries+task.data_libraries:
        _name = lib.location.name
        if lib.remote_src is None:
            Log.Info(f"[{_name}] is at [{lib.location}]")
        else:
            _extern_location = path_map.LocalToExternal(lib.location)
            Log.Info(f"[{_name}] at [{lib.location}] is remote [{lib.remote_src.address}], downloading to [{_extern_location}]")
            dest = dest_base/str(_extern_location)
            lib.ActualizeRemote(extern_dest=dest, label=f"msm_staging.{_name}")

    results_folder = "results"
    nxf_report = log_dir/"nxf_report.html"
    nxf_dag = log_dir/"workflow.dag_nxf.dot"
    output_path = workspace/results_folder
    if output_path.exists(): shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    # Not a new session: nextflow belongs to the run this driver is, so it stays
    # in the run's session rather than escaping where a reap cannot see it.
    with LiveShell(new_session=False) as shell:
        shell.RegisterOnOut(Log.Info)
        shell.RegisterOnErr(Log.Error)
        Log.Info(f"calling nextflow from container")
        stub_param = f"-stub --testSpread={stub_delay:0.3f}" if stub_delay>0 else ""
        with _Heartbeat(shell, workspace, task), _LineageReport(
            workspace, task, extern_home=Path(str(extern_home)), out_dir=workspace/log_dir,
        ):
            shell.Exec(
                RenderNextflowScript(
                    workspace=workspace, log_dir=log_dir, host=host,
                    results_folder=results_folder, nxf_report=nxf_report,
                    nxf_dag=nxf_dag, stub_param=stub_param,
                ),
                timeout=None,
            )

    post_run_failures: list[str] = []

    df_tasks = _post_run_step(
        "task metadata", post_run_failures,
        lambda: _extract_nxf_task_metadata(workspace/log_dir),
    )

    def _write_task_metadata():
        nxf_task_meta = workspace/log_dir/"nxf_tasks.csv"
        df_tasks.to_csv(nxf_task_meta, index=False)
        Log.Info(f"extracted task metadata to [{nxf_task_meta}]")

    if df_tasks is not None:
        _post_run_step("task metadata", post_run_failures, _write_task_metadata)
    else:
        Log.Warn(f"no task metadata extracted from [{workspace/log_dir}]")

    failed_steps = _failed_steps(df_tasks)
    if failed_steps:
        Log.Error(
            f"[{len(failed_steps)}] step(s) failed and were ignored, so their"
            f" products are missing: {', '.join(failed_steps)}"
        )

    try:
        from ..caching.layout import default_cache_root
        from ..caching.promote import record_run

        cache_log: list = []
        summary = record_run(
            workspace=workspace, cache_root=default_cache_root(Path(str(extern_home))),
            log=cache_log,
        )
        for level, msg in cache_log:
            (Log.Warn if level == "warn" else Log.Info)(f"cache: {msg}")
        Log.Info(
            f"cache: {len(summary['promoted'])} member(s) promoted, "
            f"{len(summary['hits'])} served from shards"
        )
    except Exception as e:
        Log.Warn(f"cache record failed: {e}")

    # `_metasmith/trace.jsonl` sits at the workspace root and gets truncated
    # on the next stage. Copy it alongside nxf_tasks.csv, into the one per-run
    # directory that survives collection -- after `record_run`, which is what
    # writes the run's member events into it.
    lineage_trace = workspace/"_metasmith"/"trace.jsonl"

    def _copy_trace():
        if lineage_trace.is_file():
            shutil.copy(lineage_trace, workspace/log_dir/"trace.jsonl")

    _post_run_step("trace copy", post_run_failures, _copy_trace)

    def _collect():
        Log.Info(f"compiling results")
        output = CollectResults(
            task=task,
            output_path=output_path,
            inputs_dir=output_path.parent/"inputs",
        )
        n_outputs = sum(1 for p in output.manifest if Path(p).is_relative_to(output_path) or not Path(p).is_absolute())
        external_results_path = path_map.LocalToExternal(output_path)
        Log.Info(f"[{n_outputs}] outputs for [{key}] at [{external_results_path}]")
        return output

    output = _post_run_step("collect results", post_run_failures, _collect)
    _post_run_step("log gathering", post_run_failures, lambda: _gather_step_logs(workspace, log_dir, MAIN_LOG, task))

    if output is None:
        Log.Warn(f"logs not linked to [{output_path}]: results were not collected")
    else:
        _post_run_step(
            "log links", post_run_failures,
            lambda: _link_logs(output_path/f"{output._path_to_meta}", log_dir),
        )

    if failed_steps or post_run_failures:
        # Ignoring a dead step is the right strategy -- one dead annotator must
        # not destroy an eleven-sample run -- but the run is not a success, and
        # saying it completed is how a user is told their results are there when
        # the step that makes them never ran.
        because = []
        if failed_steps:
            because.append(f"with [{len(failed_steps)}] ignored step(s): {', '.join(failed_steps)}")
        if post_run_failures:
            because.append(f"with [{len(post_run_failures)}] failed post-run step(s): {', '.join(post_run_failures)}")
        Log.Error(f"{AgentPaths.RUN_FAILED_SENTINEL} [{StdTime.Timestamp()}] {'; '.join(because)}")
    else:
        Log.Info(f"{AgentPaths.RUN_DONE_SENTINEL} [{StdTime.Timestamp()}]")


def _gather_step_logs(workspace: Path, log_dir: Path, MAIN_LOG: Path, task):
    Log.Info(f"gathering log files")
    nxf_ids = set()
    nxf_id_len = 9
    with open(MAIN_LOG, "r") as f:
        for l in f:
            candidates = re.findall(r"\[[\dabcdef]{2}/[\dabcdef]{6}\]", l)
            if len(candidates) == 0: continue
            hit = candidates[0]
            nxf_id = hit[1:-1]
            nxf_ids.add(nxf_id)
    NXF_WORK = workspace/"nxf_work"
    PROCESS_DEST = workspace/log_dir/"steps"
    PROCESS_DEST.mkdir(parents=True, exist_ok=True)
    for p in NXF_WORK.glob("*/*"):
        p = p.relative_to(NXF_WORK)
        nxf_id = str(p)[:nxf_id_len]
        if nxf_id not in nxf_ids: continue
        log_path = NXF_WORK/p/".command.log"
        if not log_path.exists(): continue
        if log_path.is_symlink(): continue
        try:
            with open(log_path) as f:
                first_line = f.readline()
                if not re.match(r"step\s?\d+", first_line): continue
                step = [int(x) for x in re.findall(r"\d+", first_line)][0]
                transform = task.plan.steps[step-1].transform
            dest = PROCESS_DEST/f"p{step:02}__{transform.name}_{nxf_id.replace('/', '-')}.log"
            src = log_path
            dest.symlink_to(f"../../../{src.relative_to(workspace)}")
        except:
            continue


def _link_logs(output_metadata_path: Path, log_dir: Path):
    Log.Info(f"linking logs [{log_dir}] to results folder [{output_metadata_path.parent}]")
    (output_metadata_path/f"{log_dir.name}").symlink_to(f"../../{log_dir}")
    latest_link = (output_metadata_path/f"logs.latest")
    if latest_link.exists(): latest_link.unlink()
    latest_link.symlink_to(f"../../{log_dir}")

def CheckWorkflow(key: str, index: int|None=None, quiet: bool=False) -> dict:
    task_path = AgentPaths.to_task(key)
    workspace = task_path.parent.parent
    assert workspace.exists(), f"task workspace not found [{workspace}], maybe it wasn't staged yet"

    internals = workspace/AgentPaths.INTERNALS
    log_scan_result = list((internals).glob("logs.*"))
    log_dirs = [p for p in log_scan_result if "latest" not in p.name]
    log_dirs = sorted(log_dirs, key=lambda x: x.name)

    result = {
        "key": key,
        "runs": [{"index": i+1, "name": d.name, "path": str(d)} for i, d in enumerate(log_dirs)],
        "total_runs": len(log_dirs),
        "selected_run": None,
        "log_content": None,
    }

    if len(log_dirs) == 0:
        if not quiet:
            Log.Warn(f"no logs found for [{key}]")
        return result

    if not quiet:
        Log.Info(f"found [{len(log_dirs)}] runs")
        for i, log_entry in enumerate(log_dirs):
            n_str = f"{i+1}"
            Log.Info(f"{' '*(5-len(n_str))}{n_str}: [{log_entry.name}]")

    log_dir = log_dirs[-1]
    selected_index = len(log_dirs)
    msg = f"here is the main log of the latest run [{log_dir.name}]"
    if index is not None:
        if index < 1 or index > len(log_dirs):
            if not quiet:
                Log.Warn(f"index [{index}] out of range")
        else:
            log_dir = log_dirs[index-1]
            selected_index = index
            msg = f"here is the main log for run [{index}] [{log_dir.name}]"

    result["selected_run"] = {"index": selected_index, "name": log_dir.name, "path": str(workspace/log_dir)}
    with open(workspace/log_dir/AgentPaths.MAIN_LOG_FILE, "r") as f:
        result["log_content"] = f.read()

    if not quiet:
        Log.Info(msg)
        Log.Info(f">"*len(msg))
        Log.Info("")
        lines = result["log_content"].splitlines(keepends=True)
        MAXL = 1000
        HEAD = 20
        if len(lines)>1000:
            print("".join(lines[:HEAD]))
            print(f"... +{len(lines)-HEAD-MAXL}")
            print("".join(lines[-(MAXL-HEAD):]))
        else:
            print("".join(lines))
        Log.Info("")
        Log.Info(f"<"*len(msg))
        Log.Info(f"log folder at [{workspace/log_dir}]")

    return result
