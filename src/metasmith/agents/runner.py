from __future__ import annotations

import json
import os
import re
import shutil
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
from ..models.workflow import NextflowGenContext, WorkflowTask, restat_leaf_ids
from ..serialization import StdTime
from .agent import Agent
from .collect import CollectResults, PublishCachedProducts

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

def RenderLauncher(task_key: str, setup_commands: list[str], binds: str) -> str:
    # start.sh: the root of a run. Everything the run consists of descends from
    # the process it backgrounds, and carries the token it exports.
    return "\n".join([
        f'#!/bin/bash',
        'cd $( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )',
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
        f'nohup ../../msm api run_workflow -a key={task_key} host=$(hostname) log_dir=$LOG_DIR stub_delay=${{1:-0}} </dev/null >$LOG_DIR/agent.log 2>&1 &',
        f'RUN_PGID=$!',
        f'set +m',
        f'echo "$RUN_PGID" > ./{AgentPaths.RUN_PGID_FILE}',
        f'echo "run token is [${AgentPaths.RUN_TOKEN_ENV}], run pgid is [$RUN_PGID]"',
    ])


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
        shell.Exec(
            RenderNextflowScript(
                workspace=workspace, log_dir=log_dir, host=host,
                results_folder=results_folder, nxf_report=nxf_report,
                nxf_dag=nxf_dag, stub_param=stub_param,
            ),
            timeout=None,
        )

    df_tasks = _extract_nxf_task_metadata(workspace/log_dir)
    if df_tasks is not None:
        nxf_task_meta = workspace/log_dir/"nxf_tasks.csv"
        df_tasks.to_csv(nxf_task_meta, index=False)
        Log.Info(f"extracted task metadata to [{nxf_task_meta}]")
    else:
        Log.Warn(f"no task metadata extracted from [{workspace/log_dir}]")

    if os.environ.get("METASMITH_CACHE", "1").lower() not in {
        "0", "false", "off", "no"
    }:
        try:
            from ..caching.layout import default_cache_root
            from ..caching.promote import promote_run

            agent_home = Path(str(extern_home))
            cache_root = default_cache_root(agent_home)
            summary = promote_run(workspace=workspace, cache_root=cache_root)
            if summary.get("promoted") or summary.get("skipped"):
                Log.Info(
                    "cache promote: "
                    f"{len(summary['promoted'])} written, "
                    f"{len(summary['skipped'])} skipped"
                )
        except Exception as e:
            Log.Warn(f"cache promote failed: {e}")

    try:
        PublishCachedProducts(workspace, output_path)
    except Exception as e:
        Log.Warn(f"publishing cache-hit products failed: {e}")

    # `_metasmith/trace.jsonl` sits at the workspace root and gets truncated
    # on the next stage, so a cache-hit step -- which never becomes a
    # Nextflow process and so has no row in nxf_tasks.csv -- would otherwise
    # be unrecoverable once collected. Copy it alongside nxf_tasks.csv, into
    # the one per-run directory that survives collection. Taken after cache
    # promotion, which appends its own miss/promoted events to the same file --
    # a copy taken before would freeze a lineage trace that promotion hadn't
    # finished writing yet, under a filename that looks final.
    lineage_trace = workspace/"_metasmith"/"trace.jsonl"
    if lineage_trace.is_file():
        shutil.copy(lineage_trace, workspace/log_dir/"trace.jsonl")

    Log.Info(f"compiling results")
    output = CollectResults(
        task=task,
        output_path=output_path,
        inputs_dir=output_path.parent/"inputs",
    )
    n_outputs = sum(1 for p in output.manifest if Path(p).is_relative_to(output_path) or not Path(p).is_absolute())
    extern_output_path = extern_workspace/results_folder
    external_results_path = path_map.LocalToExternal(output_path)
    Log.Info(f"[{n_outputs}] outputs for [{key}] at [{external_results_path}]")

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

    Log.Info(f"linking logs [{log_dir}] to results folder [{output_path}]")
    output_metadata_path = output_path/f"{output._path_to_meta}"
    (output_metadata_path/f"{log_dir.name}").symlink_to(f"../../{log_dir}")
    latest_link = (output_metadata_path/f"logs.latest")
    if latest_link.exists(): latest_link.unlink()
    latest_link.symlink_to(f"../../{log_dir}")
    Log.Info(f"run completed at [{StdTime.Timestamp()}]")

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
