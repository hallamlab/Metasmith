from __future__ import annotations

import json

from ...ops import workflow as _ops
from ...ops import runtime as _rt


def register(subs):
    p = subs.add_parser("plan", help="plan a workflow from samples to target types")
    p.add_argument("--data-library", required=True)
    p.add_argument("--sample-type", default=None,
                   help="split the library into one run per item of this type; "
                        "omitted, everything in it is planned as a single sample")
    p.add_argument("--target-type", action="append", required=True, dest="target_types")
    p.add_argument("--transform-library", "-r", action="append", required=True,
                   dest="transform_libraries")
    p.add_argument("--resource-library", action="append", default=[], dest="resource_libraries")
    p.set_defaults(func=_cmd_plan)

    w = subs.add_parser("workflow", help="stage/run/observe planned workflows on an agent")
    sp = w.add_subparsers(dest="sub", metavar="ACTION")

    _stage = sp.add_parser("stage", help="compile DAG to Nextflow and transfer to agent")
    _stage.add_argument("agent")
    _stage.add_argument("task_ref", metavar="TASK",
                        help="task key in the workspace, or a path to a task bundle directory")
    _stage.add_argument("--on-exist", default="skip",
                        choices=["skip", "error", "clear", "update", "update_workflow", "update_data"])
    _stage.add_argument("--timeout", type=float, default=None, dest="idle_timeout",
                        help="seconds an agent-side step may produce no output before "
                             "staging gives up (default: METASMITH_IDLE_TIMEOUT, else 300)")
    _stage.set_defaults(func=lambda a: _rt.stage(
        a.agent, a.task_ref, a.on_exist, a.workspace, a.idle_timeout,
    ))

    _mat = sp.add_parser(
        "materialise", aliases=["materialize"],
        help="fetch a staged task's tool images onto the agent, before running",
    )
    _mat.add_argument("agent")
    _mat.add_argument("task_key")
    _mat.add_argument("--force", action="store_true",
                      help="re-fetch every image even if the store already holds it")
    _mat.set_defaults(func=lambda a: _rt.materialise(a.agent, a.task_key, a.force))

    _env = sp.add_parser(
        "setup-env",
        help="prepare the agent's host for a staged task: tool images for a "
             "container agent, tool conda envs for a mamba one",
    )
    _env.add_argument("agent")
    _env.add_argument("task_key")
    _env.add_argument("--force", action="store_true",
                      help="re-fetch or rebuild even what is already there")
    _env.add_argument("--library", default=None,
                      help="library to read conda recipes from "
                           "(default: the installed metasmith_libraries)")
    _env.set_defaults(func=lambda a: _rt.setup_environment(
        a.agent, a.task_key, a.force, a.library,
    ))

    _run = sp.add_parser("run", help="launch a staged workflow")
    _run.add_argument("agent")
    _run.add_argument("task_key")
    _run.add_argument("--preset")
    _run.add_argument("--params", help="JSON dict")
    _run.add_argument("--override", action="append", default=[], dest="overrides",
                      help="step_key=KEY:VALUE,KEY:VALUE (e.g. step1=cpus:4,memory_gb:8)")
    _run.add_argument("--stub-delay", type=float, default=0)
    _run.set_defaults(func=_cmd_run)

    _wait = sp.add_parser("wait", help="block until completion sentinel appears")
    _wait.add_argument("agent")
    _wait.add_argument("task_key")
    _wait.add_argument("--timeout", type=float, default=3600.0)
    _wait.add_argument("--poll", type=float, default=5.0)
    _wait.add_argument("--run", type=int)
    _wait.set_defaults(func=lambda a: _rt.wait(a.agent, a.task_key, a.timeout, a.poll, a.run))

    _tail = sp.add_parser("tail", help="tail agent.log / main.log")
    _tail.add_argument("agent")
    _tail.add_argument("task_key")
    _tail.add_argument("--source", default="agent", choices=["agent", "main"])
    _tail.add_argument("--lines", type=int, default=50)
    _tail.add_argument("--run", type=int)
    _tail.set_defaults(func=lambda a: _rt.tail(a.agent, a.task_key, a.source, a.lines, a.run))

    _cancel = sp.add_parser("cancel", help="best-effort cancel via PID lock")
    _cancel.add_argument("agent")
    _cancel.add_argument("task_key")
    _cancel.add_argument("--timeout", type=float, default=30.0)
    _cancel.set_defaults(func=lambda a: _rt.cancel(a.agent, a.task_key, a.timeout))

    _ps = sp.add_parser("ps", help="what this run still has running on the agent")
    _ps.add_argument("agent")
    _ps.add_argument("task_key")
    _ps.add_argument("--scope", default="run", choices=["run", "workload"])
    _ps.set_defaults(func=lambda a: _rt.ps(a.agent, a.task_key, a.scope))

    _reap = sp.add_parser("reap", help="kill whatever a run left running on the agent")
    _reap.add_argument("agent")
    _reap.add_argument("task_key")
    _reap.add_argument("--passes", type=int, default=3)
    _reap.add_argument("--scope", default="run", choices=["run", "workload"])
    _reap.set_defaults(func=lambda a: _rt.reap(a.agent, a.task_key, a.passes, a.scope))

    _runs = sp.add_parser("runs", help="list all runs for a task on an agent")
    _runs.add_argument("agent")
    _runs.add_argument("task_key")
    _runs.set_defaults(func=lambda a: _rt.list_runs(a.agent, a.task_key))

    _check = sp.add_parser("check", help="same-machine status + logs check")
    _check.add_argument("task_key")
    _check.add_argument("--run", type=int)
    _check.set_defaults(func=lambda a: _rt.check(a.task_key, a.run))

    _collect = sp.add_parser("collect", help="transfer results from agent to a URI")
    _collect.add_argument("agent")
    _collect.add_argument("task_key")
    _collect.add_argument("--dest", required=True)
    _collect.add_argument("--no-globus", action="store_true")
    _collect.set_defaults(func=lambda a: _rt.collect(
        a.agent, a.task_key, a.dest, not a.no_globus,
    ))

    _res = sp.add_parser("result-source", help="show URI of result directory")
    _res.add_argument("agent")
    _res.add_argument("task_key")
    _res.set_defaults(func=lambda a: _rt.result_source(a.agent, a.task_key))

    _pre = sp.add_parser("presets", help="list Nextflow config presets for an agent")
    _pre.add_argument("agent")
    _pre.set_defaults(func=lambda a: _rt.list_presets(a.agent))


def _cmd_plan(args):
    return _ops.plan_workflow(
        args.data_library,
        args.sample_type,
        args.target_types,
        args.transform_libraries,
        args.resource_libraries or None,
        args.workspace,
    )


def _cmd_run(args):
    params = json.loads(args.params) if args.params else None
    overrides: dict = {}
    for o in args.overrides:
        assert "=" in o, f"--override expects step=key:val,...; got [{o}]"
        step, body = o.split("=", 1)
        kv: dict = {}
        for entry in body.split(","):
            k, v = entry.split(":", 1)
            v = v.strip()
            if k in ("cpus",):
                kv[k] = int(v)
            else:
                kv[k] = float(v)
        overrides[step] = kv
    return _rt.run(
        args.agent, args.task_key, args.preset, params, overrides or None, args.stub_delay,
    )
