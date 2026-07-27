"""`metasmith task ...` subcommands — inspect cached workflow plans."""
from __future__ import annotations

from ...ops import workflow as _ops


def register(subs):
    p = subs.add_parser("task", help="cached workflow plans in the workspace")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser("list", help="list tasks in the workspace")
    _list.set_defaults(func=lambda a: _ops.list_tasks(a.workspace))

    _show = sp.add_parser("show", help="show plan + targets for one task")
    _show.add_argument("task_key")
    _show.set_defaults(func=lambda a: _ops.get_plan(a.task_key, a.workspace))

    _hints = sp.add_parser("hints", help="plan-failure hints for a task")
    _hints.add_argument("task_key")
    _hints.set_defaults(func=lambda a: _ops.get_hints(a.task_key, a.workspace))

    _dag = sp.add_parser("dag", help="render the plan DAG to disk")
    _dag.add_argument("task_key")
    _dag.add_argument("--format", default="svg",
                      help="svg (default), text, dot, or any raster format"
                           " graphviz can write (needs the `neato` binary)")
    _dag.add_argument("--blacklist-namespace", action="append", default=None,
                      dest="blacklist_namespaces")
    _dag.set_defaults(func=lambda a: _ops.render_dag(
        a.task_key, a.format, a.blacklist_namespaces, a.workspace,
    ))

    _del = sp.add_parser("delete", help="remove a cached task")
    _del.add_argument("task_key")
    _del.set_defaults(func=lambda a: _ops.delete_task(a.task_key, a.workspace))
