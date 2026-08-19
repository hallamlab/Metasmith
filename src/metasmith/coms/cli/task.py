from __future__ import annotations

from ...models.dag_colour import SCHEMES
from ...models.dag_renderer import THEMES
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
    _dag.add_argument("--label-mode", default="column", choices=["column", "beside"],
                      help="column (default): one label column right of every"
                           " rail; beside: label next to each marker")
    _dag.add_argument("--step-order", action="store_true",
                      help="show the step number above each transform name")
    _dag.add_argument("--colour", default="module", choices=list(SCHEMES),
                      help="colour scheme (default: module)")
    _dag.add_argument("--theme", default="light", choices=list(THEMES),
                      help="ground the drawing sits on (default: light)")
    _dag.set_defaults(func=lambda a: _ops.render_dag(
        a.task_key, a.format, a.blacklist_namespaces, a.workspace,
        label_mode=a.label_mode, show_step_order=a.step_order,
        colour=a.colour, theme=a.theme,
    ))

    _del = sp.add_parser("delete", help="remove a cached task")
    _del.add_argument("task_key")
    _del.set_defaults(func=lambda a: _ops.delete_task(a.task_key, a.workspace))
