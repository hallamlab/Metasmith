from __future__ import annotations

import json

from ...ops import agent as _ops


def _parse_params(entries: list[str]) -> dict | None:
    if not entries: return None
    out = {}
    for e in entries:
        assert "=" in e, f"--param expects NAME=VALUE; got [{e}]"
        k, _, v = e.partition("=")
        k = k.strip()
        assert k, f"--param has no name: [{e}]"
        try:
            parsed = json.loads(v)
            out[k] = v if isinstance(parsed, (dict, list)) else parsed
        except ValueError:
            out[k] = v
    return out


def register(subs):
    p = subs.add_parser("agent", help="agent loading, deployment, ping")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _list = sp.add_parser("list", help="summarize a set of agent YAMLs")
    _list.add_argument("--agent", "-a", action="append", required=True,
                       help="agent YAML path (repeatable)")
    _list.set_defaults(func=lambda a: _ops.list_agents(a.agent))

    _info = sp.add_parser("info", help="full info for one agent")
    _info.add_argument("agent_path")
    _info.set_defaults(func=lambda a: _ops.info(a.agent_path))

    _save = sp.add_parser("save", help="write a new agent YAML to disk")
    _save.add_argument("path")
    _save.add_argument("--home", required=True, dest="home_uri",
                       help="Source URI: /local/path or ssh://host/path")
    _save.add_argument("--container")
    _save.add_argument("--runtime", default="DOCKER", choices=["DOCKER", "APPTAINER"])
    _save.add_argument("--setup", action="append", default=[], dest="setup_commands")
    _save.add_argument("--globus-uuid")
    _save.add_argument("--preset", dest="default_preset",
                       help="nextflow config preset to use when a run names none (default: local)")
    _save.add_argument("--param", action="append", default=[], dest="default_params",
                       metavar="NAME=VALUE",
                       help="a param every run on this agent starts from, e.g. "
                            "slurmAccount=st-you-1; repeatable")
    _save.set_defaults(func=lambda a: _ops.save_agent(
        a.path, a.home_uri, a.container, a.runtime,
        a.setup_commands or None, a.globus_uuid,
        default_preset=a.default_preset,
        default_params=_parse_params(a.default_params),
    ))

    _ping = sp.add_parser("ping", help="echo+hostname over SSH to the agent")
    _ping.add_argument("agent_path")
    _ping.add_argument("--timeout", type=int, default=15)
    _ping.set_defaults(func=lambda a: _ops.ping(a.agent_path, a.timeout))

    _dep = sp.add_parser("deploy", help="deploy an agent to its home location")
    _dep.add_argument("agent_path")
    _dep.add_argument("--assertive", action="store_true")
    _dep.set_defaults(func=lambda a: _ops.deploy(a.agent_path, a.assertive))
