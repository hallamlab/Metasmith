from __future__ import annotations

from ...ops import source as _ops


def register(subs):
    p = subs.add_parser("source", help="Source URI parsing, reachability, transfer")
    sp = p.add_subparsers(dest="sub", metavar="ACTION")

    _parse = sp.add_parser("parse", help="parse a URI into a Source dict")
    _parse.add_argument("uri")
    _parse.set_defaults(func=lambda a: _ops.parse(a.uri))

    _exists = sp.add_parser("exists", help="probe whether a Source URI is reachable")
    _exists.add_argument("uri")
    _exists.add_argument("--timeout", type=int, default=10)
    _exists.set_defaults(func=lambda a: _ops.exists(a.uri, a.timeout))

    _xfer = sp.add_parser("transfer", help="copy between Sources via Logistics")
    _xfer.add_argument("src_uri")
    _xfer.add_argument("dest_uri")
    _xfer.add_argument("--label")
    _xfer.add_argument("--no-wait", action="store_true")
    _xfer.set_defaults(func=lambda a: _ops.transfer(
        a.src_uri, a.dest_uri, not a.no_wait, a.label,
    ))
