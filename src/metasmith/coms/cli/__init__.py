"""metasmith CLI — argparse-based entry point.

The CLI is a thin layer over `metasmith.ops`: each subcommand parses arguments,
invokes one ops function, and prints the result via `cli._format`.
"""
from ._main import main

__all__ = ["main"]
