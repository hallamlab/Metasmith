"""Pure (sync) operations that back the CLI.

Each submodule exposes plain functions taking explicit arguments and returning
plain dicts. No global state, no caches, no async. Library objects are loaded
fresh from disk on each call — persistence is the filesystem.

The CLI layer in `metasmith.coms.cli` is a thin argparse veneer over this
package; tests call these functions directly.
"""
