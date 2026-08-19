# Each subcommand dispatches to its `fabfos.pipelines` module rather than
# redeclaring its flags, so `--help` on a subcommand is the driver's own and
# argparse never sees the subcommand's arguments: anything that looks like a
# top-level flag after the subcommand name belongs to the driver.
import sys

from . import __version__, NAME, SHORT_SUMMARY

DRIVERS = {
    "assemble": "pipelines.assembly",
    "annotate": "pipelines.annotation",
    "ecspr": "pipelines.ecspr",
    "refs": "refs",
}

_USAGE = f"""usage: {NAME} <command> [options]

{SHORT_SUMMARY}

commands:
  assemble            reads -> putative inserts + ORFs
  annotate            ORFs + references -> GPR table
  ecspr               GPR table + references -> ECSPr results
  refs                pin / inspect the reference library the three lanes read

  --version           the CLI package version
  --method-version    the method id: what this pipeline IS, not what it runs
  --describe-method   the full method document the id hashes

`{NAME} <command> --help` for a command's options.
"""


def _method_document() -> int:
    import yaml

    from .method import describe_method

    desc = describe_method()
    yaml.safe_dump(desc.to_dict(), sys.stdout, sort_keys=False, default_flow_style=False)
    if desc.unresolved_containers:
        print(
            "\n# NOT STAMPABLE: unresolved containers: "
            + ", ".join(sorted(desc.unresolved_containers)),
            file=sys.stderr,
        )
        return 1
    return 0


def main(argv: "list[str] | None" = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    if not argv or argv[0] in ("-h", "--help"):
        print(_USAGE, end="")
        return 0
    if argv[0] == "--version":
        print(__version__)
        return 0
    if argv[0] == "--method-version":
        from .method import method_id

        print(method_id())
        return 0
    if argv[0] == "--describe-method":
        return _method_document()

    cmd, rest = argv[0], argv[1:]
    if cmd not in DRIVERS:
        print(f"{NAME}: unknown command '{cmd}'\n", file=sys.stderr)
        print(_USAGE, end="", file=sys.stderr)
        return 2

    import importlib

    driver = importlib.import_module(f".{DRIVERS[cmd]}", __package__)
    return driver.main(rest)


if __name__ == "__main__":
    raise SystemExit(main())
