#!/usr/bin/env python3
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ENV_DIR = Path("resources/env")
HEAD = "context.ExecWithEnv().ifContainerDo("


def conda_tools() -> set[str]:
    out = set()
    for p in ENV_DIR.glob("*.env"):
        for line in p.read_text().splitlines():
            if line.strip().startswith("conda:"):
                out.add(p.stem)
                break
    return out


def env_var_to_tool(tree: ast.Module) -> dict[str, str]:
    out: dict[str, str] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        tgt = node.targets[0]
        if not isinstance(tgt, ast.Name):
            continue
        for sub in ast.walk(node.value):
            if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                m = re.fullmatch(r"env::(.+)\.env", sub.value)
                if m:
                    out[tgt.id] = m.group(1)
    return out


def chains(tree: ast.Module):
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "ifContainerDo"
        ):
            yield node


def kwnames(call: ast.Call) -> set[str]:
    return {k.arg for k in call.keywords if k.arg}


def analyse(path: Path, tools: set[str]):
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(path))
    var2tool = env_var_to_tool(tree)
    rows = []
    for call in chains(tree):
        envkw = next((k.value for k in call.keywords if k.arg == "env"), None)
        var = envkw.id if isinstance(envkw, ast.Name) else None
        tool = var2tool.get(var or "")
        kws = kwnames(call)
        has_mount = bool(kws & {"binds", "args"})
        has_conda = tool in tools if tool else False
        eligible = has_conda and not has_mount
        rows.append(dict(
            lineno=call.lineno, var=var, tool=tool,
            has_conda=has_conda, has_mount=has_mount, eligible=eligible,
        ))
    return src, rows


def add_arms(src: str, rows: list[dict]) -> tuple[str, int]:
    lines = src.split("\n")
    added = 0
    for row in sorted(rows, key=lambda r: r["lineno"], reverse=True):
        if not row["eligible"]:
            continue
        i = row["lineno"] - 1
        if HEAD not in lines[i]:
            continue
        indent = " " * (len(lines[i]) - len(lines[i].lstrip()))
        j = i
        while j < len(lines) and lines[j].rstrip() != indent + ")":
            j += 1
        if j >= len(lines):
            continue
        cmd_ref = row["var"]
        lines[j] = (
            indent + ").ifVirtualEnvDo(\n"
            + indent + f"    env={cmd_ref},\n"
            + indent + "    cmd=_cmd,\n"
            + indent + ")"
        )
        added += 1
    return "\n".join(lines), added


def main(argv):
    mode = argv[1]
    tools = conda_tools()
    tot = elig = mount = noconda = 0
    per_lib: dict[str, list[int]] = {}
    for lib in argv[2:]:
        e = m = n = 0
        for p in sorted((Path("transforms") / lib).rglob("*.py")):
            if HEAD not in p.read_text(encoding="utf-8"):
                continue
            _, rows = analyse(p, tools)
            for r in rows:
                tot += 1
                if r["eligible"]:
                    e += 1
                elif r["has_mount"]:
                    m += 1
                else:
                    n += 1
        per_lib[lib] = [e, m, n]
        elig += e; mount += m; noconda += n
    print(f"{'library':<22} {'eligible':>9} {'has binds/args':>15} {'no conda:':>10}")
    for lib, (e, m, n) in per_lib.items():
        print(f"{lib:<22} {e:>9} {m:>15} {n:>10}")
    print(f"{'TOTAL':<22} {elig:>9} {mount:>15} {noconda:>10}   (of {tot} chains)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
