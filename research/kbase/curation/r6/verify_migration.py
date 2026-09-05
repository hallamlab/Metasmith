"""Compare every migrated call site against its pre-migration source in git HEAD.

For each ExecWithEnv call in the working tree, the corresponding container arm in
HEAD must carry the same keyword set with byte-identical source segments. A
`cmd=` whose text moved by so much as an indent is the failure this exists to catch.
"""
import ast, subprocess, sys
from pathlib import Path

def head(path):
    r = subprocess.run(["git", "show", f"HEAD:{path}"], capture_output=True)
    return None if r.returncode else r.stdout

def kwmap(call, src):
    out = {}
    for i, a in enumerate(call.args):
        out[("env", "cmd", "shell")[i]] = ast.get_source_segment(src, a, padded=False)
    for k in call.keywords:
        out[k.arg] = ast.get_source_segment(src, k.value, padded=False)
    return out

def arms_in(data, name):
    tree = ast.parse(data)
    src = data.decode()
    found = []
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call) or not isinstance(n.func, ast.Attribute): continue
        if n.func.attr == name:
            found.append((n.lineno, kwmap(n, src)))
    return found

bad = tot = 0
divergent = []
for path in sorted(p for r in ("src/metasmith_libraries", "src/fabfos", "src/metasmith")
                   for p in Path(r).rglob("*.py")):
    now = path.read_bytes()
    if b"ExecWithEnv" not in now: continue
    was = head(str(path))
    if was is None:
        print(f"NEW FILE (no HEAD): {path}"); continue
    new = arms_in(now, "ExecWithEnv")
    old = arms_in(was, "ifContainerDo")
    venv = dict(arms_in(was, "ifVirtualEnvDo"))
    if len(new) != len(old):
        print(f"COUNT {path}: now={len(new)} was={len(old)}"); bad += 1; continue
    # ast.walk is breadth-first, so the two files can enumerate the same calls in
    # different orders; compare as multisets of keyword maps.
    key = lambda m: tuple(sorted(m.items()))
    tot += len(new)
    na, ob = sorted(map(key, (a for _, a in new))), sorted(map(key, (b for _, b in old)))
    if na != ob:
        bad += 1
        print(f"DIFF {path}")
        for x in na:
            if x not in ob: print(f"   only now: {dict(x)}")
        for x in ob:
            if x not in na: print(f"   only was: {dict(x)}")
    for ln, v in venv.items():
        c = dict(old).get(ln - 1) or {}
        # the venv arm sat on the line after its container arm in every chain
        match = next((b for l2, b in old if abs(l2 - ln) <= 2), None)
        if match and match.get("cmd") != v.get("cmd"):
            divergent.append((str(path), ln, match.get("cmd"), v.get("cmd")))

print(f"\nverified {tot} call sites, {bad} mismatched")
print(f"two-arm chains whose commands differed: {len(divergent)}")
for p, ln, c, v in divergent:
    print(f"  {p}:{ln}\n    container={c}\n    venv     ={v}")
sys.exit(1 if bad else 0)
