"""The package's shape: what imports, what it drags in, and what its verbs are.

Three claims that the migration made and that nothing else would catch:

  * every module resolves at its new path, and the whole package imports;
  * `ecspr.bake` pulls in NO `ecspr.model`, and therefore no scipy, networkx or
    cobra. The bake images carry rdkit or a torch stack and nothing else, so an
    import across that seam does not degrade, it fails at load -- six hours into
    a queued job, in the one env nothing local reproduces;
  * every command line still carries exactly the verbs and flags it carried as a
    flat file. The transforms invoke these by string, so a renamed flag is a
    runtime failure with no compile-time trace.

The CLI surface is frozen as a literal below rather than compared against the
pre-move files. A frozen literal is checkable by reading; a comparison against
git history stops working the moment the history is squashed.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

# Every module in the package, so a new one has to be added here deliberately.
MODULES = [
    "ecspr.bake",
    "ecspr.bake.atom_pairs", "ecspr.bake.encoding",
    "ecspr.bake.metabolism", "ecspr.bake.evidence",
    "ecspr.bake.aam",
    "ecspr.bake.aam.combine", "ecspr.bake.aam.curation",
    "ecspr.bake.aam.indigo_member", "ecspr.bake.aam.layers",
    "ecspr.bake.aam.metacyc_member", "ecspr.bake.aam.neural_members",
    "ecspr.bake.aam.shard", "ecspr.bake.aam.worklist",
    "ecspr.bake.direction",
    "ecspr.bake.direction.calibrate", "ecspr.bake.direction.canon",
    "ecspr.bake.direction.combine", "ecspr.bake.direction.curated",
    "ecspr.bake.direction.drive", "ecspr.bake.direction.metacyc_flatfile",
    "ecspr.bake.direction.refdata", "ecspr.bake.direction.thermo_dgbyg",
    "ecspr.bake.direction.thermo_eq",
]

# Modules whose import needs a tool that is not in every env. The two thermo
# members each live in one bake image; the extractor needs rdkit, which the
# measurement env deliberately does not carry. Everything else must import
# anywhere, and that is the point of listing these three by name.
NEEDS_A_TOOL = {
    "ecspr.bake.direction.thermo_eq": "equilibrator_api",
    "ecspr.bake.direction.thermo_dgbyg": "dGbyG",
    "ecspr.bake.atom_pairs": "rdkit",
    "ecspr.bake.aam.combine": "rdkit",          # via ..atom_pairs
}

# {module: {verb: {flags}}} -- "" is the verb-less case (one flat parser).
# Frozen from the pre-move flat modules; every entry was byte-identical across the
# migration. The only additions since are `--collapsed-atom-limit` on the two steps
# that apply the size cut, which is the AAM change and is meant to show up here.
CLI = {
    "ecspr.bake.aam.worklist": {
        "build": {"--reactions", "--metabolites", "--atom-limit", "--char-limit",
                  "--collapsed-atom-limit", "--out", "--out-summary"},
        "close": {"--worklist", "--pairs", "--rescued", "--out", "--out-summary"},
    },
    "ecspr.bake.aam.curation": {
        "propose": {"--lookups", "--worklist", "--chebi", "--modelseed",
                    "--override", "--drop-lane", "--out"},
        "complete": {"--lookups", "--worklist", "--crosswalk", "--char-limit",
                     "--atom-limit", "--collapsed-atom-limit",
                     "--out", "--out-balance", "--out-placeholders"},
    },
    "ecspr.bake.aam.layers": {
        "fuse": {"--member", "--out"},
        "stack": {"--layer", "--out"},
    },
    "ecspr.bake.aam.indigo_member": {
        "map": {"--worklist", "--out", "--limit", "--timeout", "--shard",
                "--sidecar", "--exclude"},
        "merge": {"--shard-file", "--expect", "--out"},
        "retry": {"--out", "--timeout"},
    },
    "ecspr.bake.aam.neural_members": {
        "": {"--member", "--out", "--worklist", "--reac-prop", "--chem-prop",
             "--limit", "--timeout", "--timeout-log", "--shard", "--sidecar",
             "--exclude", "--covered", "--merge-from", "--mem-budget-gb"},
    },
    "ecspr.bake.aam.metacyc_member": {
        "": {"--smiles-dat", "--reac-xref", "--out", "--out-report"},
    },
    "ecspr.bake.atom_pairs": {
        "extract": {"--aam", "--reac-prop", "--chem-prop", "--out", "--out-status",
                    "--align", "--connectivity-fallback", "--fallback-forced",
                    "--balance", "--placeholders", "--resolved", "--min-confidence"},
        "selftest": set(),
    },
    "ecspr.bake.metabolism": {
        "pairs": {"--aam-pairs", "--reactions", "--out-vocab", "--out-pairs"},
        "direction": {"--direction", "--vocab", "--out"},
    },
    "ecspr.bake.evidence": {
        "collect": {"--root", "--tool", "--version", "--file"},
        "manifest": {"--tool", "--version"},
    },
    "ecspr.bake.direction.drive": {
        "universe": {"--reac-prop", "--out"},
        "eval": {"--member", "--universe", "--reac-prop", "--chem-prop",
                 "--shard", "--require", "--out"},
        "merge": {"--member", "--shard-file", "--expect", "--universe", "--out"},
    },
    "ecspr.bake.direction.curated": {
        "": {"--metacyc-reactions", "--reac-xref", "--reac-prop", "--chem-xref",
             "--out", "--out-per-reaction"},
    },
    "ecspr.bake.direction.calibrate": {
        "": {"--curated", "--reac-prop", "--chem-prop", "--eq-member", "--limit",
             "--out-calibration", "--out-points"},
    },
    "ecspr.bake.direction.combine": {
        "": {"--base-mnxrs", "--eq", "--dgbyg", "--curated", "--calibration",
             "--sigma0", "--out"},
    },
}


def _import(mod):
    import importlib
    if mod in NEEDS_A_TOOL:
        pytest.importorskip(NEEDS_A_TOOL[mod],
                            reason=f"{mod} needs a tool that lives in one bake image")
    return importlib.import_module(mod)


@pytest.mark.parametrize("mod", MODULES)
def test_every_module_imports_at_its_new_path(mod):
    assert _import(mod) is not None


@pytest.mark.parametrize("mod", sorted(CLI))
def test_the_command_line_still_carries_exactly_its_old_flags(mod):
    """Verbs and flags, frozen. The transforms invoke these by string.

    `argv` was threaded onto each entry point precisely so this can be checked
    in-process: reaching the parser costs a function call rather than a
    subprocess per module.
    """
    import argparse

    m = _import(mod)
    entry = getattr(m, "parse_args", None) or getattr(m, "main")
    captured = []
    real = argparse.ArgumentParser.parse_args

    def spy(self, args=None, namespace=None):
        captured.append(self)
        raise SystemExit(0)

    argparse.ArgumentParser.parse_args = spy
    try:
        with pytest.raises(SystemExit):
            entry([])
    finally:
        argparse.ArgumentParser.parse_args = real

    assert captured, f"{mod}: never reached ArgumentParser.parse_args"
    ap = captured[0]

    def flags(p):
        return {o for a in p._actions for o in a.option_strings
                if o.startswith("--") and o != "--help"}

    subs = [a for a in ap._actions if isinstance(a, argparse._SubParsersAction)]
    if subs:
        got = {name: flags(p) for name, p in subs[0].choices.items()}
    else:
        got = {"": flags(ap)}
    assert got == CLI[mod]


def test_importing_the_bake_does_not_drag_in_the_measurement_stack():
    """A fresh interpreter per lane, because a leak is only visible at load.

    scipy, networkx and cobra are absent from every bake image by design. The
    one module that reaches across -- `encoding.compile_atom_graph`, which READS
    a finished bake for the reference gate -- keeps its import inside the
    function for exactly this reason, and this is what stops it drifting back
    out to module scope.
    """
    pytest.importorskip("rdkit", reason="the aam lane's extractor needs it")
    probe = (
        "import sys, importlib;"
        "[importlib.import_module(m) for m in {mods!r}];"
        "leak = sorted(k for k in sys.modules"
        " if k.startswith('ecspr.model') or k in ('scipy','networkx','cobra'));"
        "print(leak)"
    )
    lanes = {
        "aam": ["ecspr.bake.aam.worklist", "ecspr.bake.aam.layers",
                "ecspr.bake.aam.curation", "ecspr.bake.atom_pairs"],
        "direction": ["ecspr.bake.direction.drive", "ecspr.bake.direction.combine"],
        "bake": ["ecspr.bake.metabolism", "ecspr.bake.encoding"],
    }
    for lane, mods in lanes.items():
        r = subprocess.run([sys.executable, "-c", probe.format(mods=mods)],
                           capture_output=True, text=True)
        assert r.returncode == 0, f"{lane}: {r.stderr}"
        assert r.stdout.strip() == "[]", f"{lane} lane leaked: {r.stdout.strip()}"
