from __future__ import annotations

import subprocess
import sys

import pytest

MODULES = [
    "ecspr.bake",
    "ecspr.bake.atom_pairs", "ecspr.bake.encoding",
    "ecspr.bake.metabolism", "ecspr.bake.evidence",
    "ecspr.bake.aam",
    "ecspr.bake.aam.algebra", "ecspr.bake.aam.combine",
    "ecspr.bake.aam.curation", "ecspr.bake.aam.forecast",
    "ecspr.bake.aam.indigo_member", "ecspr.bake.aam.layers",
    "ecspr.bake.aam.metacyc_member", "ecspr.bake.aam.neural_members",
    "ecspr.bake.aam.partial", "ecspr.bake.aam.recount",
    "ecspr.bake.aam.redox", "ecspr.bake.aam.runlogs",
    "ecspr.bake.aam.shard", "ecspr.bake.aam.twins",
    "ecspr.bake.aam.universe", "ecspr.bake.aam.worklist",
    "ecspr.bake.direction",
    "ecspr.bake.direction.calibrate", "ecspr.bake.direction.canon",
    "ecspr.bake.direction.combine", "ecspr.bake.direction.curated",
    "ecspr.bake.direction.drive", "ecspr.bake.direction.forecast",
    "ecspr.bake.direction.metacyc_flatfile", "ecspr.bake.direction.quotient",
    "ecspr.bake.direction.refdata", "ecspr.bake.direction.thermo_dgbyg",
    "ecspr.bake.direction.thermo_eq",
]

NEEDS_A_TOOL = {
    "ecspr.bake.direction.thermo_eq": "equilibrator_api",
    "ecspr.bake.direction.thermo_dgbyg": "dGbyG",
    "ecspr.bake.atom_pairs": "rdkit",
    "ecspr.bake.aam.algebra": "rdkit",
    "ecspr.bake.aam.combine": "rdkit",
    "ecspr.bake.aam.curation": "rdkit",
    "ecspr.bake.aam.forecast": "rdkit",
    "ecspr.bake.aam.universe": "rdkit",
    "ecspr.bake.aam.partial": "rdkit",
    "ecspr.bake.aam.recount": "rdkit",
    "ecspr.bake.aam.redox": "rdkit",
    "ecspr.bake.aam.twins": "rdkit",
}

CLI = {
    "ecspr.bake.aam.worklist": {
        "build": {"--reactions", "--metabolites", "--atom-limit", "--char-limit",
                  "--collapsed-atom-limit", "--out", "--out-summary"},
        "close": {"--worklist", "--pairs", "--rescued", "--forecast",
                  "--redox-emptied", "--out", "--out-summary"},
    },
    "ecspr.bake.aam.curation": {
        "propose": {"--lookups", "--element-counts", "--blockers", "--nametwin",
                    "--llm", "--worklist", "--chebi", "--modelseed",
                    "--override", "--drop-lane", "--out"},
        "complete": {"--lookups", "--element-counts", "--worklist", "--crosswalk",
                     "--char-limit", "--atom-limit", "--collapsed-atom-limit",
                     "--out", "--out-balance", "--out-placeholders"},
    },
    "ecspr.bake.aam.twins": {
        "blockers": {"--lookups", "--element-counts", "--worklist", "--out",
                     "--no-synonyms"},
        "nametwin": {"--lookups", "--element-counts", "--worklist", "--out"},
    },
    "ecspr.bake.aam.algebra": {
        "build": {"--lookups", "--element-counts", "--worklist", "--rescued",
                  "--targets", "--out-forced", "--out-claims", "--out-summary"},
    },
    "ecspr.bake.aam.partial": {
        "build": {"--lookups", "--forecast", "--rescue",
                  "--atom-limit", "--char-limit",
                  "--out", "--out-forced", "--out-summary"},
    },
    "ecspr.bake.aam.forecast": {
        "build": {"--lookups", "--worklist", "--rescued", "--element-counts",
                  "--forced", "--prior-logs", "--out", "--out-summary"},
    },
    "ecspr.bake.aam.universe": {
        "build": {"--worklist", "--rescued", "--partial", "--out", "--out-summary"},
    },
    "ecspr.bake.aam.redox": {
        "repair": {"--pairs", "--lookups", "--out", "--out-refusals",
                   "--out-cofactors", "--out-emptied", "--out-summary"},
    },
    "ecspr.bake.aam.runlogs": {
        "build": {"--cache", "--evidence", "--curated-status", "--runs", "--out",
                  "--step-max-bytes"},
    },
    "ecspr.bake.aam.recount": {
        "build": {"--metabolites", "--out", "--out-summary"},
        "check": {"--counts", "--atom-ranks"},
    },
    "ecspr.bake.aam.layers": {
        "fuse": {"--member", "--submission-class", "--out"},
        "stack": {"--layer", "--out"},
    },
    "ecspr.bake.aam.indigo_member": {
        "map": {"--universe", "--out", "--limit", "--timeout", "--shard",
                "--sidecar", "--cache-dir", "--exclude"},
        "merge": {"--shard-file", "--expect", "--out"},
        "retry": {"--out", "--timeout"},
    },
    "ecspr.bake.aam.neural_members": {
        "": {"--member", "--out", "--universe", "--reac-prop", "--chem-prop",
             "--limit", "--timeout", "--timeout-log", "--shard", "--sidecar",
             "--cache-dir", "--exclude", "--covered", "--merge-from",
             "--mem-budget-gb"},
    },
    "ecspr.bake.aam.metacyc_member": {
        "": {"--smiles-dat", "--reac-xref", "--out", "--out-report"},
    },
    "ecspr.bake.atom_pairs": {
        "extract": {"--aam", "--reac-prop", "--chem-prop", "--out", "--out-status",
                    "--align", "--connectivity-fallback", "--fallback-forced",
                    "--balance", "--placeholders", "--resolved", "--min-confidence",
                    "--universe"},
        "selftest": set(),
    },
    "ecspr.bake.metabolism": {
        "pairs": {"--aam-pairs", "--reactions", "--out-vocab", "--out-pairs"},
        "direction": {"--direction", "--vocab", "--out"},
    },
    "ecspr.bake.evidence": {
        "collect": {"--root", "--tool", "--version", "--file"},
        "manifest": {"--tool", "--version"},
        # A lane whose method lives in a subpackage has nothing to be versioned BY: the
        # default fallback hashes `bake/*.py` and misses `bake/direction/` entirely, and
        # the packages a direction lane imports are pinned by its image. So it computes
        # its own version and passes it back as `--version`.
        "fingerprint": {"--package"},
    },
    "ecspr.bake.direction.drive": {
        "universe": {"--reac-prop", "--out"},
        # `--substitutions` is r9's. It is the ONE flag that changes what the member is
        # asked, so it must appear here and on `forecast build` together: giving it to one
        # and not the other makes the accounting describe a bake nobody built.
        "eval": {"--member", "--universe", "--reac-prop", "--chem-prop",
                 "--shard", "--require", "--substitutions", "--out"},
        "merge": {"--member", "--shard-file", "--expect", "--universe", "--out"},
    },
    "ecspr.bake.direction.curated": {
        # `--supplementary-crosswalk` is a switch rather than always-on for the reason
        # `--substitutions` is: the r8 baselines have to remain reproducible from this
        # tree, and an arm that cannot be turned off cannot be shown to be off.
        "": {"--metacyc-reactions", "--reac-xref", "--reac-prop", "--chem-xref",
             "--out", "--out-per-reaction", "--supplementary-crosswalk"},
    },
    "ecspr.bake.direction.forecast": {
        # `resolve` is its own verb because it is the one part that needs
        # eQuilibrator: `build` runs on table reads in any env with rdkit, and
        # folding the two together would make the cheap half pay for the cache.
        # `--substitutions` is here because a model compound eQuilibrator's frozen cache
        # cannot resolve silences the very reaction it was added to unblock, so the
        # resolution table has to be able to include them.
        "resolve": {"--universe", "--reac-prop", "--chem-prop", "--mnxm-only",
                    "--resume", "--substitutions", "--out"},
        "build": {"--universe", "--reac-prop", "--chem-prop", "--resolution",
                  "--mnxm-only", "--substitutions", "--out", "--out-summary"},
        "backtest": {"--forecast", "--member-eq", "--member-dgbyg",
                     "--out-summary"},
    },
    "ecspr.bake.direction.calibrate": {
        # `--balance-gate` selects whether the raw reac_prop balance test runs before or
        # after the member is consulted. r10 moved it after; the r9 position is kept so
        # the re-bake can price the change as its own arm.
        #
        # `--quotient` and `--prior-quantity` are the second half of the same story: the
        # prior is fitted against the number the combiner averages it with, and r9's
        # standard-state fit is kept as its own arm too.
        "": {"--curated", "--reac-prop", "--chem-prop", "--eq-member", "--limit",
             "--substitutions", "--balance-gate", "--quotient", "--prior-quantity",
             "--out-calibration", "--out-points"},
    },
    "ecspr.bake.direction.combine": {
        # `--prior-width` picks which stored spread the curated prior uses, and `--clamp`
        # exposes the magnitude bound -- the DEPLOYED r9 was baked at 100 kJ/mol while
        # canon has since moved to three decades without a re-bake, so reproducing r9
        # needs to be able to say so.
        "": {"--base-mnxrs", "--eq", "--dgbyg", "--curated", "--calibration",
             "--sigma0", "--prior-width", "--clamp", "--quotient",
             "--no-widen-suspect", "--out"},
    },
    "ecspr.bake.direction.quotient": {
        "table": {"--source", "--chunk", "--chem-xref", "--chem-prop",
                  "--max-expansion", "--out"},
        "annotate": {"--table", "--reac-prop", "--chem-prop", "--substitutions",
                     "--member", "--out"},
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
