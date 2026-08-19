from __future__ import annotations

import pytest

from ecspr.bake.aam import runlogs


def write(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(["\t".join(header)]
                              + ["\t".join(r) for r in rows]) + "\n")
    return path


@pytest.fixture
def bake(tmp_path):
    cache = tmp_path / "aam_cache"
    ev = tmp_path / "evidence"

    write(cache / "indigo" / "cache.tsv",
          ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence"),
          [("MNXR1", "A>>B", "[A:1]>>[B:1]", "1.0"),
           ("MNXR2", "C>>D", "[C:1]>>[D:1]", "1.0"),
           ("MNXR3#C", "E>>F", "[E:1]>>[F:1]", "1.0"),
           ("MNXR3#N", "G>>H", "", "1.0")])
    (cache / "indigo" / "indigo_0.attempted").write_text(
        "#shard 0/1\nMNXR1\nMNXR2#N\nMNXR4\n")
    write(cache / "rxnmapper" / "cache.tsv",
          ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence"),
          [("MNXR1", "A>>B", "[A:1]>>[B:1]", "0.9"),
           ("MNXR2", "C>>D", "", ""),
           ("MNXR2#C", "C>>D", "[C:1]>>[D:1]", "0.8")])
    (cache / "rxnmapper" / "rxnmapper_0.attempted").write_text("#shard 0/1\nMNXR9\n")
    write(cache / "localmapper" / "cache.tsv",
          ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence"),
          [("MNXR1", "A>>B", "[A:1]>>[B:1]", "0.5")])

    write(ev / "indigo" / "1.45.0" / "indigo_0.tsv",
          ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence", "status"),
          [("MNXR1", "A>>B", "[A:1]>>[B:1]", "1.0", "ok"),
           ("MNXR2", "C>>D", "[C:1]>>[D:1]", "1.0", ""),
           ("MNXR2#N", "C>>D", "", "", "timeout"),
           ("MNXR3#C", "E>>F", "[E:1]>>[F:1]", "1.0", ""),
           ("MNXR3#N", "G>>H", "", "", "")])
    write(ev / "indigo" / "1.45.0" / "indigo.tsv",
          ("mnxr", "rxn_smiles", "mapped_rxn_smiles", "confidence"),
          [("MNXR1", "A>>B", "[A:1]>>[B:1]", "1.0")])
    return cache, ev


def build(tmp_path, cache, ev, **kw):
    out = tmp_path / "logs"
    args = runlogs.parse_args(
        ["build", "--cache", str(cache), "--evidence", str(ev), "--out", str(out)]
        + [x for k, v in kw.items() for x in (f"--{k}", str(v))])
    assert args.fn(args) == 0
    return out


def read(path):
    lines = path.read_text().splitlines()
    header = lines[0].split("\t")
    return {r[0]: dict(zip(header, r)) for r in (l.split("\t") for l in lines[1:])}


def test_one_row_per_reaction_and_the_whole_submission_wins(tmp_path, bake):
    cache, ev = bake
    st = read(build(tmp_path, cache, ev) / "indigo_status.tsv")

    assert st["MNXR1"]["status"] == "ok"
    assert st["MNXR2"]["status"] == "ok"
    assert st["MNXR3"]["status"] == "empty"
    assert len(st) == 3


def test_a_blank_status_is_read_from_the_map_not_dropped(tmp_path, bake):
    cache, ev = bake
    st = read(build(tmp_path, cache, ev) / "indigo_status.tsv")
    assert {"MNXR2", "MNXR3"} <= set(st)


def test_unreturned_is_attempted_minus_returned(tmp_path, bake):
    cache, ev = bake
    un = read(build(tmp_path, cache, ev) / "indigo_unreturned.tsv")
    assert set(un) == {"MNXR4"}


def test_a_kill_is_carried_as_unreturned(tmp_path, bake):
    cache, ev = bake
    p = ev / "indigo" / "1.45.0" / "indigo_0.tsv"
    p.write_text(p.read_text() + "MNXR5\tI>>J\t\t\tkilled\n")
    out = build(tmp_path, cache, ev)
    assert read(out / "indigo_status.tsv")["MNXR5"]["status"] == "killed"
    assert "MNXR5" in read(out / "indigo_unreturned.tsv")


def test_derived_status_is_the_missing_confidence(tmp_path, bake):
    cache, ev = bake
    out = build(tmp_path, cache, ev)
    rx = read(out / "rxnmapper_derived_status.tsv")
    assert rx["MNXR1"]["derived_status"] == "ok"
    assert rx["MNXR2"]["derived_status"] == "no_confidence"
    assert read(out / "localmapper_derived_status.tsv")["MNXR1"]["derived_status"] == "ok"


def test_attempted_holds_indigo_alone(tmp_path, bake):
    cache, ev = bake
    names = sorted(p.name for p in (build(tmp_path, cache, ev) / "attempted").iterdir())
    assert names == ["indigo_0.attempted"]


def test_without_an_evidence_tree_the_cache_still_answers(tmp_path, bake):
    cache, _ev = bake
    out = tmp_path / "logs"
    args = runlogs.parse_args(["build", "--cache", str(cache), "--out", str(out)])
    assert args.fn(args) == 0
    st = read(out / "indigo_status.tsv")
    assert st["MNXR1"]["status"] == "ok" and st["MNXR3"]["status"] == "empty"


def test_a_large_step_log_is_truncated_with_a_marker(tmp_path, bake):
    cache, ev = bake
    run = tmp_path / "_run_map" / "_metadata" / "logs.latest" / "steps"
    run.mkdir(parents=True)
    (run / "p01__indigo_ab-1234.log").write_text("x" * 5000)
    out = tmp_path / "logs"
    args = runlogs.parse_args(
        ["build", "--cache", str(cache), "--evidence", str(ev), "--out", str(out),
         "--runs", str(tmp_path / "_run_map"), "--step-max-bytes", "1000"])
    assert args.fn(args) == 0
    kept = (out / "steps" / "run_map__p01__indigo_ab-1234.log").read_text()
    assert "elided" in kept and len(kept) < 5000
