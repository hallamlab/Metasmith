"""A community, declared: what the table says and what a member is."""
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.model import compose as ec
from ecspr.model import compositions as X
from ecspr.model import conditions as C


def _rows(unit, orfs, mnxrs):
    return [dict(source=unit, orf=o, gene=o, channel="clean", mnxr=r,
                 intermediate_id=r, intermediate_name=r, llr=1.0,
                 llr_route="calibrated", projection_via="t", evidence_quality="q",
                 lane_set="clean", build_id="b", host="h", genome_unit=unit)
            for o in orfs for r in mnxrs]


CORPUS = {"nos.parquet": pd.DataFrame(_rows("nos", ["n1", "n2"], ["R1", "R2"])),
          "ery.parquet": pd.DataFrame(_rows("ery", ["e1"], ["R2", "R3"]))}

CONDITIONS = {
    "nos": C.single("nos", "nos.parquet"),
    "ery": C.single("ery", "ery.parquet"),
    # A PERTURBED MEMBER. The knockout is written in the conditions table like every
    # other perturbation in this project, and the composition just names it.
    "ery_ko": C.Condition("ery_ko", (
        C.Step(table="ery.parquet", type="", set=(), operation="union"),
        C.Step(table="ery.parquet", type="mnxr", set=("R3",), operation="subtract"))),
}

PAIRS = pd.DataFrame([
    dict(mnxr=r, element="C", substrate=s, product=p, sub_idx=0, prod_idx=0, pair_w=1.0)
    for r, s, p in (("R1", "S", "X"), ("R2", "X", "P1"), ("R3", "P1", "Y"))])


def test_a_blank_member_b_is_a_one_member_network(tmp_path):
    # WHY IT MATTERS: every ecspr_test names a composition, so a single organism has to be
    # expressible here rather than being a second kind of thing a test can name.
    p = X.write([X.single("solo", "nos")], tmp_path / "c.parquet")
    back = X.read(p)[0]
    assert back.members == ("nos",) and back.is_singleton and back.bridges == {}


def test_the_on_column_accumulates_across_rows(tmp_path):
    df = pd.DataFrame([
        dict(composition_id="c", member_a="nos", member_b="ery", on="MNXM40"),
        dict(composition_id="c", member_a="nos", member_b="ery", on="MNXM26"),
    ])
    p = tmp_path / "c.tsv"
    df.to_csv(p, sep="\t", index=False)
    c = X.read(p)[0]
    assert c.declared_for("nos", "ery") == ("MNXM40", "MNXM26")
    assert c.declared_for("ery", "nos") == ("MNXM40", "MNXM26"), "a bridge is symmetric"


def test_a_blank_on_means_bridge_on_everything(tmp_path):
    p = X.write([X.open_pair("open", "nos", "ery")], tmp_path / "c.parquet")
    c = X.read(p)[0]
    assert c.bridges == {("nos", "ery"): ()}


def test_a_member_is_a_condition_and_can_be_perturbed():
    c = X.Composition("nos_eryko", ("nos", "ery_ko"), {("nos", "ery_ko"): ()})
    members = X.resolve_members(c, CONDITIONS, CORPUS)
    assert set(members) == {"nos", "ery_ko"}
    assert set(members["ery_ko"].mnxr) == {"R2"}, \
        "the knockout member lost R3, which is the whole point of a member being a condition"
    assert set(members["nos"].mnxr) == {"R1", "R2"}


def test_a_member_naming_no_condition_is_refused():
    c = X.Composition("bad", ("nos", "nobody"), {("nos", "nobody"): ()})
    with pytest.raises(ValueError, match="not condition_ids"):
        X.resolve_members(c, CONDITIONS, CORPUS)


def test_composing_a_declared_community_end_to_end():
    c = X.Composition("nos_ery", ("nos", "ery"), {("nos", "ery"): ("P1",)})
    out = ec.compose_composition(c, CONDITIONS, CORPUS, PAIRS, elements=("C",), g0=1.0)
    mets = set(out["pairs"].substrate) | set(out["pairs"]["product"])
    assert "nos:X" in mets and "ery:X" in mets, "each member keeps a private copy"
    assert out["report"]["network_id"] == "nos_ery"
    assert out["report"]["n_declared"] == 1
    assert set(out["bridges"].metabolite) == {"P1"}, \
        "a declared list restricts the bridges to what it names"


def test_a_strain_beside_its_own_mutant_composes():
    # The two members share every ORF id by construction. That used to raise.
    c = X.Composition("ery_vs_ko", ("ery", "ery_ko"), {("ery", "ery_ko"): ()})
    out = ec.compose_composition(c, CONDITIONS, CORPUS, PAIRS, elements=("C",), g0=1.0)
    orfs = {o for o in out["gpr"]["orf"] if not o.startswith("__bridge__")}
    assert orfs == {"ery:e1", "ery_ko:e1"}
    assert out["report"]["n_orfs"] == len(set(out["gpr"]["orf"]))


def test_the_table_round_trips(tmp_path):
    comps = [X.single("solo", "nos"),
             X.Composition("pair", ("nos", "ery"), {("nos", "ery"): ("MNXM40",)}),
             X.open_pair("open", "nos", "ery")]
    p = X.write(comps, tmp_path / "c.parquet")
    back = {c.composition_id: c for c in X.read(p)}
    assert set(back) == {"solo", "pair", "open"}
    assert back["pair"].declared_for("nos", "ery") == ("MNXM40",)
    assert back["open"].bridges == {("nos", "ery"): ()}
    assert back["solo"].is_singleton


def test_a_table_with_no_member_a_is_refused(tmp_path):
    p = tmp_path / "c.tsv"
    pd.DataFrame({"composition_id": ["c"], "on": ["x"]}).to_csv(p, sep="\t", index=False)
    with pytest.raises(ValueError, match="member_a"):
        X.read(p)
