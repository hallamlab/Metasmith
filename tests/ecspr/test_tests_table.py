"""The tests tables: one construction, several measurements, two arms."""
from __future__ import annotations

import pandas as pd
import pytest

from ecspr.model import tests as T


def _ecspr(test_id, network, **kw):
    read_against = kw.pop("read_against", "")
    return T.Test(test_id=test_id, network=network, arm="ecspr",
                  read_against=read_against, settings=kw)


def test_one_construction_carries_several_probes(tmp_path):
    # THE POINT OF THE SPLIT. Both tests name the same network and differ only in what
    # they ask of it; on the old wide table this was two conditions that happened to
    # carry identical masks.
    ts = [_ecspr("dh1/ground", "dh1", element="C", source_hub="GLC",
                 sink_hub="ATP|NADH", probe="ground"),
          _ecspr("dh1/two_point", "dh1", element="C", source_hub="GLC",
                 sink_hub="ATP", probe="two-point")]
    back = T.read(T.write(ts, tmp_path / "ecspr_tests.tsv", "ecspr"), "ecspr")
    assert {t.network for t in back} == {"dh1"}
    assert [t.probe for t in back] == ["ground", "two-point"]
    assert back[0].sinks == ("ATP", "NADH")


def test_read_against_names_a_test_not_a_network(tmp_path):
    ts = [_ecspr("bg", "dh1", source_hub="GLC", sink_hub="ATP"),
          _ecspr("gof", "dh1_glgA", source_hub="GLC", sink_hub="ATP",
                 read_against="bg")]
    back = T.read(T.write(ts, tmp_path / "t.tsv", "ecspr"), "ecspr")
    assert back[1].read_against == "bg"
    assert back[1].network == "dh1_glgA"


def test_a_dangling_baseline_is_refused(tmp_path):
    # A baseline that resolves to nothing makes a scorer compare against zero, which
    # reads as a very large effect rather than as a broken reference.
    ts = [_ecspr("gof", "dh1_glgA", source_hub="GLC", read_against="nope")]
    p = T.write(ts, tmp_path / "t.tsv", "ecspr")
    with pytest.raises(ValueError, match="read_against names"):
        T.read(p, "ecspr")


def test_a_test_must_name_its_network(tmp_path):
    p = tmp_path / "t.tsv"
    pd.DataFrame([dict(test_id="x", element="C")]).to_csv(p, sep="\t", index=False)
    with pytest.raises(ValueError, match="no 'network' column"):
        T.read(p, "ecspr")


def test_defaults_fill_what_the_table_leaves_blank(tmp_path):
    # A driver supplies one source hub for a whole sweep rather than writing it on
    # every row.
    p = tmp_path / "t.tsv"
    pd.DataFrame([dict(test_id="x", network="dh1", sink_hub="ATP")]).to_csv(
        p, sep="\t", index=False)
    t = T.read(p, "ecspr", defaults=dict(source_hub="GLC", element="C"))[0]
    assert (t.source_hub, t.element, t.sinks) == ("GLC", "C", ("ATP",))


def test_the_two_arms_carry_different_columns(tmp_path):
    gem = T.Test(test_id="dh1/fba", network="dh1", arm="gem",
                 settings=dict(frame="iML1515", media="kornberg",
                               objective="BIOMASS", target_hub="glycogen",
                               growth_pin=0.1, min_lanes=1))
    p = T.write([gem], tmp_path / "gem_tests.tsv", "gem")
    cols = list(pd.read_csv(p, sep="\t").columns)
    assert cols == list(T.GEM_COLS)
    assert "element" not in cols and "source_hub" not in cols
    back = T.read(p, "gem")[0]
    assert back.media == "kornberg"
    assert back.settings["min_lanes"] == 1


def test_media_is_not_an_ecspr_column():
    # It was declared on every condition and read by nothing. A conductance measurement
    # has no exchange bounds to set.
    assert "media" in T.GEM_COLS
    assert "media" not in T.ECSPR_COLS


def test_results_key_on_the_test(tmp_path):
    assert T.RESULT_KEY == ("test_id", "readout")


def test_an_unknown_arm_is_refused():
    with pytest.raises(ValueError, match="arm"):
        T.Test(test_id="x", network="n", arm="flux")
