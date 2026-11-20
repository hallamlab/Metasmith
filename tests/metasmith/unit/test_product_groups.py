from __future__ import annotations

import pytest

from metasmith.models.libraries.transforms import TransformInstance
from metasmith.models.solver import Transform


def _fork_sharing_a() -> tuple[Transform, object]:
    model = Transform()
    dep = model.AddRequirement(properties={"start"})
    a = model.AddProduct(properties={"a"})
    model.AddProduct(properties={"b"})
    model.NewProductGroup()
    model.AddProduct(a)
    model.AddProduct(properties={"c"})
    return model, dep


def test_a_shared_product_is_one_dependency_in_both_groups():
    model, _ = _fork_sharing_a()
    first, second = model.produces
    assert first[0] is second[0]
    assert {next(iter(d.properties)) for d in first} == {"a", "b"}
    assert {next(iter(d.properties)) for d in second} == {"a", "c"}


def test_a_product_cannot_repeat_within_its_group():
    model = Transform()
    model.AddRequirement(properties={"start"})
    a = model.AddProduct(properties={"a"})
    with pytest.raises(AssertionError):
        model.AddProduct(a)


def test_two_groups_with_the_same_outputs_are_refused():
    model = Transform()
    dep = model.AddRequirement(properties={"start"})
    model.AddProduct(properties={"a"})
    model.NewProductGroup()
    model.AddProduct(properties={"a"})
    with pytest.raises(AssertionError, match="same outputs"):
        TransformInstance(protocol=lambda c: None, model=model, group_by=dep)


def test_a_fork_with_distinct_groups_is_accepted():
    model, dep = _fork_sharing_a()
    TransformInstance(protocol=lambda c: None, model=model, group_by=dep)
