import pytest

from conftest import _find_generator_case
from scripts.data_generators.tests import IcebergTest
from scripts.data_generators.tests.default.spark_rewrite_mor_residual_deletes import Test as RewriteGenerator


def fail_constructor(self):
    raise AssertionError("Lookup must not construct unrelated or ambiguous generators")


@pytest.mark.parametrize("name", ["spark_rewrite_mor_residual_deletes", "default.spark_rewrite_mor_residual_deletes"])
def test_lookup_only_constructs_selected_generator(monkeypatch, name):
    for generator_class in IcebergTest.registry:
        if generator_class is not RewriteGenerator:
            monkeypatch.setattr(generator_class, "__init__", fail_constructor)

    generator = _find_generator_case(name)
    assert isinstance(generator, RewriteGenerator)
    assert generator.qualified_name == "default.spark_rewrite_mor_residual_deletes"


def test_unknown_generator_does_not_construct_anything(monkeypatch):
    for generator_class in IcebergTest.registry:
        monkeypatch.setattr(generator_class, "__init__", fail_constructor)

    with pytest.raises(ValueError, match="No data generator registered"):
        _find_generator_case("missing_seed_table")


def test_ambiguous_generator_does_not_construct_anything(monkeypatch):
    monkeypatch.setattr(IcebergTest, "registry", [RewriteGenerator, RewriteGenerator])
    monkeypatch.setattr(RewriteGenerator, "__init__", fail_constructor)

    with pytest.raises(ValueError, match="Multiple data generators match"):
        _find_generator_case("spark_rewrite_mor_residual_deletes")
