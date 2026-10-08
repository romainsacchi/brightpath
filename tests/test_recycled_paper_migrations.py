from copy import deepcopy

import pytest
from test_composed_uvek_exports import POLICY, context, inventory

from brightpath import MigrationError
from brightpath.background import PackageCatalogProvider
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_technosphere_resources

OLD_NAME = "tissue paper production"
NEW_NAME = "tissue paper production, recycled"
UVEK_NAME = "Paper, recycling, with deinking, at plant"


def paper_exchange(name, exchange_type="technosphere"):
    return {
        "name": name,
        "reference product": UVEK_NAME if name == UVEK_NAME else "tissue paper",
        "location": "RER",
        "unit": "kilogram",
        "type": exchange_type,
        "amount": 0.75,
        "uncertainty type": 3,
        "loc": 0.75,
        "scale": 0.03,
        "minimum": 0.5,
        "maximum": 1.0,
    }


@pytest.mark.parametrize("source_version", ["3.10", "3.10.1"])
@pytest.mark.parametrize("target_version", ["3.11", "3.12"])
def test_paper_output_forward_rename_preserves_quantities_and_original(source_version, target_version):
    exchange = paper_exchange(OLD_NAME)
    initial = inventory(context(source_version), [exchange])
    original = deepcopy(initial.data)
    result = initial.migrate_background(context(target_version))
    assert result.data[0]["exchanges"][1] == {**exchange, "name": NEW_NAME, "product": "tissue paper"}
    assert initial.data == original
    assert result.migrate_background(context(target_version)).data == result.data


@pytest.mark.parametrize(
    "source_family,source_version", [("ecoinvent", "3.11"), ("ecoinvent", "3.12"), ("uvek", "2025")]
)
@pytest.mark.parametrize("target_version", ["3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1"])
def test_paper_output_reverse_routes_use_the_recycled_process(source_family, source_version, target_version):
    exchange = paper_exchange(UVEK_NAME if source_family == "uvek" else NEW_NAME)
    initial = inventory(context(source_version, source_family), [exchange])
    original = deepcopy(initial.data)
    result = initial.migrate_background(context(target_version), policy=POLICY)
    migrated = result.data[0]["exchanges"][1]
    for field, value in exchange.items():
        expected = OLD_NAME if field == "name" else "tissue paper" if field == "reference product" else value
        assert migrated[field] == expected
    assert initial.data == original
    assert result.migrate_background(context(target_version)).data == result.data
    assert not any("ambiguous" in issue.code for issue in result.last_migration_report.issues)


def test_paper_substitution_keeps_direction_and_negative_amount():
    exchange = {
        **paper_exchange(NEW_NAME, "substitution"),
        "amount": -0.75,
        "loc": -0.75,
        "minimum": -1.0,
        "maximum": -0.5,
    }
    initial = inventory(context("3.11"), [exchange])
    result = initial.migrate_background(context("3.10.1"), policy=POLICY)
    assert result.data[0]["exchanges"][1] == {**exchange, "name": OLD_NAME, "product": "tissue paper"}


def test_paper_reverse_route_still_requires_explicit_reverse_policy():
    initial = inventory(context("3.11"), [paper_exchange(NEW_NAME)])
    original = deepcopy(initial.data)
    with pytest.raises(MigrationError):
        initial.migrate_background(context("3.10.1"))
    assert initial.data == original
    with pytest.raises(MigrationError):
        initial.migrate_background(context("3.10.1"), policy=MigrationPolicy(on_inferred_reverse=PolicyAction.WARN))
    result = initial.migrate_background(context("3.10.1"), policy=POLICY)
    assert result.data[0]["exchanges"][1]["name"] == OLD_NAME


@pytest.mark.parametrize(
    "field,value", [("location", "Unreviewed"), ("reference product", "Unreviewed"), ("unit", "megajoule")]
)
def test_paper_rename_does_not_accept_unreviewed_identities(field, value):
    initial = inventory(context("3.11"), [{**paper_exchange(NEW_NAME), field: value}])
    original = deepcopy(initial.data)
    with pytest.raises(MigrationError):
        initial.migrate_background(context("3.10.1"), policy=POLICY)
    assert initial.data == original


def test_reviewed_output_rule_does_not_replace_the_upstream_waste_product_rules():
    resource = load_technosphere_resources("cutoff")[("3.10", "3.11")]
    rules = [rule for rule in resource["replace"] if rule["source"]["name"] == OLD_NAME]
    output_rules = [rule for rule in rules if rule["source"]["reference product"] == "tissue paper"]
    assert len(output_rules) == 1
    rule = output_rules[0]
    assert rule["source"]["location"] == rule["target"]["location"] == "RER"
    assert rule["source"]["unit"] == rule["target"]["unit"] == "kg"
    assert rule["target"]["name"] == NEW_NAME
    assert rule["brightpath_review"]["product_uuid"] == "e04720d0-1b18-41bd-b729-93cfc5f862ab"
    assert rule["brightpath_review"]["evidence"] == "docs/recycled-paper-migration.rst"
    waste_rules = [rule for rule in rules if rule["source"]["reference product"] == "waste paper, sorted"]
    assert {rule["source"]["location"] for rule in waste_rules} == {"GLO", "RER"}
    assert all(rule["target"]["reference product"] == "waste paper, sorted" for rule in waste_rules)


@pytest.mark.parametrize("version", ["3.10", "3.10.1", "3.11", "3.12"])
def test_paper_output_identity_is_present_in_each_exact_target(version):
    catalog = PackageCatalogProvider().load_technosphere(context(version).technosphere)
    expected = OLD_NAME if version in {"3.10", "3.10.1"} else NEW_NAME
    assert (expected, "tissue paper", "RER", "kilogram") in catalog.identities
