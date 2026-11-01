import math
from copy import deepcopy

import pytest

from brightpath.background.catalogs import BiosphereCatalog, InMemoryCatalogProvider, TechnosphereCatalog
from brightpath.background.execution import _apply_factored_disaggregation, execute_background_migration
from brightpath.core.context import (
    BackgroundContext,
    BiosphereProfile,
    FormatProfile,
    InventoryContext,
    TechnosphereProfile,
)
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.engine import _apply_disaggregation
from brightpath.migrations.models import MigrationStepReport
from brightpath.migrations.resources import load_technosphere_resources
from brightpath.migrations.scaling import scale_exchange
from brightpath.models import InventoryDocument


def identity(value):
    return (value["name"], value["reference product"], value["location"], "kilogram")


def acetylene_rule():
    return deepcopy(
        next(
            rule
            for rule in load_technosphere_resources("cutoff")[("3.11", "3.12")]["disaggregate"]
            if rule["source"]["name"] == "acetylene production" and rule["source"]["location"] == "RER"
        )
    )


def exchange(**changes):
    value = {
        **acetylene_rule()["source"],
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 0.42733,
        "uncertainty type": 2,
        "loc": math.log(0.42733),
        "scale": 0.2249323477024245,
        "comment": "Authored metadata must remain intact.",
    }
    value.update(changes)
    return value


def step():
    return MigrationStepReport("3.11", "3.12", "forward", "allocation regression")


@pytest.mark.parametrize("offset", [0.0, math.log(1.3)])
def test_split_scales_lognormal_centre_without_recentering_independent_source(offset):
    original = exchange(loc=math.log(0.42733) + offset, formula="dose")
    data = [{"exchanges": [deepcopy(original)]}]
    report = step()
    _apply_disaggregation(data, [acetylene_rule()], report)
    assert not report.issues
    assert report.technosphere_disaggregations == 1
    assert len(data[0]["exchanges"]) == 2
    for child in data[0]["exchanges"]:
        assert child["amount"] == pytest.approx(0.213665)
        assert child["loc"] == pytest.approx(original["loc"] + math.log(0.5))
        assert child["loc"] - math.log(child["amount"]) == pytest.approx(offset)
        assert child["scale"] == original["scale"]
        assert child["formula"] == "(dose) * (0.5)"
        assert child["comment"] == original["comment"]
    assert original == exchange(loc=math.log(0.42733) + offset, formula="dose")


def test_normal_split_scales_standard_deviation_and_bounds():
    original = exchange(amount=4.0, **{"uncertainty type": 3, "loc": 5.0, "scale": 2.0, "minimum": 1.0, "maximum": 9.0})
    data = [{"exchanges": [deepcopy(original)]}]
    _apply_disaggregation(data, [acetylene_rule()], step())
    for child in data[0]["exchanges"]:
        assert (child["amount"], child["loc"], child["scale"], child["minimum"], child["maximum"]) == (
            2.0,
            2.5,
            1.0,
            0.5,
            4.5,
        )


def test_factored_split_scales_allocation_and_documented_unit_factor_once():
    rule = acetylene_rule()
    for target in rule["targets"]:
        target.update(unit="gram", conversion_factor=1000.0)
    original = exchange(formula="dose")
    data = [{"exchanges": [deepcopy(original)]}]
    report = step()
    _apply_factored_disaggregation(data, [rule], report)
    assert not report.issues
    for child in data[0]["exchanges"]:
        assert child["amount"] == pytest.approx(original["amount"] * 500.0)
        assert child["loc"] == pytest.approx(original["loc"] + math.log(500.0))
        assert child["scale"] == original["scale"]
        assert child["formula"] == "(dose) * (500.0)"
        assert child["unit"] == "gram"


@pytest.mark.parametrize("factored", [False, True])
@pytest.mark.parametrize("kind", ["zero_allocation", "unsupported"])
def test_unscalable_split_retains_whole_input_exchange_and_reports_error(factored, kind):
    rule = acetylene_rule()
    original = exchange()
    if kind == "zero_allocation":
        rule["targets"][0]["allocation"] = 1.0
        rule["targets"][1]["allocation"] = 0.0
    else:
        original.update({"uncertainty type": 6, "loc": 0.4, "minimum": 0.1, "maximum": 0.8})
    data = [{"exchanges": [deepcopy(original)]}]
    before = deepcopy(data)
    report = step()
    (_apply_factored_disaggregation if factored else _apply_disaggregation)(data, [rule], report)
    assert data == before
    assert report.technosphere_disaggregations == 0
    assert any(
        issue.severity == "error" and issue.code == "migration_disaggregation_scaling_invalid"
        for issue in report.issues
    )


def migration_input(original):
    source = BackgroundContext(
        TechnosphereProfile("ecoinvent", "3.11", "cutoff"), BiosphereProfile("ecoinvent", "3.11")
    )
    target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), source.biosphere)
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(source.technosphere, {identity(acetylene_rule()["source"])}),
            TechnosphereCatalog(target.technosphere, {identity(value) for value in acetylene_rule()["targets"]}),
        ],
        biosphere=[BiosphereCatalog(source.biosphere, set())],
    )
    document = InventoryDocument(
        data=[
            {
                "name": "HFC foreground",
                "reference product": "HFC",
                "location": "US",
                "unit": "kilogram",
                "exchanges": [original],
            }
        ],
        context=InventoryContext(format=FormatProfile("brightway_excel"), background=source),
    )
    return document, target, provider


def test_actual_forward_executor_scales_marginals_and_preserves_caller_document():
    document, target, provider = migration_input(exchange())
    before = document.data
    result = execute_background_migration(document, target, provider)
    assert result.succeeded
    assert document.data == before
    for child in result.value.data[0]["exchanges"]:
        assert math.exp(child["loc"]) == pytest.approx(child["amount"])
        assert child["amount"] == pytest.approx(0.213665)


@pytest.mark.parametrize("loss_action", [PolicyAction.ERROR, PolicyAction.WARN])
def test_unsupported_distribution_refuses_transactionally_without_partial_output(loss_action):
    original = exchange(**{"uncertainty type": 6, "loc": 0.4, "minimum": 0.1, "maximum": 0.8})
    document, target, provider = migration_input(original)
    before = document.data
    result = execute_background_migration(
        document, target, provider, policy=MigrationPolicy(on_information_loss=loss_action)
    )
    assert not result.succeeded
    assert result.value.data == document.data == before
    assert any(issue.code == "migration.disaggregation_scaling_invalid" for issue in result.report.issues)
    assert any(
        issue.code == "migration.disaggregation_scaling_invalid" and issue.severity.value == "error"
        for issue in result.report.issues
    )


@pytest.mark.parametrize("factor", [0.5, -2.0])
def test_triangular_scaling_transforms_mode_and_ordered_bounds_without_scaling_inactive_sigma(factor):
    original = exchange(
        amount=0.0148,
        **{"uncertainty type": 5, "loc": 0.0148, "minimum": 0.0074, "maximum": 0.0222},
        formula="neutraliser",
    )
    before = deepcopy(original)
    scaled = deepcopy(original)
    scale_exchange(scaled, factor)
    assert original == before
    assert scaled["amount"] == pytest.approx(original["amount"] * factor)
    assert scaled["loc"] == pytest.approx(original["loc"] * factor)
    bounds = sorted([original["minimum"] * factor, original["maximum"] * factor])
    assert [scaled["minimum"], scaled["maximum"]] == pytest.approx(bounds)
    assert scaled["minimum"] <= scaled["loc"] <= scaled["maximum"]
    assert scaled["scale"] == original["scale"]
    assert scaled["formula"] == f"(neutraliser) * ({factor!r})"
    assert scaled["comment"] == original["comment"]


def test_actual_forward_executor_scales_triangular_marginal_without_mutating_caller():
    original = exchange(**{"uncertainty type": 5, "loc": 0.4, "minimum": 0.1, "maximum": 0.8}, formula="dose")
    document, target, provider = migration_input(original)
    before = deepcopy(document.data)
    result = execute_background_migration(document, target, provider)
    assert result.succeeded
    assert document.data == before
    for child in result.value.data[0]["exchanges"]:
        assert child["amount"] == pytest.approx(original["amount"] * 0.5)
        assert child["loc"] == pytest.approx(0.2)
        assert child["minimum"] == pytest.approx(0.05)
        assert child["maximum"] == pytest.approx(0.4)
        assert child["scale"] == original["scale"]
        assert child["formula"] == "(dose) * (0.5)"


def test_strict_reverse_does_not_claim_an_exact_uncertain_roundtrip():
    document, target, provider = migration_input(exchange())
    forward = execute_background_migration(document, target, provider)
    assert forward.succeeded
    before_reverse = forward.value.data
    reverse = execute_background_migration(forward.value, document.context.background, provider)
    assert not reverse.succeeded
    assert reverse.value.data == forward.value.data == before_reverse


def test_lognormal_split_preserves_authored_negative_sign():
    original = exchange(amount=-0.42733, negative=True)
    data = [{"exchanges": [deepcopy(original)]}]
    _apply_disaggregation(data, [acetylene_rule()], step())
    for child in data[0]["exchanges"]:
        assert child["amount"] == pytest.approx(-0.213665)
        assert child["negative"] is True
        assert child["loc"] == pytest.approx(original["loc"] + math.log(0.5))
