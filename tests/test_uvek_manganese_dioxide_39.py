"""MnO2 supply keeps its reaction, allocated recovery share and reference basis."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_battery_manganese_dioxide_39_review.json").read_text())
ROWS = REVIEW["records"]
MARKET = "battery-39-manganese-dioxide-delivery"
PRODUCER = "battery-39-manganese-dioxide-production"
STOCK = "battery-39-manganese-dioxide-recovered-stock"


def policy(approved=True):
    resource = load_uvek_technosphere_resource()
    scopes = tuple(
        compatibility_rule_fingerprint(
            resource,
            "replace",
            row["compiled_rule"],
            TechnosphereProfile(**REVIEW["source_profile"]),
            TechnosphereProfile(**REVIEW["target_profile"]),
        )
        for row in ROWS
    )
    return MigrationPolicy(
        approved_compatibility_rules=scopes if approved else (), on_information_loss=PolicyAction.WARN
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("uncertainty", [2, 3])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_reaction_supply_and_allocated_stock_keep_signed_caller_coordinates(amount, uncertainty, release):
    sources = [row["compiled_rule"]["source"] for row in ROWS]
    data = consumer(sources[0], amount, uncertainty)
    data[0]["exchanges"] += [consumer(source, amount, uncertainty)[0]["exchanges"][1] for source in sources[1:]]
    original = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert data == original
    for actual, old in zip(result.data[0]["exchanges"][1:], original[0]["exchanges"][1:], strict=True):
        for key in ("amount", "loc", "scale", "negative", "minimum", "maximum", "formula", "uncertainty type"):
            if key in old:
                assert actual[key] == old[key]
    helpers = {a["brightpath_conversion"]["recipe_id"]: a for a in result.data[1:]}
    assert {MARKET, PRODUCER, STOCK} <= helpers.keys()
    assert sum(a["brightpath_conversion"]["recipe_id"] == STOCK for a in result.data[1:]) == 1
    purchases = helpers[MARKET]["exchanges"][1:]
    assert len(purchases) == 6
    assert [edge["amount"] for edge in purchases[:2]] == [0.998336106489185, 0.00166389351081531]
    assert [edge["amount"] for edge in purchases[2:]] == [0.3091, 0.0246, 0.2088, 0.599]
    production = helpers[PRODUCER]["exchanges"][1:]
    technology = [edge for edge in production if edge["type"] == "technosphere"]
    biosphere = [edge for edge in production if edge["type"] == "biosphere"]
    assert len(technology) == 13 and len(biosphere) == 9
    for edge, binding in zip(technology, REVIEW["component_bindings"][:13], strict=True):
        assert edge["amount"] == binding["source_amount"] * binding["conversion_factor"]
        assert edge.get("uncertainty type", 0) == binding["source_statistics"].get("uncertainty type", 0)
        assert edge.get("scale") == binding["source_statistics"].get("scale")
        if binding["source_statistics"].get("uncertainty type") == 2:
            assert edge["loc"] == binding["source_statistics"]["loc"]
    precursor = technology[5]
    assert precursor["name"] == "Manganese oxide (Mn2O3), at plant"
    assert precursor["location"] == "CN" and precursor["amount"] == 1.1312598767454822
    assert all("Lithium manganese oxide" not in edge["name"] for edge in production)
    assert [edge["amount"] for edge in biosphere] == [
        0.0007873310206133219,
        0.0014055674489186472,
        0.0008285253811450413,
        0.001993190636394771,
        0.00127218372829704,
        0.010005627585013723,
        0.009705583036270483,
        0.0005089513055605253,
        0.0004911971902502745,
    ]
    assert len(helpers[STOCK]["exchanges"]) == 1
    assert helpers[STOCK]["exchanges"][0]["type"] == "production"
    assert helpers[STOCK]["brightpath_conversion"]["zero_burden"] is True
    assert any(issue.code == "migration.zero_burden_supplier" for issue in result.last_migration_report.issues)


@pytest.mark.parametrize(
    "version,model,approved",
    [("3.9", "cutoff", False), ("3.8", "cutoff", True), ("3.9.1", "cutoff", True), ("3.9", "consequential", True)],
)
def test_root_approval_does_not_expand_to_other_source_contexts(version, model, approved):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[0]["compiled_rule"]["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy(approved))


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10"])
def test_uninspected_stock_profile_is_not_zeroed_even_with_a_permissive_policy(version):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROWS[1]["compiled_rule"]["source"], 1), context=contexts("ecoinvent", version)
        ).migrate_background(contexts("uvek", "2026").background, policy=MigrationPolicy.permissive())


def test_stock_has_a_paired_nonempty_treatment_and_no_new_omissions():
    assert any(
        a["reference_amounts"] == [-1] and a["nonproduction_count"] == 22 for a in REVIEW["paired_treatment_witnesses"]
    )
    assert ROWS[1]["source_witness"]["nonproduction_count"] == 0
    assert ROWS[1]["source_witness"]["reference_amount"] == 1
    assert REVIEW["source_formulas"] == 0 and REVIEW["new_foreground_omissions"] == []
    assert REVIEW["producer_technosphere_entries"] == 13 and REVIEW["producer_biosphere_entries"] == 9
