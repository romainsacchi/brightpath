"""Exact nutritional references cannot be replaced by unvalidated crop or heat supplies."""

import json
from collections import Counter
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_reference_signs import (
    APPROVED_NUTRIENT_REFERENCE_OMISSIONS,
    PROFILE_OMISSION_APPROVED_PROFILES,
)

WHITELIST = json.loads((DATA_DIR / "export/uvek_feed_nutrient_omission_review.json").read_text())
PROFILES = [
    profile
    for profile in sorted({label for rule in WHITELIST["rules"] for label, _ in rule["observations"]})
    if profile.endswith("-cutoff")
]


def nutrient_data(label, amount):
    data = None
    for rule in WHITELIST["rules"]:
        if label not in {p for p, _ in rule["observations"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            batch = consumer(rule["source"], amount, uncertainty, kind)
            row = batch[0]["exchanges"][1]
            row["audit_note"] = "original nutrient reference"
            if data is None:
                data = batch
            else:
                data[0]["exchanges"].append(row)
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
            "type": "biosphere",
        }
    )
    return data


@pytest.mark.parametrize("label", PROFILES)
@pytest.mark.parametrize("release", ["2025", "2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_positive_reference_nutrient_omissions_warn_and_preserve_full_source(label, release, amount):
    _, version, model = label.split("-")
    data = nutrient_data(label, amount)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(
        contexts("uvek", release).background,
        policy=MigrationPolicy(
            on_heuristic_mapping=PolicyAction.WARN,
            on_deletion=PolicyAction.ERROR,
            on_information_loss=PolicyAction.ERROR,
        ),
    )
    assert result.data[0]["exchanges"] == [before[0]["exchanges"][0], before[0]["exchanges"][-1]]
    warnings = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_omission"]
    losses = [i for i in result.last_migration_report.losses if i.code == "migration.technosphere_omission"]
    assert len(warnings) == len(losses) == len(before[0]["exchanges"]) - 2
    originals = {f"datasets[0].exchanges[{i}]": e for i, e in enumerate(before[0]["exchanges"])}
    for loss, warning in zip(losses, warnings, strict=True):
        original = originals[loss.path]
        assert loss.details["source_exchange"] == original
        assert warning.severity.value == "warning"
        assert "nutritional supply contributions and any associated substitution credits" in warning.message
        assert "method-dependent and unquantified" in warning.message
        basis = (
            "gross nutritional feed energy in MJ"
            if original["unit"] == "megajoule"
            else "100%-crude-protein feed in kg"
        )
        assert basis in warning.message
    assert inventory.data == data == before


def test_historical_nutrient_approval_is_bound_to_95_identities_692_positive_profiles_and_prior_targets():
    resource = historical_forward_resource()
    whitelist = {tuple(r["source"][f] for f in FIELDS): r for r in WHITELIST["rules"]}
    assert len(whitelist) == WHITELIST["source_count"] == 95
    assert Counter(s[1] for s in whitelist) == {"energy feed, gross": 45, "protein feed, 100% crude": 50}
    assert set(whitelist) == set(APPROVED_NUTRIENT_REFERENCE_OMISSIONS)
    rules = [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in whitelist]
    assert len(rules) == 95
    count = 0
    for rule in rules:
        source = tuple(rule["source"][f] for f in FIELDS)
        evidence = whitelist[source]
        assert rule["target"] == evidence["previous_target"]
        profiles = rule["reference_flow_sign_review"]["profiles"]
        assert len(profiles) == len(evidence["observations"])
        assert PROFILE_OMISSION_APPROVED_PROFILES[source] == {p for p, _ in evidence["observations"]}
        for entry in profiles:
            assert entry["operation"] == "delete" and entry["approved_omission"]
            assert entry["review"]["status"] == "verified"
            assert entry["review"]["source_sign"] == entry["review"]["target_sign"] == 1
            assert "conversion_factor" not in entry
            assert resource["reviews"][entry["review_ref"]] == APPROVED_NUTRIENT_REFERENCE_OMISSIONS[source]["review"]
            count += 1
    assert count == WHITELIST["profile_pairs"] == 692


def test_nutrient_omissions_preserve_local_foreground_suppliers_and_cached_rules():
    resource = load_uvek_technosphere_resource()
    before = deepcopy(resource)
    for label in PROFILES:
        _, version, model = label.split("-")
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
        data = nutrient_data(label, -10)
        sources = {tuple(e[f] for f in FIELDS) for e in data[0]["exchanges"][1:-1]}
        assert sources <= {tuple(r["source"][f] for f in FIELDS) for r in selected["delete"]}
        for source in sources:
            supplier = dict(zip(FIELDS, source, strict=True))
            data.append({**supplier, "exchanges": [{**supplier, "amount": 1, "type": "production"}]})
        original = deepcopy(data)
        result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
            contexts("uvek", "2025").background, policy=MigrationPolicy.permissive()
        )
        assert result.data == data == original
    assert resource == before


def test_nutrient_omissions_do_not_infer_approval_for_missing_profiles():
    source = WHITELIST["rules"][0]
    labels = {label for label, _ in source["observations"]}
    missing = next((label for label in PROFILES if label not in labels), "ecoinvent-3.10.1-cutoff")
    for label in (missing, "ecoinvent-3.9.1-cutoff"):
        _, version, model = label.split("-")
        data = consumer(source["source"], -10)
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
        assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
        assert not any(i.code == "migration.technosphere_omission" for i in caught.value.report.issues)
        assert inventory.data == data
