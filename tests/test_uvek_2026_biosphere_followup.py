"""Context and chemical-identity follow-up must retain qualifiers and prior decisions."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.schema import dumps_migration_resource
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_biosphere_followup import (
    CAS_METHOD,
    METHODS,
    REPORT,
    apply_followup,
    candidate,
    valid_cas,
    validate_decision,
)
from tests.test_uvek_2026_biosphere_review import document
from tests.test_uvek_2026_biosphere_review import forward as initial_forward
from tests.test_uvek_2026_biosphere_review import resources as initial_resources
from tests.uvek_biosphere_history import before_land_review

ROOT = Path(__file__).resolve().parents[1]
CATALOGS = ROOT / "brightpath/data/export/reference_catalogs"


def resources():
    """Reconstruct the frozen 55-rule follow-up before subsequent SI additions."""
    result = before_land_review()
    resource = next(row for row in result if row["source_profiles"][0]["family"] == "uvek")
    if "radioactivity_review" in resource["provenance"]:
        from scripts.review_uvek_2026_radioactivity import METHOD
        from scripts.review_uvek_2026_radioactivity import REPORT as SI_REPORT

        review = json.loads(SI_REPORT.read_text())
        resource["replace"] = [row for row in resource["replace"] if row["mapping_method"] != METHOD]
        used = {row["review_ref"] for row in resource["replace"]}
        resource["reviews"] = {key: value for key, value in resource["reviews"].items() if key in used}
        resource["provenance"].pop("radioactivity_review")
        resource["unresolved"] = [row["source"] for row in review["decisions"]]
        resource["coverage"] = review["sources"]["baseline_coverage"]
    return result


def forward():
    return next(row for row in resources() if row["source_profiles"][0]["family"] == "uvek")


def additions():
    return [row for row in forward()["replace"] if row["mapping_method"] in METHODS]


def test_report_covers_all_previous_blockers_and_preserves_all_prior_rules():
    report = json.loads(REPORT.read_text())
    current = forward()
    assert report["summary"]["accepted_identities"] == len(additions()) == 55
    assert len(current["replace"]) == 1772 and len(current["unresolved"]) == 907
    assert len(report["decisions"]) == 962
    assert current["coverage"]["directional_additions"] == 482
    assert current["coverage"]["information_loss_rules"] == 90
    parent = initial_forward()
    assert (
        hashlib.sha256(dumps_migration_resource(parent).encode()).hexdigest()
        == report["sources"]["baseline_forward_sha256"]
    )
    prior = {row["id"]: row for row in parent["replace"]}
    assert {row["id"]: row for row in current["replace"] if row["id"] in prior} == prior
    assert apply_followup(initial_resources(), REPORT, CATALOGS) == resources()


@pytest.mark.parametrize("version", ["3.10", "3.12"])
def test_all_55_links_execute_with_unchanged_quantities_uncertainty_and_formulas(version):
    original = document(additions())
    data = original.data
    data[0]["parameters"] = [{"name": "dose", "amount": 12}]
    for exchange in data[0]["exchanges"][1:]:
        exchange["formula"] = "dose"
    original = original.replace(data=data)
    before = deepcopy(original.data)
    target = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    result = execute_background_migration(
        original,
        target,
        PackageCatalogProvider(),
        MigrationPolicy(
            on_information_loss=PolicyAction.WARN,
            on_inferred_reverse=PolicyAction.WARN,
        ),
    )
    assert result.succeeded, result.report.to_json()
    assert result.value.context.background == target
    assert len(result.value.data[0]["exchanges"]) == 56
    for exchange in result.value.data[0]["exchanges"][1:]:
        assert (exchange["amount"], exchange["loc"], exchange["scale"], exchange["formula"]) == (12, 12, 0.3, "dose")
        assert exchange["custom"] == [1]
    assert original.data == before


def test_long_term_water_context_is_retained_and_strict_accepts_lossless_rules():
    rules = [row for row in additions() if not row.get("biosphere_information_loss")]
    target = BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("ecoinvent", "3.10"))
    result = execute_background_migration(document(rules), target, PackageCatalogProvider())
    assert result.succeeded, result.report.to_json()
    for source, exchange in zip(rules, result.value.data[0]["exchanges"][1:], strict=True):
        if source["source"]["categories"][-1] == "groundwater, long-term":
            assert tuple(exchange["categories"]) == ("water", "ground-, long-term")


@pytest.mark.parametrize(
    "source",
    [
        {"name": "Water, CH", "categories": ["emissions to water", "unspecified"], "unit": "cubic meter"},
        {
            "name": "Dioxin, 2,3,7,8 Tetrachlorodibenzo-p-",
            "categories": ["emissions to air", "high. pop."],
            "unit": "kilogram",
        },
        {"name": "Carbon dioxide, biogenic", "categories": ["emissions to air", "unspecified"], "unit": "kilogram"},
        {"name": "Gas, natural/m3", "categories": ["resources", "in ground"], "unit": "normal cubic meter"},
    ],
)
def test_cas_cannot_discard_geography_equivalent_basis_carbon_origin_or_gas_units(source):
    catalog = json.loads((CATALOGS / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    _reason, match = candidate(source, {"cas": {"7732-18-5"}, "formulas": set()}, [], catalog)
    assert match is None


@pytest.mark.parametrize(
    "mutation",
    [
        "cas",
        "cas_ambiguity",
        "formula",
        "uuid",
        "unit",
        "context",
        "legacy_context",
        "empty_legacy",
        "publisher_ambiguity",
        "scale",
    ],
)
def test_cas_correspondence_requires_unambiguous_compatible_publisher_metadata(mutation):
    report = json.loads(REPORT.read_text())
    decision = deepcopy(next(row for row in report["decisions"] if row["reason"] == CAS_METHOD and "target" in row))
    catalog = json.loads((CATALOGS / "ecoinvent__3.10__cutoff.json").read_text())["biosphere"]
    if mutation == "cas":
        decision["source_cas"] = ["123-45-6"]
    elif mutation == "cas_ambiguity":
        decision["source_cas"].append("7732-18-5")
    elif mutation == "formula":
        decision["source_formulas"] = ["Different"]
    elif mutation == "uuid":
        decision["target"]["uuid"] = "Wrong"
    elif mutation == "unit":
        decision["source"]["unit"] = "gram"
    elif mutation == "context":
        decision["source"]["categories"] = ["emissions to water", "groundwater, long-term"]
    elif mutation == "legacy_context":
        for row in decision["publisher_rows"]:
            row["Category v2.2"] = "water" if row["Category v2.2"] != "water" else "air"
    elif mutation == "empty_legacy":
        for row in decision["publisher_rows"]:
            row["Elementary Name v2.2"] = ""
    elif mutation == "publisher_ambiguity":
        extra = deepcopy(decision["publisher_rows"][0])
        extra["UUID Elementary Flow v3"] = "Different historic UUID"
        decision["publisher_rows"].append(extra)
    else:
        decision["conversion_factor"] = 2
    with pytest.raises(ValueError):
        validate_decision(decision, {identity(row): row for row in catalog}, {})


@pytest.mark.parametrize(
    "value,expected",
    [("71-36-3", True), ("7732-18-5", True), ("123-45-6", False), ("0-00-0", False), ("not-a-cas", False)],
)
def test_cas_numbers_require_a_non_placeholder_valid_check_digit(value, expected):
    assert valid_cas(value) is expected
