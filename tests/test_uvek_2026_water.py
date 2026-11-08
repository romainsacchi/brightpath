"""Unspecified water discharges retain volume/mass coordinates in both directions."""

import json
import math
from copy import deepcopy

import pytest

from brightpath import BackgroundContext, BiosphereProfile, InventoryContext, TechnosphereProfile
from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_2026_ash_water import before_ash_water_review
from scripts.review_uvek_2026_biosphere import identity
from scripts.review_uvek_2026_water import METHOD, REPORT, SOURCE, TARGET, apply_water_review, validate_decision
from tests.test_uvek_2026_biosphere_review import CATALOGS, context, document
from tests.uvek_biosphere_history import before_water_review


def test_one_authored_rule_reproduces_resource_and_preserves_independently_reviewed_reverse():
    prior = before_water_review()
    original = deepcopy(prior)
    current = before_ash_water_review(list(load_uvek_release_resources("biosphere")))
    assert apply_water_review(prior, REPORT, CATALOGS) == current
    assert prior == original
    for family in ("ecoinvent", "uvek"):
        before = next(r for r in prior if r["source_profiles"][0]["family"] == family)
        after = next(r for r in current if r["source_profiles"][0]["family"] == family)
        if family == "uvek":
            assert after == before
            continue
        assert len(after["replace"]) == 1298
        assert len(after["unresolved"]) == 3064
        assert [r for r in after["replace"] if r["mapping_method"] != METHOD] == before["replace"]
        assert len([r for r in after["replace"] if r["mapping_method"] == METHOD]) == 1
        assert all(after[k] == before[k] for k in ("recipes", "delete", "disaggregate"))


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("version", ["3.10", "3.12"])
@pytest.mark.parametrize("uncertainty", [0, 2, 3, 4])
@pytest.mark.parametrize("amount", [-12, 0, 12])
def test_water_units_signs_uncertainty_formulas_and_owned_data_scale_once(reverse, version, uncertainty, amount):
    source, target, factor = (TARGET, SOURCE, 0.001) if reverse else (SOURCE, TARGET, 1000.0)
    biosphere_source = ("uvek", "2026") if reverse else ("ecoinvent", version)
    biosphere_target = ("ecoinvent", version) if reverse else ("uvek", "2026")
    original = document([{"source": source}])
    data = original.data
    data[0]["parameters"] = [{"name": "dose", "amount": amount}]
    flow = data[0]["exchanges"][1]
    flow.update(amount=amount, formula="dose", minimum=-15, maximum=15, **{"uncertainty type": uncertainty})
    if uncertainty == 2:
        flow.update(loc=math.log(12), scale=0.3, negative=amount < 0)
    else:
        flow["loc"] = amount
    original = original.replace(data=data, context=InventoryContext(original.context.format, context(biosphere_source)))
    saved = deepcopy(original.data)
    # Ecoinvent 3.12 first traverses the existing inferred reverse release
    # bridges to 3.10; accepting this water rule does not authorize those bridges.
    policy = MigrationPolicy(
        on_inferred_reverse=PolicyAction.WARN if version == "3.12" and not reverse else PolicyAction.ERROR
    )
    result = execute_background_migration(original, context(biosphere_target), PackageCatalogProvider(), policy)
    assert result.succeeded, result.report.to_json()
    after = result.value.data[0]["exchanges"][1]
    assert identity(after) == identity(target)
    assert after["amount"] == pytest.approx(amount * factor)
    assert after["loc"] == pytest.approx(math.log(12) + math.log(factor) if uncertainty == 2 else amount * factor)
    assert after["scale"] == pytest.approx(0.3 * factor if uncertainty == 3 else 0.3)
    assert (after["minimum"], after["maximum"]) == pytest.approx((-15 * factor, 15 * factor))
    assert after["formula"] == f"(dose) * ({factor!r})"
    assert after["custom"] == [1]
    if uncertainty == 2:
        assert after["negative"] is (amount < 0)
    assert original.data == saved
    repeated = execute_background_migration(result.value, context(biosphere_target), PackageCatalogProvider())
    assert repeated.succeeded and repeated.value.data == result.value.data


@pytest.mark.parametrize("release", ["2025", "2026", "native2026"])
def test_generated_helper_keeps_both_water_emissions_and_scales_native_mass(monkeypatch, release):
    provider = PackageCatalogProvider()
    source_profile = TechnosphereProfile("ecoinvent", "3.10", "cutoff")
    resource = deepcopy(load_uvek_technosphere_resource())
    selected = select_profile_sign_rules(resource, source_profile)
    catalog_ids = provider.load_technosphere(source_profile).identities
    fields = ("name", "reference product", "location", "unit")
    rule = next(
        r
        for r in selected["replace"]
        if "recipe_ref" in r["target"]
        and r["reference_flow_sign_review"]["status"] == "verified"
        and tuple(r["source"][f] for f in fields) in catalog_ids
        and not resource["recipes"][r["target"]["recipe_ref"]].get("zero_burden")
    )
    # Test-only recipe isolates helper biosphere execution without redistributing
    # any licensed process coefficients or authorizing a production helper.
    recipe = resource["recipes"][rule["target"]["recipe_ref"]]
    recipe["inputs"] = [
        {"name": "Water", "categories": [compartment], "unit": "cubic meter", "amount": value, "type": "biosphere"}
        for compartment, value in (("air", 0.25), ("water", 0.75))
    ]
    monkeypatch.setattr("brightpath.background.uvek_import.load_uvek_technosphere_resource", lambda: resource)
    original = document([])
    data = original.data
    data[0]["exchanges"].append({**rule["source"], "type": "technosphere", "amount": -2})
    original = original.replace(
        data=data,
        context=InventoryContext(
            original.context.format, BackgroundContext(source_profile, BiosphereProfile("ecoinvent", "3.10"))
        ),
    )
    saved = deepcopy(original.data)
    native = release == "native2026"
    target = BackgroundContext(
        TechnosphereProfile("uvek", "2025" if release == "2025" else "2026", "cutoff"),
        BiosphereProfile("uvek", "2026") if native else BiosphereProfile("ecoinvent", "3.10"),
    )
    result = execute_background_migration(
        original,
        target,
        provider,
        MigrationPolicy(on_information_loss=PolicyAction.WARN, on_heuristic_mapping=PolicyAction.WARN),
    )
    assert result.succeeded, result.report.to_json()
    flows = [e for d in result.value.data[1:] for e in d["exchanges"] if e["type"] == "biosphere"]
    assert len(flows) == 2
    assert [e["amount"] for e in flows] == pytest.approx([250, 750] if native else [0.25, 0.75])
    assert [e["categories"] for e in flows] == (
        [["emissions to air", "unspecified"], ["emissions to water", "unspecified"]] if native else [["air"], ["water"]]
    )
    assert all(e["unit"] == ("kilogram" if native else "cubic meter") for e in flows)
    assert not any("omission" in loss.code for loss in result.report.losses)
    assert original.data == saved


@pytest.mark.parametrize(
    "mutation", ["source_compartment", "target_compartment", "source_unit", "cas", "uuid", "factor", "density", "loss"]
)
def test_water_review_rejects_substance_compartment_and_quantity_drift(mutation):
    decision = deepcopy(json.loads(REPORT.read_text())["decision"])
    if mutation == "source_compartment":
        decision["source"]["categories"] = ["water", "surface water"]
    elif mutation == "target_compartment":
        decision["target"]["categories"] = ["emissions to water", "river"]
    elif mutation == "source_unit":
        decision["source"]["unit"] = "kilogram"
    elif mutation == "cas":
        decision["source_cas"] = ["wrong"]
    elif mutation == "uuid":
        decision["source_uuid"] = "wrong"
    elif mutation == "factor":
        decision["conversion_factor"] = 1
    elif mutation == "density":
        decision["density_kg_per_m3"] = 1
    else:
        decision["biosphere_information_loss"] = "Not approved."
    catalogs = [
        json.loads((CATALOGS / name).read_text())["biosphere"]
        for name in ("ecoinvent__3.10__cutoff.json", "uvek__2026__cutoff.json")
    ]
    with pytest.raises(ValueError, match="water correspondence"):
        validate_decision(decision, *catalogs)


def test_reapplying_water_review_rejects_changed_directional_baseline():
    with pytest.raises(ValueError, match="directional baseline"):
        apply_water_review(list(load_uvek_release_resources("biosphere")), REPORT, CATALOGS)


def test_new_water_rule_does_not_authorize_inferred_reverse_version_bridges():
    original = document([{"source": SOURCE}])
    original = original.replace(context=InventoryContext(original.context.format, context(("ecoinvent", "3.12"))))
    saved = deepcopy(original.data)
    result = execute_background_migration(original, context(("uvek", "2026")), PackageCatalogProvider())
    assert not result.succeeded
    assert any(i.code == "migration.inferred_reverse" for i in result.report.issues)
    assert original.data == result.value.data == saved
