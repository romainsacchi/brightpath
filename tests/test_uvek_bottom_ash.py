"""Bottom-ash services retain their own leachate and separate operating/facility demand."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, InventoryContext, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from scripts.review_uvek_bottom_ash import APPROVAL, SOURCES, validate_bindings, validate_inventories

POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)
REVERSE_POLICY = MigrationPolicy(
    on_heuristic_mapping=PolicyAction.WARN,
    on_information_loss=PolicyAction.WARN,
    on_inferred_reverse=PolicyAction.WARN,
)
VERSIONS = ("3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")


def source_rows(version):
    label = f"ecoinvent-{version}-cutoff"
    return {source: row for source, row in SOURCES.items() if any(p["label"] == label for p in row["profiles"])}


def batch(version, amount):
    data = None
    for source in source_rows(version):
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            part = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            part[0]["exchanges"][1]["caller_metadata"] = {"keep": [1, 2]}
            if data is None:
                data = part
            else:
                data[0]["exchanges"].append(part[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    return data


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-10, 0, 10))
def test_every_reviewed_waste_profile_keeps_its_releases_and_correct_one_kg_service_coordinate(
    version, release, amount
):
    data = batch(version, amount)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
    result = inventory.migrate_background(target_context(release), policy=POLICY)
    helpers = {d["code"]: d for d in result.data[1:]}
    assert len(result.data) == 1 + len(source_rows(version))
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    native = release == "native2026"
    indicator_count = 0
    for before, after in zip(original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], strict=True):
        assert after["amount"] == -before["amount"] and after["type"] == before["type"]
        assert after["formula"] == "(dose) * (-1.0)" and after["caller_metadata"] == before["caller_metadata"]
        assert after["scale"] == before["scale"]
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is not before["negative"]
        else:
            assert after["loc"] == -before["loc"]
            assert (after["minimum"], after["maximum"]) == (-before["maximum"], -before["minimum"])
        helper = helpers[after["input"][1]]
        tech = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        bio = [e for e in helper["exchanges"] if e["type"] == "biosphere"]
        assert sum(e["amount"] for e in tech if e["unit"] == "kilogram") == pytest.approx(1)
        assert sum(e["amount"] for e in tech if e["unit"] == "unit") == pytest.approx(1.7778e-9)
        assert all("slag" in e["name"].lower() for e in tech)
        assert not any("incineration" in e["name"].lower() or "sanitary landfill" in e["name"].lower() for e in tech)
        profile = next(
            p
            for p in SOURCES[tuple(before[f] for f in FIELDS)]["profiles"]
            if p["label"] == f"ecoinvent-{version}-cutoff"
        )
        expected_bio = [e for e in profile["inputs"] if e["type"] == "biosphere"]
        indicators = [e for e in expected_bio if e["categories"][0] == "inventory indicator"]
        if native:
            expected_bio = [e for e in expected_bio if e["categories"][0] != "inventory indicator"]
        assert len(bio) == len(expected_bio)
        assert sorted(e["amount"] for e in bio) == pytest.approx(sorted(e["amount"] for e in expected_bio))
        # A helper is reused by its two callers; omission losses belong to
        # generated emissions once, not to each repeated treatment reference.
        if before["type"] == "technosphere":
            indicator_count += len(indicators)
        if before["reference product"] == "bottom ash, MSWI, waste polyethylene":
            name = "Zinc" if native else "Zinc II"
            ground = [e for e in bio if e["name"] == name and "long-term" in "/".join(e["categories"])]
            surface = [e for e in bio if e["name"] == name and "long-term" not in "/".join(e["categories"])]
            assert len(ground) == len(surface) == 1
            assert ground[0]["amount"] == 0.0046932 and surface[0]["amount"] == 1.3971e-7
            assert ground[0]["categories"] == (
                ["emissions to water", "groundwater, long-term"] if native else ["water", "ground-, long-term"]
            )
            assert surface[0]["categories"] == (
                ["emissions to water", "unspecified"] if native else ["water", "surface water"]
            )
    omissions = [loss for loss in result.last_migration_report.losses if loss.code == "migration.recipe_input_omission"]
    assert len(omissions) == (indicator_count if native else 0)
    assert all(loss.details["source_exchange"]["categories"][0] == "inventory indicator" for loss in omissions)
    assert inventory.data == data == original
    repeated = result.migrate_background(target_context(release), policy=POLICY)
    assert repeated.data == result.data


@pytest.mark.parametrize("release", ("2025", "2026"))
def test_complete_canonical_biosphere_helper_graph_has_an_independently_supported_reverse(release):
    data = batch("3.12", -1)
    forward = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context(release), policy=POLICY
    )
    saved = deepcopy(forward.data)
    reverse = forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=REVERSE_POLICY)
    assert len(reverse.data) == len(forward.data)
    assert [e["amount"] for e in reverse.data[0]["exchanges"]] == [e["amount"] for e in saved[0]["exchanges"]]
    for helper in reverse.data[1:]:
        tech = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert sum(e["amount"] for e in tech if e["unit"] == "kilogram") == pytest.approx(1)
        assert sum(e["amount"] for e in tech if e["unit"] == "unit") == pytest.approx(1.7778e-9)
        assert {e["name"] for e in tech} == {"process-specific burdens, slag landfill", "slag landfill construction"}
    assert forward.data == saved


def test_native_generated_graph_does_not_infer_the_28_missing_generic_water_reverse_correspondences():
    data = batch("3.12", -1)
    forward = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("native2026"), policy=POLICY
    )
    saved = deepcopy(forward.data)
    with pytest.raises(MigrationError) as caught:
        forward.migrate_background(contexts("ecoinvent", "3.12").background, policy=REVERSE_POLICY)
    assert any("unresolved" in issue.code for issue in caught.value.report.issues)
    assert forward.data == saved


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
@pytest.mark.parametrize("amount", (-2, 0, 2))
def test_independent_native_operating_and_facility_suppliers_retain_positive_reference_coordinates(release, amount):
    data = None
    for name, unit in (("Process-specific burdens, slag compartment", "kilogram"), ("Slag compartment", "unit")):
        source = dict(zip(FIELDS, (name, name, "CH", unit), strict=True))
        if release != "2025":
            bridge = next(
                r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
            )
            source = next(r["target"] for r in bridge["replace"] if r["source"] == source)
        part = consumer(source, amount, 3)
        if data is None:
            data = part
        else:
            data[0]["exchanges"].append(part[0]["exchanges"][1])
    original = deepcopy(data)
    context = target_context(release)
    inventory = BrightwayInventory.from_data(
        data, context=InventoryContext(contexts("uvek", context.technosphere.version).format, context)
    )
    result = inventory.migrate_background(contexts("ecoinvent", "3.12").background, policy=REVERSE_POLICY)
    assert len(result.data) == 1
    assert [e["amount"] for e in result.data[0]["exchanges"][1:]] == [amount, amount]
    assert {e["name"] for e in result.data[0]["exchanges"][1:]} == {
        "process-specific burdens, slag landfill",
        "slag landfill construction",
    }
    assert inventory.data == data == original


@pytest.mark.parametrize("release", ("2025", "2026", "native2026"))
def test_bottom_ash_does_not_enable_consequential_routes(release):
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "consequential"))
    original = deepcopy(inventory.data)
    with pytest.raises(MigrationError, match="system models"):
        inventory.migrate_background(target_context(release), policy=MigrationPolicy.permissive())
    assert inventory.data == original


def test_helpers_require_loss_acceptance_and_do_not_replace_owned_foreground_suppliers():
    data = batch("3.12", -1)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    original = deepcopy(data)
    with pytest.raises(MigrationError):
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert inventory.data == original
    for source in source_rows("3.12"):
        row = dict(zip(FIELDS, source, strict=True))
        data.append({**row, "exchanges": [{**row, "type": "production", "amount": 1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data


@pytest.mark.parametrize("version", ("3.8", "3.13"))
def test_uninspected_profiles_are_not_approved_by_release_or_name_similarity(version):
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", version, "cutoff")
    )
    rows = [r for r in selected["replace"] if tuple(r["source"][f] for f in FIELDS) in SOURCES]
    assert len(rows) == 49 and all(r["reference_flow_sign_review"]["status"] == "unresolved" for r in rows)


def observed_evidence():
    inventories, observations = {}, {}
    for row in APPROVAL["inventories"]:
        source = tuple(row["source"][f] for f in FIELDS)
        inventories.setdefault(source, []).append({**row, "source": list(source)})
        observations.setdefault(source, set()).add((row["label"], -1))
    return inventories, observations


@pytest.mark.parametrize(
    "change", ("inventory", "comment", "product_information", "missing", "duplicate", "sign", "profile")
)
def test_changed_treatment_inventory_or_exact_profile_requires_new_scientific_review(change):
    inventories, observations = observed_evidence()
    validate_inventories(inventories, observations)
    source = next(iter(SOURCES))
    if change in ("inventory", "comment", "product_information"):
        field = {
            "inventory": "inventory_sha256",
            "comment": "activity_comment_sha256",
            "product_information": "product_information_sha256",
        }[change]
        inventories[source][0][field] = "0" * 64
    elif change == "missing":
        inventories[source].pop()
    elif change == "duplicate":
        inventories[source].append(deepcopy(inventories[source][0]))
    elif change == "sign":
        observations[source] = {(label, 1) for label, sign in observations[source]}
    else:
        observations[source].add(("ecoinvent-3.13-cutoff", -1))
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_inventories(inventories, observations)


def test_changed_operating_component_cannot_inherit_complete_ash_helper_approval():
    resource = deepcopy(load_uvek_technosphere_resource())
    validate_bindings(resource)
    component = next(r for r in resource["replace"] if r["source"]["name"] == "market for slag landfill")
    component["target"]["name"] = "Sanitary landfill facility"
    with pytest.raises(ValueError, match="component binding changed"):
        validate_bindings(resource)
