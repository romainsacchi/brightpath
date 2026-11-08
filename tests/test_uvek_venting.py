"""Mass-conserving venting services preserve direct emissions and avoided releases."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.catalogs import BiosphereCatalog, PackageCatalogProvider
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_venting import CHEMICALS, NATIVE_EMISSIONS, VENTING_SOURCES, venting_decision

LABELS = [
    label
    for label in sorted({p["label"] for a in VENTING_SOURCES.values() for p in a["profiles"]})
    if label.endswith("-cutoff")
]


def batch(label, amount):
    data = None
    for source, approval in VENTING_SOURCES.items():
        if label not in {p["label"] for p in approval["profiles"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    return data


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_venting_services_preserve_mass_chemical_identity_uncertainty_and_complete_provenance(
    label, target_kind, amount
):
    _, version, model = label.split("-")
    data = batch(label, amount)
    before = deepcopy(data)
    native = target_kind == "native2026"
    target = BackgroundContext(
        TechnosphereProfile("uvek", "2025" if target_kind == "2025" else "2026", "cutoff"),
        BiosphereProfile("uvek", "2026") if native else BiosphereProfile("ecoinvent", "3.10"),
    )
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN))
    assert len(result.data) == 4
    assert result.data[0]["exchanges"][0] == before[0]["exchanges"][0]
    helpers = {h["code"]: h for h in result.data[1:]}
    findings = [c for c in result.last_migration_report.changes if c.code == "migration.foreground_recipe"]
    assert len(findings) == 6
    for original, exchange, finding in zip(
        before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], findings, strict=True
    ):
        source = tuple(original[f] for f in FIELDS)
        gas = VENTING_SOURCES[source]["refrigerant"]
        helper = helpers[exchange["input"][1]]
        emission = helper["exchanges"][1]
        assert emission["name"] == (NATIVE_EMISSIONS[gas] if native else CHEMICALS[gas][1])
        assert emission["categories"] == (["emissions to air", "unspecified"] if native else CHEMICALS[gas][2])
        assert emission["amount"] == 1
        assert emission["type"] == "biosphere"
        assert helper["exchanges"][0]["amount"] == 1
        assert exchange["amount"] == -amount
        assert exchange["amount"] * emission["amount"] == -amount
        assert exchange["type"] == original["type"]
        assert exchange["caller_metadata"] == original["caller_metadata"]
        assert exchange["formula"] == "(dose) * (-1.0)"
        if original["uncertainty type"] == 2:
            assert exchange["negative"] is (amount >= 0)
            assert exchange["loc"] == original["loc"]
        else:
            assert exchange["loc"] == -amount
            assert exchange["minimum"] == -amount - 1
            assert exchange["maximum"] == -amount + 1
        assert exchange["scale"] == 0.2
        assert finding.to_dict()["before"] == original
        assert finding.to_dict()["details"]["source_exchange"] == original
        assert len(helper["exchanges"]) == (3 if version == "3.12" and not native else 2)
    omissions = [i for i in result.last_migration_report.issues if i.code == "migration.recipe_input_omission"]
    approximations = [
        i for i in result.last_migration_report.issues if i.code == "migration.recipe_biosphere_approximation"
    ]
    assert len(omissions) == (3 if native and version == "3.12" else 0)
    assert len(approximations) == (1 if native else 0)
    assert all(i.severity.value == "warning" for i in omissions + approximations)
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert inventory.data == data == before
    assert result.migrate_background(target).data == result.data


def test_historical_exact_venting_profiles_use_six_recipes_without_mutating_cached_resources():
    resource = historical_forward_resource()
    before = deepcopy(resource)
    rules = [r for r in resource["replace"] if r.get("mapping_method") == "reviewed_venting_recipe"]
    assert len(rules) == 5 and len([key for key in resource["recipes"] if key.startswith("venting-")]) == 6
    from scripts.review_uvek_refrigerant_patch_formulas import SOURCES as PATCH_SOURCES

    expected = {}
    patch_count = 0
    for source, approval in VENTING_SOURCES.items():
        labels = {p["label"] for p in approval["profiles"]}
        patches = {p["label"] for p in PATCH_SOURCES.get(source, {}).get("profiles", [])}
        assert not labels.intersection(patches)
        expected[source] = labels | patches
        patch_count += len(patches)
    assert sum(len(a["profiles"]) for a in VENTING_SOURCES.values()) == 42
    assert patch_count == 6
    assert sum(len(r["reference_flow_sign_review"]["profiles"]) for r in rules) == 48
    for rule in rules:
        source = tuple(rule["source"][f] for f in FIELDS)
        actual = {
            "-".join(p["source_profile"][f] for f in ("family", "version", "system_model"))
            for p in rule["reference_flow_sign_review"]["profiles"]
        }
        assert actual == expected[source]
    cutoff_labels = sorted({label for labels in expected.values() for label in labels if label.endswith("-cutoff")})
    for label in cutoff_labels:
        _, version, model = label.split("-")
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
        approved = [
            r
            for r in selected["replace"]
            if r.get("mapping_method") == "reviewed_venting_recipe"
            and r["reference_flow_sign_review"]["status"] == "verified"
        ]
        assert len(approved) == 3
        assert all(("waste-indicator" in r["target"]["recipe_ref"]) == (version == "3.12") for r in approved)
    assert resource == before


@pytest.mark.parametrize("label", ["ecoinvent-3.9-cutoff", "ecoinvent-3.12-cutoff"])
def test_venting_preserves_matching_foreground_suppliers_and_their_original_biosphere(label):
    _, version, model = label.split("-")
    data = batch(label, -1)
    for row in data[0]["exchanges"][1::2]:
        source = {f: row[f] for f in FIELDS}
        data.append({**source, "exchanges": [{**source, "type": "production", "amount": -1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model)).migrate_background(
        contexts("uvek", "2025").background, policy=MigrationPolicy.permissive()
    )
    assert result.data == data == before


def test_venting_unknown_patch_and_unrelated_errors_roll_back_the_entire_operation(unreviewed_uvek_correspondence):
    for version in ("3.9.1", "3.12"):
        data = batch("ecoinvent-3.12-cutoff", -1)
        if version == "3.12":
            data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "type": "technosphere", "amount": -1})
        before = deepcopy(data)
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
        assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
        assert inventory.data == data == before


@pytest.mark.parametrize(
    "change", ["missing_recipe", "pending_target", "wrong_unit", "duplicate_biosphere", "optional_emission"]
)
def test_invalid_profile_recipes_and_optional_omissions_cannot_load(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    rule = next(r for r in resource["replace"] if tuple(r["source"][field] for field in FIELDS) in VENTING_SOURCES)
    entry = rule["reference_flow_sign_review"]["profiles"][0]
    recipe = resource["recipes"][entry["target"]["recipe_ref"]]
    if change == "missing_recipe":
        entry["target"]["recipe_ref"] = "missing"
    elif change == "pending_target":
        entry["review"]["status"] = "unresolved"
        entry.pop("conversion_factor")
    elif change == "wrong_unit":
        recipe["unit"] = "megajoule"
    elif change == "duplicate_biosphere":
        recipe["inputs"][0]["biosphere_targets"] *= 2
    else:
        recipe["inputs"][0].update(
            approved_omission_if_unavailable=True, omission_rationale="Invalid emission omission"
        )
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)


def test_venting_authoring_requires_unchanged_exact_inventory_fingerprints():
    source, approval = next(iter(VENTING_SOURCES.items()))
    rule = {"id": "test", "source": dict(zip(FIELDS, source, strict=True)), "target": approval["previous_target"]}
    observations = [(p["label"], -1) for p in approval["profiles"]]
    fingerprints = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    assert venting_decision(rule, observations, fingerprints, "test", None)["factor"] == -1
    fingerprints[0] = (fingerprints[0][0], "changed")
    with pytest.raises(ValueError, match="inventory changed"):
        venting_decision(rule, observations, fingerprints, "test", None)


def test_missing_native_emission_binding_fails_even_under_permissive_policy():
    class MissingRefrigerantProvider(PackageCatalogProvider):
        def load_biosphere(self, profile):
            catalog = super().load_biosphere(profile)
            if profile == BiosphereProfile("uvek", "2026"):
                return BiosphereCatalog(profile, {i for i in catalog.identities if i[0] != NATIVE_EMISSIONS["r12"]})
            return catalog

    data = batch("ecoinvent-3.12-cutoff", -1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    target = BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target, policy=MigrationPolicy.permissive(), catalog_provider=MissingRefrigerantProvider()
        )
    assert any(i.code == "migration.recipe_transform_invalid" for i in caught.value.report.issues)
    assert inventory.data == data == before


def test_helper_code_collision_rolls_back_instead_of_reusing_an_unrelated_foreground_dataset():
    data = batch("ecoinvent-3.12-cutoff", -1)
    context = contexts("ecoinvent", "3.12")
    target = contexts("uvek", "2025").background
    converted = BrightwayInventory.from_data(data, context=context).migrate_background(
        target, policy=MigrationPolicy.permissive()
    )
    helper = converted.data[1]
    collider = {f: helper[f] for f in FIELDS}
    collider.update(name="Unrelated supplied foreground", code=helper["code"], exchanges=[])
    data.append(collider)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=context)
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target, policy=MigrationPolicy.permissive())
    assert any(i.code == "migration.recipe_transform_invalid" for i in caught.value.report.issues)
    assert inventory.data == data == original


@pytest.mark.parametrize("version", ["3.6", "3.9", "3.12"])
def test_helpers_are_in_the_declared_biosphere_independently_of_the_input_biosphere_version(version):
    data = batch(f"ecoinvent-{version}-cutoff", -1)
    source = BackgroundContext(
        TechnosphereProfile("ecoinvent", version, "cutoff"), BiosphereProfile("ecoinvent", version)
    )
    context = contexts("ecoinvent", version)
    inventory = BrightwayInventory.from_data(data, context=type(context)(context.format, source))
    result = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert len(result.data) == 4
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    again = inventory.migrate_background(contexts("uvek", "2025").background, policy=MigrationPolicy.permissive())
    assert again.data == result.data


@pytest.mark.parametrize("native", [False, True])
def test_generated_venting_helpers_export_and_round_trip_in_brightway_and_simapro(tmp_path, native):
    from brightpath import SimaProInventory

    version = "3.12" if native else "3.11"
    data = batch(f"ecoinvent-{version}-cutoff", -1)
    data[0]["comment"] = "Synthetic complete-venting export fixture."
    for exchange in data[0]["exchanges"][1:]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    target = BackgroundContext(
        TechnosphereProfile("uvek", "2026" if native else "2025", "cutoff"),
        BiosphereProfile("uvek", "2026") if native else BiosphereProfile("ecoinvent", "3.10"),
    )
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    )
    excel = migrated.write_excel(tmp_path / "venting.xlsx")
    reloaded = BrightwayInventory.from_excel(excel)
    assert len(reloaded.data) == 4
    assert not reloaded.validate().has_errors
    categorized = deepcopy(migrated.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    csv = (
        BrightwayInventory.from_data(categorized, context=migrated.context)
        .to_simapro()
        .write_csv(tmp_path / "venting.csv")
    )
    loaded_csv = SimaProInventory.from_csv(
        csv, background_profile=migrated.background_profile, biosphere_profile=target.biosphere
    ).to_brightway()
    assert len(loaded_csv.data) == 4
    assert not loaded_csv.validate().has_errors
    expected = sorted(e["name"] for h in migrated.data[1:] for e in h["exchanges"] if e["type"] == "biosphere")
    actual = sorted(e["name"] for h in loaded_csv.data[1:] for e in h["exchanges"] if e["type"] == "biosphere")
    assert actual == expected


def test_legacy_312_indicator_is_preserved_in_brightway_and_not_silently_dropped_by_simapro(tmp_path):
    from brightpath.exceptions import InventoryValidationError

    data = batch("ecoinvent-3.12-cutoff", -1)
    data[0]["comment"] = "Synthetic indicator preservation fixture."
    for exchange in data[0]["exchanges"][1:]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        contexts("uvek", "2025").background, policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
    )
    path = migrated.write_excel(tmp_path / "with-indicator.xlsx")
    restored = BrightwayInventory.from_excel(path)
    indicators = [
        e for h in restored.data[1:] for e in h["exchanges"] if e.get("name") == "Non-hazardous waste disposed"
    ]
    assert len(indicators) == 3 and all(e["amount"] == 1 for e in indicators)
    categorized = deepcopy(migrated.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    with pytest.raises(InventoryValidationError, match="No reviewed SimaPro representation for inventory indicator"):
        BrightwayInventory.from_data(categorized, context=migrated.context).to_simapro().write_csv(
            tmp_path / "unsupported-indicator.csv"
        )
    assert not (tmp_path / "unsupported-indicator.csv").exists()
