"""Approved disposal proxies retain incineration demand, direct leaks and provenance."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from uvek_review_helpers import historical_forward_resource

from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, TechnosphereProfile
from brightpath.background.catalogs import PackageCatalogProvider, TechnosphereCatalog
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_release_resources
from scripts.review_uvek_refrigerant_disposal import DISPOSAL_SOURCES, DISPOSAL_TARGET, disposal_decision
from scripts.review_uvek_venting import CHEMICALS, NATIVE_EMISSIONS

LABELS = [
    label
    for label in sorted({p["label"] for a in DISPOSAL_SOURCES.values() for p in a["profiles"]})
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.WARN)


def batch(label, amount):
    data = None
    for source, approval in DISPOSAL_SOURCES.items():
        if label not in {p["label"] for p in approval["profiles"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            row = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            row[0]["exchanges"][1]["caller_metadata"] = {"retain": [1, 2]}
            if data is None:
                data = row
            else:
                data[0]["exchanges"].append(row[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "amount": 0.5,
        }
    )
    return data


def target_context(kind):
    return BackgroundContext(
        TechnosphereProfile("uvek", "2025" if kind == "2025" else "2026", "cutoff"),
        BiosphereProfile("uvek", "2026") if kind == "native2026" else BiosphereProfile("ecoinvent", "3.10"),
    )


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("target_kind", ["2025", "2026", "native2026"])
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_disposal_preserves_mass_split_chemical_uncertainty_foreground_emissions_and_original_exchange(
    label, target_kind, amount
):
    _, version, model = label.split("-")
    data = batch(label, amount)
    original = deepcopy(data)
    native = target_kind == "native2026"
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    target = target_context(target_kind)
    result = inventory.migrate_background(target, policy=POLICY)
    assert len(result.data) == 4
    assert result.data[0]["exchanges"][0] == original[0]["exchanges"][0]
    assert result.data[0]["exchanges"][-1]["name"] == "Carbon dioxide, fossil"
    assert result.data[0]["exchanges"][-1]["amount"] == 0.5
    helpers = {h["code"]: h for h in result.data[1:]}
    findings = [c for c in result.last_migration_report.changes if c.code == "migration.foreground_recipe"]
    proxies = [i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"]
    losses = [
        loss for loss in result.last_migration_report.losses if loss.code == "migration.technosphere_information_loss"
    ]
    assert len(findings) == len(proxies) == len(losses) == 6
    if target_kind == "2025":
        expected_supplier = DISPOSAL_TARGET
    else:
        release = next(
            r for r in load_uvek_release_resources("technosphere") if r["source_profiles"][0]["version"] == "2025"
        )
        expected_supplier = next(r["target"] for r in release["replace"] if r["source"] == DISPOSAL_TARGET)
    for before, after, finding in zip(
        original[0]["exchanges"][1:-1], result.data[0]["exchanges"][1:-1], findings, strict=True
    ):
        gas = DISPOSAL_SOURCES[tuple(before[f] for f in FIELDS)]["refrigerant"]
        helper = helpers[after["input"][1]]
        release = next(
            e
            for e in helper["exchanges"]
            if e.get("type") == "biosphere" and e.get("name") != "Non-hazardous waste disposed"
        )
        incineration = next(e for e in helper["exchanges"] if e.get("type") == "technosphere")
        assert release["name"] == (NATIVE_EMISSIONS[gas] if native else CHEMICALS[gas][1])
        assert release["categories"] == (["emissions to air", "unspecified"] if native else CHEMICALS[gas][2])
        assert release["amount"] == 0.001
        assert incineration["amount"] == 0.999
        assert {f: incineration[f] for f in FIELDS} == {f: expected_supplier[f] for f in FIELDS}
        assert release["amount"] + incineration["amount"] == 1
        assert after["amount"] == -amount
        assert after["amount"] * release["amount"] == pytest.approx(-amount * 0.001)
        assert after["amount"] * incineration["amount"] == pytest.approx(-amount * 0.999)
        assert after["type"] == before["type"] and after["caller_metadata"] == before["caller_metadata"]
        assert after["formula"] == "(dose) * (-1.0)"
        if before["uncertainty type"] == 2:
            assert after["loc"] == before["loc"] and after["negative"] is (amount >= 0)
        else:
            assert after["loc"] == -amount
            assert after["minimum"] == -amount - 1 and after["maximum"] == -amount + 1
        assert after["scale"] == 0.2
        assert finding.to_dict()["before"] == before
        assert finding.to_dict()["details"]["source_exchange"] == before
        assert len(helper["exchanges"]) == (4 if version == "3.12" and not native else 3)
    assert all(
        i.severity.value == "warning" and "CH average hazardous-waste incineration" in i.message for i in proxies
    )
    assert {loss.path: loss.to_dict()["details"]["source_exchange"] for loss in losses} == {
        f"datasets[0].exchanges[{i}]": e for i, e in enumerate(original[0]["exchanges"][1:-1], 1)
    }
    assert len([i for i in result.last_migration_report.issues if i.code == "migration.recipe_input_omission"]) == (
        3 if native and version == "3.12" else 0
    )
    assert len(
        [i for i in result.last_migration_report.issues if i.code == "migration.recipe_biosphere_approximation"]
    ) == (1 if native else 0)
    assert not any(i.code.endswith("link_unresolved") for i in result.last_migration_report.issues)
    assert inventory.data == data == original
    assert result.migrate_background(target).data == result.data


def test_disposal_proxy_requires_loss_acceptance_and_warns_even_when_information_loss_is_allowed():
    data = batch("ecoinvent-3.12-cutoff", -1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(
            target_context("2025"), policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN)
        )
    assert any(
        i.code == "migration.technosphere_information_loss" and i.severity.value == "error"
        for i in caught.value.report.issues
    )
    result = inventory.migrate_background(
        target_context("2025"),
        policy=MigrationPolicy(on_heuristic_mapping=PolicyAction.WARN, on_information_loss=PolicyAction.ALLOW),
    )
    assert all(
        i.severity.value == "warning"
        for i in result.last_migration_report.issues
        if i.code == "migration.technosphere_information_loss"
    )
    assert inventory.data == data == before


def test_historical_disposal_keeps_42_base_profiles_and_six_reviewed_patch_extensions():
    resource = historical_forward_resource()
    before = deepcopy(resource)
    rules = [r for r in resource["replace"] if r.get("mapping_method") == "reviewed_refrigerant_disposal_recipe"]
    from scripts.review_uvek_refrigerant_patch_formulas import SOURCES as PATCH_SOURCES

    assert len(rules) == 5
    expected = {}
    patch_count = 0
    for source, approval in DISPOSAL_SOURCES.items():
        labels = {p["label"] for p in approval["profiles"]}
        patches = {p["label"] for p in PATCH_SOURCES.get(source, {}).get("profiles", [])}
        assert not labels.intersection(patches)
        expected[source] = labels | patches
        patch_count += len(patches)
    assert sum(len(a["profiles"]) for a in DISPOSAL_SOURCES.values()) == 42
    assert patch_count == 6
    assert sum(len(r["reference_flow_sign_review"]["profiles"]) for r in rules) == 48
    for rule in rules:
        source = tuple(rule["source"][f] for f in FIELDS)
        actual = {
            "-".join(p["source_profile"][f] for f in ("family", "version", "system_model"))
            for p in rule["reference_flow_sign_review"]["profiles"]
        }
        assert actual == expected[source]
    assert len([k for k in resource["recipes"] if k.startswith("disposal-")]) == 6
    cutoff_labels = sorted({label for labels in expected.values() for label in labels if label.endswith("-cutoff")})
    for label in cutoff_labels:
        _, version, model = label.split("-")
        selected = select_profile_sign_rules(resource, TechnosphereProfile("ecoinvent", version, model))
        approved = [
            r
            for r in selected["replace"]
            if r.get("mapping_method") == "reviewed_refrigerant_disposal_recipe"
            and r["reference_flow_sign_review"]["status"] == "verified"
        ]
        assert len(approved) == 3 and all(r["conversion_factor"] == -1 for r in approved)
        assert all(("waste-indicator" in r["target"]["recipe_ref"]) == (version == "3.12") for r in approved)
        assert all(r["classification"] == "proxy" for r in approved)
    assert resource == before


def test_disposal_preserves_foreground_suppliers_and_rolls_back_unreviewed_profiles_or_unrelated_errors(
    unreviewed_uvek_correspondence,
):
    data = batch("ecoinvent-3.12-cutoff", -1)
    for e in data[0]["exchanges"][1:-1:2]:
        supplier = {f: e[f] for f in FIELDS}
        data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data == before
    for version in ("3.9.1", "3.12"):
        data = batch("ecoinvent-3.12-cutoff", -1)
        if version == "3.12":
            data[0]["exchanges"].append({**unreviewed_uvek_correspondence, "type": "technosphere", "amount": -1})
        before = deepcopy(data)
        inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version))
        with pytest.raises(MigrationError) as caught:
            inventory.migrate_background(target_context("2025"), policy=MigrationPolicy.permissive())
        assert any(i.code == "migration.reference_flow_sign_unresolved" for i in caught.value.report.issues)
        assert inventory.data == data == before


def test_disposal_authoring_rejects_changed_profile_hashes_signs_and_prior_factors():
    source, approval = next(iter(DISPOSAL_SOURCES.items()))
    rule = {"id": "test", "source": dict(zip(FIELDS, source, strict=True)), "target": approval["previous_target"]}
    observations = [(p["label"], -1) for p in approval["profiles"]]
    fingerprints = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    assert disposal_decision(rule, observations, fingerprints, "test", None, {})["factor"] == -1
    for obs, hashes, prior in [
        (observations, [], {}),
        ([], fingerprints, {}),
        (observations, fingerprints, {"original_factor": 2}),
    ]:
        with pytest.raises(ValueError):
            disposal_decision(rule, obs, hashes, "test", None, prior)


def test_missing_hazardous_incineration_target_rolls_back_generated_helpers():
    class MissingIncinerator(PackageCatalogProvider):
        def load_technosphere(self, profile):
            catalog = super().load_technosphere(profile)
            if profile == TechnosphereProfile("uvek", "2025", "cutoff"):
                return TechnosphereCatalog(profile, catalog.identities - {tuple(DISPOSAL_TARGET[f] for f in FIELDS)})
            return catalog

    data = batch("ecoinvent-3.12-cutoff", -1)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12"))
    with pytest.raises(MigrationError) as caught:
        inventory.migrate_background(target_context("2025"), policy=POLICY, catalog_provider=MissingIncinerator())
    assert any(i.code == "background.technosphere_link_unresolved" for i in caught.value.report.issues)
    assert inventory.data == data == before


@pytest.mark.parametrize("native", [False, True])
def test_disposal_helpers_and_incineration_inputs_round_trip_in_brightway_and_simapro(tmp_path, native):
    from brightpath import SimaProInventory

    version = "3.12" if native else "3.11"
    data = batch(f"ecoinvent-{version}-cutoff", -1)
    data[0]["comment"] = "Synthetic disposal-service export fixture."
    for exchange in data[0]["exchanges"][1:-1]:
        exchange.pop("formula")
        exchange["type"] = "technosphere"
    migrated = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version)).migrate_background(
        target_context("native2026" if native else "2025"), policy=POLICY
    )
    xlsx = migrated.write_excel(tmp_path / "disposal.xlsx")
    restored = BrightwayInventory.from_excel(xlsx)
    assert len(restored.data) == 4 and not restored.validate().has_errors
    categorized = deepcopy(migrated.data)
    for dataset in categorized:
        dataset["exchanges"][0]["simapro category"] = "Materials/Services"
    csv = (
        BrightwayInventory.from_data(categorized, context=migrated.context)
        .to_simapro()
        .write_csv(tmp_path / "disposal.csv")
    )
    loaded = SimaProInventory.from_csv(
        csv, background_profile=migrated.background_profile, biosphere_profile=migrated.context.background.biosphere
    ).to_brightway()
    assert len(loaded.data) == 4 and not loaded.validate().has_errors
    for helper in loaded.data[1:]:
        incineration = [e for e in helper["exchanges"] if e["type"] == "technosphere"]
        assert len(incineration) == 1 and incineration[0]["amount"] == 0.999
        assert any(e["type"] == "biosphere" and e["amount"] == 0.001 for e in helper["exchanges"])
