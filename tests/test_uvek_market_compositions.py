"""Market helpers inherit exact component bindings and preserve caller demands."""

from collections import defaultdict
from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer, contexts
from test_uvek_refrigerant_disposal import target_context

from brightpath import BrightwayInventory, TechnosphereProfile
from brightpath.background.reference_signs import select_profile_sign_rules
from brightpath.core.policies import MigrationPolicy
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts import review_uvek_market_compositions
from scripts.review_uvek_market_compositions import (
    APPROVAL,
    SUPERSEDED_DIGESTER_MARKETS,
    SUPERSEDED_STEEL_MARKETS,
    composition_decision,
    composition_recipe,
    validate_composition_bindings,
)
from scripts.review_uvek_reference_signs import prior_recipe_review

# Steel replacements are covered in test_uvek_steel_residue_markets; the
# allocated-sludge market replacements are covered in test_uvek_digester_stock.
COMPOSITIONS = {
    s: row
    for s, row in review_uvek_market_compositions.COMPOSITIONS.items()
    if s not in (SUPERSEDED_STEEL_MARKETS | SUPERSEDED_DIGESTER_MARKETS)
}

LABELS = [
    label
    for label in sorted({p["label"] for r in COMPOSITIONS.values() for p in r["profiles"]})
    if label.endswith("-cutoff")
]
POLICY = MigrationPolicy.permissive()


def batch(label, amount, *, definitions=COMPOSITIONS):
    data = None
    for source, row in definitions.items():
        if label not in {p["label"] for p in row["profiles"]}:
            continue
        for kind, uncertainty in (("technosphere", 2), ("substitution", 3)):
            inventory = consumer(dict(zip(FIELDS, source, strict=True)), amount, uncertainty, kind)
            inventory[0]["exchanges"][1]["caller_metadata"] = {"preserve": [1, 2]}
            if data is None:
                data = inventory
            else:
                data[0]["exchanges"].append(inventory[0]["exchanges"][1])
    return data


def assert_composition_quantities(
    label,
    amount,
    *,
    definitions=COMPOSITIONS,
    recipe_factory=composition_recipe,
    warning_fragment="component correspondences' heuristic",
):
    _, version, model = label.split("-")
    data = batch(label, amount, definitions=definitions)
    before = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", version, model))
    result = inventory.migrate_background(target_context("2025"), policy=POLICY)
    helpers = {tuple(d[f] for f in FIELDS): d for d in result.data[1:]}
    changes = {c.path: c for c in result.last_migration_report.changes if c.code == "migration.foreground_recipe"}
    assert len(changes) == len(before[0]["exchanges"]) - 1
    expected_warnings = {}
    for index, (old, new) in enumerate(
        zip(before[0]["exchanges"][1:], result.data[0]["exchanges"][1:], strict=True), 1
    ):
        source = tuple(old[f] for f in FIELDS)
        expected_warnings[f"datasets[0].exchanges[{index}]"] = definitions[source].get(
            "review_rationale", warning_fragment
        )
        profile = next(p for p in definitions[source]["profiles"] if p["label"] == label)
        recipe_id, recipe = recipe_factory(source, profile)
        helper = helpers[tuple(new[f] for f in FIELDS)]
        factor = profile["source_sign"]
        assert helper["brightpath_conversion"]["recipe_id"] == recipe_id
        assert helper["exchanges"][0]["type"] == "production" and helper["exchanges"][0]["amount"] == 1
        assert new["amount"] == factor * amount and new["type"] == old["type"]
        assert new["formula"] == ("dose" if factor == 1 else "(dose) * (-1.0)")
        assert new["caller_metadata"] == old["caller_metadata"] and new["scale"] == old["scale"]
        if old["uncertainty type"] == 2:
            assert new["loc"] == old["loc"] and new["negative"] is (old["negative"] if factor == 1 else amount >= 0)
        else:
            assert new["loc"] == factor * amount
            assert new["minimum"] == factor * amount - 1 and new["maximum"] == factor * amount + 1
        change = changes[f"datasets[0].exchanges[{index}]"].to_dict()
        assert change["before"] == old == change["details"]["source_exchange"]
        expected, observed = defaultdict(float), defaultdict(float)
        for e in recipe["inputs"]:
            key = ("recipe", e["recipe"]) if "recipe" in e else tuple(e[f] for f in FIELDS)
            expected[key] += e["amount"]
        for e in helper["exchanges"][1:]:
            child = helpers.get(tuple(e[f] for f in FIELDS))
            key = ("recipe", child["brightpath_conversion"]["recipe_id"]) if child else tuple(e[f] for f in FIELDS)
            observed[key] += e["amount"]
            assert e["type"] == "technosphere"
        assert dict(observed) == pytest.approx(dict(expected))
        assert recipe["inputs"] and not recipe.get("zero_burden")
    assert inventory.data == data == before
    assert result.migrate_background(target_context("2025")).data == result.data
    warnings = {
        i.path: i for i in result.last_migration_report.issues if i.code == "migration.technosphere_information_loss"
    }
    assert all(
        expected_warnings[path] in warnings[path].message
        or "explicitly approved cut-off allocation supplies" in warnings[path].message
        for path in changes
    )


@pytest.mark.parametrize("label", LABELS)
@pytest.mark.parametrize("amount", [-10, 0, 10])
def test_complete_market_compositions_preserve_profile_quantities_uncertainty_and_components(label, amount):
    assert_composition_quantities(label, amount)


@pytest.mark.parametrize("kind", ["2026", "native2026"])
def test_composed_markets_follow_exact_uvek_release_bridge(kind):
    result = BrightwayInventory.from_data(
        batch("ecoinvent-3.12-cutoff", -1), context=contexts("ecoinvent", "3.12", "cutoff")
    ).migrate_background(target_context(kind), policy=POLICY)
    assert result.background_profile.version == "2026"
    for exchange in result.data[0]["exchanges"][1:]:
        assert abs(exchange["amount"]) == 1
    unresolved = [i for i in result.last_migration_report.issues if i.code.endswith("link_unresolved")]
    if kind != "native2026":
        assert not unresolved
    else:
        # New textile markets retain independently documented physical native gaps.
        # Keep strict checks for every other flow and for all technosphere links.
        from scripts.review_uvek_uncontrolled_disposal import APPROVAL as DISPOSAL_APPROVAL
        from scripts.review_uvek_uncontrolled_disposal import REVIEW_PATH

        def key(row):
            return (row["name"], tuple(row["categories"]), row["unit"])

        missing = {key(row) for row in DISPOSAL_APPROVAL["native_unmapped_physical_definitions"]}
        resource = load_uvek_technosphere_resource()
        expected = set()
        approved_datasets = set()
        for di, dataset in enumerate(result.data):
            recipe_id = dataset.get("brightpath_conversion", {}).get("recipe_id")
            if not recipe_id:
                continue
            recipe = resource["recipes"][recipe_id]
            if not any(REVIEW_PATH in evidence for evidence in recipe.get("evidence", [])):
                continue
            for exchange in dataset["exchanges"]:
                if exchange["type"] == "biosphere" and key(exchange) in missing:
                    expected.add(key(exchange))
                    approved_datasets.add(di)
        assert expected
        assert all(i.code == "background.biosphere_link_unresolved" for i in unresolved)
        # Staged-helper and final-dataset validation report the same physical links.
        observed = {key(i.details["identity"]) for i in unresolved}
        assert observed == expected
        for issue in unresolved:
            if issue.path.startswith("datasets["):
                assert int(issue.path.split("[", 1)[1].split("]", 1)[0]) in approved_datasets
            else:
                assert issue.path.startswith("generated_helpers[")


def test_market_composition_strict_policy_rollback_foreground_protection_and_unknown_profile():
    data = batch("ecoinvent-3.12-cutoff", -1)
    original = deepcopy(data)
    inventory = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff"))
    with pytest.raises(MigrationError):
        inventory.migrate_background(target_context("2025"))
    assert inventory.data == data == original
    for source, row in COMPOSITIONS.items():
        if "ecoinvent-3.12-cutoff" in {p["label"] for p in row["profiles"]}:
            supplier = dict(zip(FIELDS, source, strict=True))
            data.append({**supplier, "exchanges": [{**supplier, "type": "production", "amount": -1}]})
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.12", "cutoff")).migrate_background(
        target_context("2025"), policy=POLICY
    )
    assert result.data == data
    selected = select_profile_sign_rules(
        load_uvek_technosphere_resource(), TechnosphereProfile("ecoinvent", "3.12.1", "cutoff")
    )
    assert all(
        r["reference_flow_sign_review"]["status"] == "unresolved"
        for r in selected["replace"]
        if tuple(r["source"][f] for f in FIELDS) in COMPOSITIONS
    )


@pytest.mark.parametrize("change", ["profile", "hash", "prior_target"])
def test_changed_market_inventories_cannot_inherit_composition_approval(change):
    source, approval = next(iter(COMPOSITIONS.items()))
    rule = {"source": approval["source"], "target": deepcopy(approval["previous_target"])}
    observations = [(p["label"], p["source_sign"]) for p in approval["profiles"]]
    inventories = [(p["label"], p["inventory_sha256"]) for p in approval["profiles"]]
    if change == "profile":
        observations[0] = ("ecoinvent-3.12.1-cutoff", 1)
    elif change == "hash":
        inventories[0] = (inventories[0][0], "0" * 64)
    else:
        rule["target"]["name"] = "Unreviewed"
    with pytest.raises(ValueError, match="repeat the scientific review"):
        composition_decision(rule, observations, inventories, "synthetic", None)


@pytest.mark.parametrize("change", ["component", "recipe"])
def test_changed_components_require_a_new_composition_review(change):
    resource = deepcopy(load_uvek_technosphere_resource())
    if change == "component":
        provider = tuple(APPROVAL["rules"][0]["profiles"][0]["bindings"][0]["source"])
        rule = next(r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) == provider)
        rule["target"]["name"] = "Unreviewed"
    else:
        key = next(k for k in APPROVAL["recipe_dependencies"] if resource["recipes"][k]["inputs"])
        resource["recipes"][key]["inputs"][0]["amount"] = 999
    with pytest.raises(ValueError, match="repeat the scientific review"):
        validate_composition_bindings(resource)


def test_replay_uses_retained_original_review_after_unused_runtime_review_is_pruned():
    original = {"rationale": "Original review retained only in authoring evidence."}
    assert prior_recipe_review({"reviews": {}}, {"review_ref": "pruned"}, {"original_review": original}) == original
