"""Historical authoring evidence for tests; never used as an executable route."""

from copy import deepcopy
from functools import lru_cache
from pathlib import Path

from scripts.generate_uvek_migration_resources import _build_historical_review_resources


@lru_cache(maxsize=1)
def _historical_forward():
    root = Path(__file__).resolve().parents[1]
    resource, _ = _build_historical_review_resources(
        root / "brightpath/data/export/reference_catalogs",
        root / "brightpath/data/export/ecoinvent_to_uvek_mapping.csv",
        root / "brightpath/data/export/uvek_conversion_factors.yaml",
    )
    return resource


def historical_forward_resource():
    """Return independent archived-review inputs without restoring runtime support."""
    return deepcopy(_historical_forward())


def before_plastic_processing_39_review(resource):
    """Restore frozen parents for testing the earlier 3.12 authoring stage."""
    import json

    from brightpath import DATA_DIR

    review = json.loads((DATA_DIR / "export/uvek_plastic_processing_39_review.json").read_text())
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        if parent not in (row["original_rule"], row["compiled_rule"]):
            raise ValueError("Plastic-processing parent changed; repeat review.")
        parent.clear()
        parent.update(deepcopy(row["original_rule"]))
    return resource


def before_fuels_312_review(resource, *, before_wastewater=False):
    """Restore the prior stage, including parents used as component bindings."""
    import json

    from brightpath import DATA_DIR

    review = json.loads((DATA_DIR / "export/uvek_fuels_312_dependencies_review.json").read_text())
    for row in review["records"]:
        parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        parent.clear()
        parent.update(deepcopy(row["original_rule"]))
        if "review_ref" in parent:
            resource["reviews"][parent["review_ref"]] = deepcopy(row["original_review"])
    if before_wastewater:
        # These later reviews already superseded three component bindings before
        # the fuel work. Replay the wastewater stage against its frozen inputs,
        # not against the final resource containing those later corrections.
        sources = (
            ("uvek_pilot_steel_row_routes_review.json", "bp-32fc216e24665d6f"),
            ("uvek_pilot_ng_heat_market_review.json", "bp-f7dd94e542cba21d"),
            ("uvek_heat_dependency_batch_review.json", "bp-667e1179d41e1bd5"),
        )
        for filename, rule_id in sources:
            authority = json.loads((DATA_DIR / "export" / filename).read_text())
            row = next(r for r in authority["records"] if r.get("previous_rule", {}).get("id") == rule_id)
            parent = next(r for r in resource["replace"] if r["id"] == rule_id)
            parent.clear()
            parent.update(deepcopy(row["previous_rule"]))
            if "review_ref" in parent and row.get("previous_review"):
                resource["reviews"][parent["review_ref"]] = deepcopy(row["previous_review"])
    # Earlier authoring stages also freeze component-rule metadata. Restore all
    # extended parents, then recompute only the summaries changed by that stage.
    from collections import Counter

    resource["coverage"]["mapping_methods"] = dict(
        sorted(Counter(r["mapping_method"] for r in resource["replace"]).items())
    )
    resource["coverage"]["mean_confidence"] = round(
        sum(r["confidence"] for r in resource["replace"]) / len(resource["replace"]), 6
    )
    dispositions = Counter(
        "recipe" if "recipe_ref" in r["target"] else r.get("classification", "unclassified")
        for r in resource["replace"]
    )
    dispositions["omitted"] = len(resource["delete"])
    if resource["coverage"].get("unresolved_identities"):
        dispositions["unresolved"] = resource["coverage"]["unresolved_identities"]
    resource["coverage"]["dispositions"] = dict(dispositions)
    return resource


def reviewed_fuels_312_record(rule_id, version):
    """Return a later exact-profile review without broadening older test scopes."""
    import json

    from brightpath import DATA_DIR

    review = json.loads((DATA_DIR / "export/uvek_fuels_312_dependencies_review.json").read_text())
    return next((r for r in review["records"] if r["rule_id"] == rule_id), None) if version == "3.12" else None


def before_mining_38_material_profiles(resource):
    """Restore the exact pre-extension parents for historical authoring tests."""
    import json

    from brightpath import DATA_DIR, TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules

    review = json.loads((DATA_DIR / "export/uvek_mining_38_material_suppliers_review.json").read_text())
    indexed = {rule["id"]: rule for rule in resource["replace"]}
    for row in review["records"]:
        previous = row["original_rule"]
        profiles = previous.get("reference_flow_sign_review", {}).get("profiles", [])
        if not profiles:
            continue
        actual = indexed[row["rule_id"]]
        assert actual in (previous, row["compiled_rule"])
        for entry in profiles:
            profile = TechnosphereProfile(**entry["source_profile"])
            assert select_profile_sign_rules({"replace": [actual]}, profile) == select_profile_sign_rules(
                {"replace": [previous]}, profile
            )
        actual.clear()
        actual.update(deepcopy(previous))
    return resource


def before_nylon6_39_review(resource):
    """Restore only the frozen nylon parent for earlier authoring replay tests."""
    import json

    from brightpath import DATA_DIR

    review = json.loads((DATA_DIR / "export/uvek_nylon6_39_review.json").read_text())
    row = review["records"][0]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    assert parent in (row["original_rule"], row["compiled_rule"])
    parent.clear()
    parent.update(deepcopy(row["original_rule"]))
    return resource


def before_converter_steel_39_review(resource):
    """Restore the exact old converter parent/card for historical authoring tests."""
    import json

    from brightpath import DATA_DIR

    review = json.loads((DATA_DIR / "export/uvek_converter_steel_39_review.json").read_text())
    row = review["records"][0]
    parent = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    assert parent in (row["original_rule"], row["compiled_rule"])
    new_ref = row["compiled_rule"]["review_ref"]
    assert resource["reviews"][new_ref] == review["reviews"][0]
    assert not any(r.get("review_ref") == new_ref and r["id"] != row["rule_id"] for r in resource["replace"])
    resource["reviews"].pop(new_ref)
    for ref, expected in review["retired_review_cards"].items():
        assert ref not in resource["reviews"] or resource["reviews"][ref] == expected
        resource["reviews"][ref] = deepcopy(expected)
    parent.clear()
    parent.update(deepcopy(row["original_rule"]))
    return resource
