"""Nontext source comments cannot prevent a reviewed recipe conversion."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.migrations.resources import load_uvek_technosphere_resource


@pytest.mark.parametrize("comment", [1.25, 42, "existing text", 0, None])
def test_recipe_provenance_accepts_source_comment_values_without_changing_quantities(comment):
    review = json.loads((DATA_DIR / "export/uvek_shared_pitch_nickel_39_review.json").read_text())
    row = next(r for r in review["records"] if r["rule_id"] == "bp-d700fe3d29d48083")
    resource = load_uvek_technosphere_resource()
    rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
    fingerprint = compatibility_rule_fingerprint(
        resource,
        "replace",
        rule,
        TechnosphereProfile(**review["source_profile"]),
        TechnosphereProfile(**review["target_profile"]),
    )
    policy = MigrationPolicy(approved_compatibility_rules=(fingerprint,), on_information_loss=PolicyAction.WARN)
    data = consumer(row["source"], 2, 3)
    data[0]["exchanges"][1]["comment"] = comment
    before = deepcopy(data)
    result = BrightwayInventory.from_data(data, context=contexts("ecoinvent", "3.9")).migrate_background(
        contexts("uvek", "2025").background, policy=policy
    )
    edge, original = result.data[0]["exchanges"][1], before[0]["exchanges"][1]
    for key in ("amount", "loc", "scale", "minimum", "maximum", "negative", "formula", "uncertainty type"):
        if key in original:
            assert edge[key] == original[key]
    prefix = str(comment) + "\n" if comment else ""
    assert edge["comment"].startswith(prefix + "Apply reviewed physical recipe")
    assert data == before
