"""The source relabelling boundary buys primary metal, not a wrought-alloy mix."""

import json
from copy import deepcopy

import pytest
from test_uvek_reference_signs import consumer, contexts

from brightpath import DATA_DIR, BrightwayInventory, TechnosphereProfile
from brightpath.background.rule_approvals import compatibility_rule_fingerprint
from brightpath.core.policies import MigrationPolicy, PolicyAction
from brightpath.exceptions import MigrationError
from brightpath.migrations.resources import load_uvek_technosphere_resource

REVIEW = json.loads((DATA_DIR / "export/uvek_primary_aluminium_adapter_38_review.json").read_text())
ROW = REVIEW["records"][0]


def policy():
    return MigrationPolicy(
        approved_compatibility_rules=(
            compatibility_rule_fingerprint(
                load_uvek_technosphere_resource(),
                "replace",
                ROW["compiled_rule"],
                TechnosphereProfile("ecoinvent", "3.8", "cutoff"),
                TechnosphereProfile("uvek", "2025", "cutoff"),
            ),
        ),
        on_information_loss=PolicyAction.WARN,
    )


@pytest.mark.parametrize("amount", [-2.5, 2.5])
@pytest.mark.parametrize("release", ["2025", "2026"])
def test_adapter_replaces_the_whole_supply_with_primary_metal_once(amount, release):
    source = consumer(ROW["source"], amount, 3)
    before = deepcopy(source)
    result = BrightwayInventory.from_data(source, context=contexts("ecoinvent", "3.8")).migrate_background(
        contexts("uvek", release).background, policy=policy()
    )
    assert source == before and len(result.data) == 1
    assert len(result.data[0]["exchanges"]) == 2
    edge = result.data[0]["exchanges"][1]
    assert all(edge[k] == v for k, v in ROW["target"].items())
    assert edge["name"] == "Aluminium, primary, at plant"
    for key in ("amount", "loc", "scale", "formula", "uncertainty type"):
        assert edge[key] == before[0]["exchanges"][1][key]


@pytest.mark.parametrize("version,model", [("3.9", "cutoff"), ("3.8", "consequential")])
def test_adapter_review_does_not_expand_to_another_source_profile(version, model):
    with pytest.raises(MigrationError):
        BrightwayInventory.from_data(
            consumer(ROW["source"], 1), context=contexts("ecoinvent", version, model)
        ).migrate_background(contexts("uvek", "2026").background, policy=policy())


def test_adapter_evidence_has_no_forming_or_recycled_metal_permission():
    assert ROW["inspected_positive_primary_ingot_inputs"] == 3
    assert ROW["inspected_total_input_kg"] == pytest.approx(1)
    assert REVIEW["recipes"] == {} and REVIEW["new_foreground_omissions"] == []
    assert "recycled metal" in " ".join(ROW["limitations"])
