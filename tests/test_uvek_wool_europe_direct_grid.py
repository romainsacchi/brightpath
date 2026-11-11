"""The wool electricity simplification retires only one redundant helper."""

import json
from contextlib import ExitStack
from copy import deepcopy
from functools import lru_cache
from unittest.mock import patch

import pytest

from brightpath import DATA_DIR
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.review_uvek_wool_europe_direct_grid import (
    _digest,
    apply_wool_europe_direct_grid_review,
)


def review():
    return json.loads((DATA_DIR / "export/uvek_wool_europe_direct_grid_review.json").read_text())


def before_review():
    resource = deepcopy(load_uvek_technosphere_resource())
    decision = review()
    original = json.loads((DATA_DIR / "export/uvek_pilot_nitrate_wool_europe_review.json").read_text())
    for key in (decision["owner_recipe_id"], decision["retired_recipe_id"]):
        resource["recipes"][key] = deepcopy(original["auxiliary_recipes"][key])
    for row in decision["rules"]:
        rule = next(r for r in resource["replace"] if r["id"] == row["rule_id"])
        old_review = next(
            key for key, card in resource["reviews"].items() if _digest(card) == row["original_review_sha256"]
        )
        rule["review_ref"] = old_review
        assert _digest(rule) == row["original_rule_sha256"]
    return resource


def test_direct_european_grid_preserves_every_original_input_statistic():
    resource = before_review()
    decision = review()
    old = deepcopy(resource["recipes"][decision["owner_recipe_id"]])
    apply_wool_europe_direct_grid_review(resource)
    assert decision["retired_recipe_id"] not in resource["recipes"]
    new = resource["recipes"][decision["owner_recipe_id"]]
    index = decision["input_index"]
    assert old["inputs"][:index] == new["inputs"][:index]
    assert old["inputs"][index + 1 :] == new["inputs"][index + 1 :]
    for key, value in old["inputs"][index].items():
        if key != "recipe":
            assert new["inputs"][index][key] == value
    assert {key: new["inputs"][index][key] for key in decision["native_supplier"]} == decision["native_supplier"]
    assert resource == load_uvek_technosphere_resource()


def test_direct_european_grid_replay_is_idempotent():
    resource = deepcopy(load_uvek_technosphere_resource())
    before = deepcopy(resource)
    apply_wool_europe_direct_grid_review(resource)
    assert resource == before


def test_direct_european_grid_rejects_changed_source_inventory():
    resource = before_review()
    decision = review()
    resource["recipes"][decision["owner_recipe_id"]]["inputs"][decision["input_index"]]["amount"] *= 2
    before = deepcopy(resource)
    with pytest.raises(ValueError, match="changed before review"):
        apply_wool_europe_direct_grid_review(resource)
    assert resource == before


@lru_cache(maxsize=1)
def _historical_authoring_stages():
    # Earlier reviews intentionally refuse later changes to their frozen leaves.
    # Exercise their actual generation stages rather than a final-resource proxy.
    from scripts import generate_uvek_migration_resources as generator
    from scripts import review_uvek_pilot_nitrate_market as nitrate
    from scripts import review_uvek_pilot_nitrate_wool_europe as wool

    stages = {}

    def capture(label, original):
        def apply(resource, *args, **kwargs):
            original(resource, *args, **kwargs)
            stages[label] = deepcopy(resource)

        return apply

    with ExitStack() as stack:
        for label, module, name in (
            ("wool", wool, "apply_pilot_nitrate_wool_europe_review"),
            ("nitrate", nitrate, "apply_pilot_nitrate_market_review"),
        ):
            stack.enter_context(patch.object(module, name, capture(label, getattr(module, name))))
        generator.build_resources(
            DATA_DIR / "export/reference_catalogs",
            DATA_DIR / "export/ecoinvent_to_uvek_mapping.csv",
            DATA_DIR / "export/uvek_conversion_factors.yaml",
        )
    assert set(stages) == {"wool", "nitrate"}
    return stages


def historical_authoring_stage(label):
    return deepcopy(_historical_authoring_stages()[label])
