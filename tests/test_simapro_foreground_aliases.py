"""Export-only UVEK labels must keep distinct foreground products distinct."""

import base64
import json
from copy import deepcopy

import pytest
from preservation_helpers import assert_preserved, identity

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.exceptions import InventoryValidationError
from brightpath.formats.simapro_csv import _restore_activity_identity, _uvek_foreground_link_name

PROFILE = BackgroundProfile("uvek", "2025", "cutoff")
FIELDS = ("name", "reference product", "location", "unit")


def foreground():
    records = []
    for product, amount in (("conditioned cooling water", 1.0), ("used cooling water", -1.0)):
        supplier = {
            "name": "Foreground cooling loop",
            "reference product": product,
            "location": "DE",
            "unit": "cubic meter",
        }
        records.append(
            {
                **supplier,
                "comment": "Original provenance.",
                "source": "Synthetic cited source",
                "custom scalar": 17,
                "parameters": [{"name": "batch", "amount": 1.0}],
                "exchanges": [
                    {
                        **supplier,
                        "type": "production",
                        "amount": amount,
                        "formula": "batch" if amount > 0 else "-batch",
                        "simapro category": "Materials/Other",
                    },
                ],
            }
        )
    consumer = {"name": "Cooling consumer", "reference product": "cooled item", "location": "DE", "unit": "unit"}
    records.append(
        {
            **consumer,
            "comment": "Original consumer.",
            "parameters": [{"name": "dose", "amount": 0.25}],
            "exchanges": [
                {**consumer, "type": "production", "amount": 1.0, "simapro category": "Materials/Other"},
                *[
                    {**{key: r[key] for key in FIELDS}, "type": "technosphere", "amount": amount, "formula": formula}
                    for r, amount, formula in zip(records, (0.25, -0.25), ("dose", "-dose"), strict=True)
                ],
            ],
        }
    )
    records[0]["exchanges"].append(
        {**{key: records[0][key] for key in FIELDS}, "type": "technosphere", "amount": 0.125}
    )
    return records


@pytest.mark.parametrize("reverse_order", [False, True])
def test_distinct_foreground_products_keep_signed_quantities_formulas_and_links(tmp_path, reverse_order):
    source = foreground()
    if reverse_order:
        source.reverse()
    before = deepcopy(source)
    inventory = SimaProInventory.from_data(source, background_profile=PROFILE)
    original_aliases = {
        _uvek_foreground_link_name({key: r[key] for key in FIELDS})
        for r in source
        if r["name"] == "Foreground cooling loop"
    }
    assert len(original_aliases) == 2
    for iteration in range(2):
        snapshot = inventory.data
        path = inventory.write_csv(tmp_path / f"products-{iteration}.csv")
        content = path.read_text(encoding="latin-1")
        assert all(label in content for label in original_aliases)
        assert "Source: Synthetic cited source" in content
        restored = SimaProInventory.from_csv(path, background_profile=PROFILE)
        by_identity = {identity(r): r for r in restored.data}
        assert len(by_identity) == 3
        for original in before:
            actual = by_identity[identity(original)]
            assert_preserved(original, actual)
            assert actual["comment"].startswith(original["comment"])
            if original.get("source"):
                assert original["source"] in actual["comment"]
        assert inventory.data == snapshot
        inventory = restored
    assert source == before


def test_labels_do_not_depend_on_bundle_order(tmp_path):
    source = foreground()
    outputs = []
    for index, data in enumerate((source, list(reversed(source)))):
        path = SimaProInventory.from_data(data, background_profile=PROFILE).write_csv(tmp_path / f"order-{index}.csv")
        outputs.append(
            sorted(row for row in path.read_text(encoding="latin-1").splitlines() if "[BrightPath fg " in row)
        )
    assert outputs[0] == outputs[1]


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("case_variant", [False, True])
def test_generated_alias_collision_with_foreground_or_external_supplier_still_fails(tmp_path, external, case_variant):
    source = foreground()
    label = _uvek_foreground_link_name({key: source[0][key] for key in FIELDS})
    name = label.removesuffix("/DE U")
    if case_variant:
        name = name.upper()
    supplier = {"name": name, "reference product": "Unrelated product", "location": "DE", "unit": "cubic meter"}
    if external:
        source[-1]["exchanges"].append({**supplier, "type": "technosphere", "amount": 2.0})
    else:
        source.append(
            {
                **supplier,
                "comment": "Unrelated process.",
                "exchanges": [{**supplier, "type": "production", "amount": 1, "simapro category": "Materials/Other"}],
            }
        )
    before = deepcopy(source)
    with pytest.raises(InventoryValidationError, match="alias collides"):
        SimaProInventory.from_data(source, background_profile=PROFILE).write_csv(tmp_path / "collision.csv")
    assert source == before


def test_duplicate_canonical_foreground_identity_still_fails(tmp_path):
    source = foreground()
    source.append(deepcopy(source[0]))
    with pytest.raises(InventoryValidationError, match="Ambiguous foreground"):
        SimaProInventory.from_data(source, background_profile=PROFILE).write_csv(tmp_path / "duplicate.csv")


@pytest.mark.parametrize("field", FIELDS)
def test_alias_identity_metadata_rejects_edits_to_any_bound_identity_field(field):
    source = foreground()
    original = source[0]
    label = _uvek_foreground_link_name({key: original[key] for key in FIELDS})
    identity_fields = {key: original[key] for key in FIELDS}
    identity_fields[field] = "Edited identity"
    encoded = base64.b64encode(json.dumps(identity_fields).encode()).decode()
    dataset = {"simapro name": label, "unit": original["unit"], "comment": f"[BrightPath activity v1:{encoded}]"}
    before = deepcopy(dataset)
    with pytest.raises(ValueError, match="metadata conflicts"):
        _restore_activity_identity(dataset, PROFILE)
    assert dataset == before
