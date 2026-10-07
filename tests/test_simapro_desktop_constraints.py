"""Regressions from SimaPro Desktop import diagnostics, using synthetic data."""

from copy import deepcopy

import pytest

from brightpath import BackgroundProfile, SimaProInventory
from brightpath.exceptions import SimaProSerializationError
from brightpath.formats.simapro_csv import _simapro_process_identifier


def activity(name="example", category="material/Test", **metadata):
    return {
        "name": name,
        "reference product": name,
        "unit": "kilogram",
        "location": "GLO",
        "comment": "Synthetic import regression.",
        "simapro metadata": metadata,
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "reference product": name,
                "unit": "kilogram",
                "location": "GLO",
                "amount": -2 if category.startswith("waste treatment/") else 2,
                "simapro category": category,
            }
        ],
    }


def inventory(data, **kwargs):
    return SimaProInventory.from_data(
        data, background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"), **kwargs
    )


@pytest.mark.parametrize("identifier", ["EI3ARUNI000000000000001", "TESTPREX214748364799999"])
def test_importable_native_identifiers_are_preserved(identifier):
    assert _simapro_process_identifier(identifier, {}) == identifier


@pytest.mark.parametrize("identifier", ["TESTPREX214748364800000", "BRTPATH0999999999999999", "canonical-code"])
def test_generated_identifiers_stay_within_desktop_numeric_range(identifier):
    result = _simapro_process_identifier(identifier, {})
    assert len(result) == 23
    assert result.startswith("BRTPATH0")
    assert result[8:].isascii() and result[8:].isdigit()
    assert int(result[8:18]) < 1_000_000_000
    assert _simapro_process_identifier(identifier, {}) == result
    assert _simapro_process_identifier(result, {}) == result


def test_duplicate_process_identifiers_stop_publication(tmp_path):
    data = [activity("first"), activity("second")]
    for dataset in data:
        dataset["code"] = "same-canonical-code"
    destination = tmp_path / "existing.csv"
    destination.write_text("previous inventory")
    with pytest.raises(SimaProSerializationError, match="Duplicate SimaPro process identifier"):
        inventory(data).write_csv(destination, validate=False)
    assert destination.read_text() == "previous inventory"


@pytest.mark.parametrize("waste", [False, True])
def test_allocation_keywords_follow_process_category_and_preserve_metadata(waste, tmp_path):
    category = "waste treatment/Test" if waste else "material/Test"
    label = "Waste treatment allocation" if waste else "Multiple output allocation"
    data = [activity(category=category, **{label: "Physical causality"})]
    original = deepcopy(data)
    source = inventory(data)
    result = source.render()
    assert not result.has_errors, result.issues
    rows = result.rows
    assert rows[rows.index([label]) + 1] == ["Physical causality"]
    assert (["Waste treatment allocation"] in rows) == waste
    assert (["Multiple output allocation"] in rows) == (not waste)
    assert (["Substitution allocation"] in rows) == (not waste)
    assert data == source.data == original
    path = source.write_csv(tmp_path / "allocation.csv", validate=False)
    restored = SimaProInventory.from_csv(path, background_profile=source.background_profile)
    assert restored.data[0]["simapro metadata"][label] == "Physical causality"


def test_disallowed_nondefault_allocation_is_not_silently_dropped():
    result = inventory(
        [activity(category="waste treatment/Test", **{"Substitution allocation": "Custom rule"})]
    ).render()
    assert result.rows == []
    assert any(issue.code == "simapro_metadata_not_allowed" for issue in result.issues)


@pytest.mark.parametrize("excess", [0, 1])
@pytest.mark.parametrize("field", ["folder", "process_system", "document_system"])
def test_desktop_text_limits_are_checked_before_rendering(field, excess):
    data = [activity()]
    metadata = {}
    if field == "folder":
        data[0]["exchanges"][0]["simapro category"] = "material/" + "x" * (255 + excess)
    elif field == "process_system":
        data[0]["simapro metadata"]["System description"] = "x" * (50 + excess)
    else:
        metadata = {"system description": {"name": "x" * (50 + excess), "description": "Full description"}}
    original = deepcopy(data)
    result = inventory(data, metadata=metadata).render()
    assert result.has_errors == bool(excess)
    assert bool(result.rows) == (not excess)
    assert all(issue.code == "simapro_text_too_long" for issue in result.issues)
    assert data == original
