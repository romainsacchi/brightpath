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
        folder = "/".join(["x" * 50] * 4 + ["x" * (51 + excess)])
        data[0]["exchanges"][0]["simapro category"] = "material/" + folder
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


@pytest.mark.parametrize("excess", [0, 1])
@pytest.mark.parametrize("field", ["product", "system"])
def test_individual_folder_names_have_a_separate_sixty_character_limit(excess, field):
    data = [activity()]
    folder = "Parent/" + "x" * (60 + excess)
    metadata = {}
    if field == "product":
        data[0]["simapro category path"] = folder
    else:
        metadata = {"system description": {"name": "Example", "category": folder}}
    result = inventory(data, metadata=metadata).render()
    assert result.has_errors == bool(excess)
    if excess:
        assert any("folder name" in issue.message for issue in result.issues)


@pytest.mark.parametrize("category", [None, "", "Custom/Category"])
def test_system_description_definition_has_required_category_without_mutation(category):
    system = {"name": "Example", "description": "Complete provenance"}
    if category is not None:
        system["category"] = category
    metadata = {"system description": system}
    before = deepcopy(metadata)
    source = inventory([activity()], metadata=metadata)
    result = source.render()
    assert not result.has_errors
    rows = result.rows
    assert rows[rows.index(["Category"]) + 1] == ["Custom\\Category" if category else "Others"]
    assert rows[rows.index(["Name"]) + 1] == ["Example"]
    assert metadata == before and source.metadata == before


@pytest.mark.parametrize("version", ["3.8", "3.9.1", "3.10", "3.12"])
@pytest.mark.parametrize("model", ["cutoff", "consequential"])
def test_ocean_salt_water_uses_native_volume_based_name(version, model):
    data = [activity()]
    data[0]["exchanges"].append(
        {
            "type": "biosphere",
            "name": "Water, salt, ocean",
            "categories": ("natural resource", "in water"),
            "unit": "cubic meter",
            "amount": 0.0393,
            "uncertainty type": 3,
            "loc": 0.0393,
            "scale": 0.005,
        }
    )
    before = deepcopy(data)
    source = SimaProInventory.from_data(data, background_profile=BackgroundProfile("ecoinvent", version, model))
    result = source.render()
    assert not result.has_errors, result.issues
    row = next(row for row in result.rows if row and row[0] == "Water, salt, ocean")
    assert row[1:5] == ["in water", "m3", "0.0393", "Normal"]
    assert float(row[5]) == pytest.approx(0.005**2)
    assert not any(row and row[0] == "Water, cooling, salt, ocean" for row in result.rows)
    assert data == source.data == before
    repeated = source.render()
    assert not repeated.has_errors
    # Export timestamps can change between renders; the resource row must not.
    assert next(item for item in repeated.rows if item and item[0] == "Water, salt, ocean") == row
    assert data == source.data == before
