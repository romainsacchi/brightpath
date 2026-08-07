from copy import deepcopy

from brightpath import BackgroundProfile, SimaProCategoryMode, SimaProInventory
from brightpath.profiles.simapro_categories import (
    SimaProCategoryCatalog,
    SimaProCategoryReference,
    load_simapro_category_catalog,
    resolve_simapro_category,
)


def profile(version="3.9"):
    return BackgroundProfile("ecoinvent", version, "cutoff")


def carbon_capture_activity(*, market=False):
    name = "market for carbon dioxide, captured" if market else "carbon dioxide capture"
    return {
        "name": name,
        "reference product": "carbon dioxide, captured",
        "location": "GLO",
        "unit": "kilogram",
        "comment": "Documented foreground dataset.",
        "exchanges": [
            {
                "type": "production",
                "name": name,
                "reference product": "carbon dioxide, captured",
                "product": "carbon dioxide, captured",
                "location": "GLO",
                "unit": "kilogram",
                "amount": 1.0,
                "simapro category": "material/carbon dioxide, captured",
            }
        ],
    }


def row_after(rows, label):
    return rows[rows.index([label]) + 1]


def test_packaged_category_catalog_covers_the_complete_reference_taxonomy():
    catalog = load_simapro_category_catalog(profile())

    assert catalog.source_version == "3.9.1"
    assert len(catalog.references) == 8525
    assert len(catalog.categories) == 542
    assert "material/Chemicals/Gases/Transformation" in catalog.categories
    assert "waste scenario/Others/Obsolete" in catalog.categories


def test_exact_product_unit_and_role_match_precedes_fuzzy_inference():
    resolution = resolve_simapro_category(
        {
            "name": "carbon dioxide production",
            "reference product": "carbon dioxide, in chemical industry",
            "unit": "kilogram",
        },
        profile=profile(),
        current_category="material/custom",
    )

    assert resolution.category == "material/Chemicals/Gases/Transformation"
    assert resolution.method == "exact_product"
    assert resolution.confidence == 1.0


def test_fuzzy_products_must_agree_on_an_existing_hierarchy():
    production = resolve_simapro_category(carbon_capture_activity(), profile=profile())
    market = resolve_simapro_category(carbon_capture_activity(market=True), profile=profile())

    assert production.category == "material/Chemicals/Gases/Transformation"
    assert production.method == "fuzzy_hierarchy"
    assert production.confidence >= 0.78
    assert market.category == "material/Chemicals/Gases/Market"


def test_ambiguous_exact_product_categories_are_not_silently_selected():
    references = (
        SimaProCategoryReference("sample product", "kilogram", "transformation", "material/Chemicals", 1),
        SimaProCategoryReference("sample product", "kilogram", "transformation", "material/Metals", 1),
    )
    catalog = SimaProCategoryCatalog(
        profile=profile(),
        source_version="test",
        references=references,
        categories=frozenset(reference.category for reference in references),
        digest="",
        source="memory",
    )

    resolution = resolve_simapro_category(
        {"name": "sample production", "reference product": "sample product", "unit": "kilogram"},
        profile=profile(),
        current_category="material/custom",
        catalog=catalog,
    )

    assert resolution.category == "material/custom"
    assert resolution.method == "unresolved"


def test_existing_reference_category_is_preserved():
    resolution = resolve_simapro_category(
        carbon_capture_activity(),
        profile=profile(),
        current_category="Materials/Chemicals/Gases/Transformation",
    )

    assert resolution.category == "material/Chemicals/Gases/Transformation"
    assert resolution.method == "existing"


def test_category_inference_is_explicit_reported_and_non_mutating():
    activity = carbon_capture_activity()
    source = deepcopy(activity)
    inventory = SimaProInventory.from_data([activity], background_profile=profile())

    preserved = inventory.render()
    inferred = inventory.render(category_mode=SimaProCategoryMode.INFER_EXISTING)

    assert row_after(preserved.rows, "Products")[5] == "carbon dioxide, captured"
    assert row_after(inferred.rows, "Category type") == ["material"]
    assert row_after(inferred.rows, "Products")[5] == "Chemicals\\Gases\\Transformation"
    assert [issue.code for issue in inferred.issues] == ["simapro_category_inferred"]
    assert inventory.data == [source]
    assert activity == source


def test_write_csv_forwards_the_explicit_category_mode(tmp_path):
    inventory = SimaProInventory.from_data(
        [carbon_capture_activity()],
        background_profile=profile(),
    )

    path = inventory.write_csv(
        tmp_path / "inferred.csv",
        validate=False,
        category_mode="infer_existing",
    )

    assert "Chemicals\\Gases\\Transformation" in path.read_text(encoding="latin-1")


def test_inference_reports_when_the_exact_profile_has_no_category_catalog():
    inventory = SimaProInventory.from_data(
        [carbon_capture_activity()],
        background_profile=profile("3.10"),
    )

    result = inventory.render(category_mode="infer_existing")

    assert row_after(result.rows, "Products")[5] == "carbon dioxide, captured"
    assert [issue.code for issue in result.issues] == ["simapro_category_catalog_unavailable"]
