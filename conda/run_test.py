"""Verify installed formats, catalogs and migration routes."""

from pathlib import Path
from tempfile import TemporaryDirectory

from brightpath.background import PackageCatalogProvider
from brightpath.background.execution import execute_background_migration
from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.migrations.resources import load_uvek_release_resources, load_uvek_technosphere_resource
from brightpath.models import InventoryDocument

for resource in (load_uvek_technosphere_resource(), load_uvek_export_resource()):
    assert resource["schema"] == "brightpath.migration"
    assert resource["schema_version"] == 3
    assert resource["replace"]

release_resources = {axis: load_uvek_release_resources(axis) for axis in ("technosphere", "biosphere")}
for resources in release_resources.values():
    assert len(resources) == 2
    for resource in resources:
        assert resource["schema"] == "brightpath.migration"
        assert resource["schema_version"] == 3
        assert resource["reverse_policy"] == "explicit_only"
        assert resource["replace"]

provider = PackageCatalogProvider()
assert len(provider.load_technosphere(TechnosphereProfile("uvek", "2026", "cutoff")).identities) == 11947
assert len(provider.load_biosphere(BiosphereProfile("uvek", "2026")).identities) == 2679

context = InventoryContext(
    FormatProfile("openlca_jsonld"),
    BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), BiosphereProfile("ecoinvent", "3.12")),
)
identity = {
    "name": "Conda package smoke test",
    "reference product": "test product",
    "location": "GLO",
    "unit": "kilogram",
}
document = InventoryDocument(
    data=[{**identity, "exchanges": [{**identity, "type": "production", "amount": 1.0}]}],
    context=context,
)
with TemporaryDirectory() as directory:
    path = write_openlca_jsonld(document, Path(directory) / "inventory.zip")
    restored = load_openlca_jsonld(path, context=context)
    assert len(restored.data) == 1
    for field, value in identity.items():
        assert restored.data[0][field] == value
    assert restored.data[0]["exchanges"][0]["amount"] == 1.0

for source_version, target_version in (("2025", "2026"), ("2026", "2025")):
    source = BackgroundContext(
        TechnosphereProfile("uvek", source_version, "cutoff"),
        BiosphereProfile("ecoinvent", "3.10") if source_version == "2025" else BiosphereProfile("uvek", "2026"),
    )
    target = BackgroundContext(
        TechnosphereProfile("uvek", target_version, "cutoff"),
        BiosphereProfile("ecoinvent", "3.10") if target_version == "2025" else BiosphereProfile("uvek", "2026"),
    )
    process_resource = next(
        resource
        for resource in release_resources["technosphere"]
        if resource["source_profiles"][0]["version"] == source_version
    )
    process_rule = next(
        rule for rule in process_resource["replace"] if rule["source"]["name"] == "Natural gas, at production"
    )
    biosphere_resource = next(
        resource
        for resource in release_resources["biosphere"]
        if resource["source_profiles"][0]["version"] == source.biosphere.version
    )
    biosphere_rule = next(
        rule for rule in biosphere_resource["replace"] if rule["source"]["name"] == "Carbon dioxide, fossil"
    )
    document = InventoryDocument(
        data=[
            {
                **identity,
                "exchanges": [
                    {**identity, "type": "production", "amount": 1},
                    {
                        **process_rule["source"],
                        "type": "technosphere",
                        "amount": 2,
                        "uncertainty type": 3,
                        "loc": 2,
                        "scale": 0.25,
                    },
                    {**biosphere_rule["source"], "type": "biosphere", "amount": 3},
                ],
            }
        ],
        context=InventoryContext(FormatProfile("brightway_excel"), source),
    )
    original = document.data
    result = execute_background_migration(document, target, provider)
    assert result.succeeded
    assert result.value.context.background == target
    assert document.data == original
    converted = result.value.data[0]["exchanges"]
    assert converted[0] == original[0]["exchanges"][0]
    assert all(converted[1][field] == value for field, value in process_rule["target"].items())
    assert (converted[1]["amount"], converted[1]["loc"], converted[1]["scale"]) == (2, 2, 0.25)
    assert converted[2]["name"] == biosphere_rule["target"]["name"]
    assert tuple(converted[2]["categories"]) == tuple(biosphere_rule["target"]["categories"])
    assert converted[2]["amount"] == 3

# Exercise an added publisher correspondence with the native biosphere held fixed.
export_resource = load_uvek_export_resource()
publisher_rules = [
    rule for rule in export_resource["replace"] if rule.get("mapping_method") == "publisher_product_correspondence"
]
assert len(export_resource["replace"]) == 1424
assert len(publisher_rules) == 1070
export_rule = next(
    rule
    for rule in publisher_rules
    if rule["source"]["name"] == "Zirconium oxide, at plant" and rule["source"]["location"] == "AU"
)
native_bridge = next(
    resource for resource in release_resources["technosphere"] if resource["source_profiles"][0]["version"] == "2026"
)
native_rule = next(rule for rule in native_bridge["replace"] if rule["target"] == export_rule["source"])
native_biosphere = next(
    resource for resource in release_resources["biosphere"] if resource["source_profiles"][0]["version"] == "2026"
)
native_flow = next(
    rule["source"] for rule in native_biosphere["replace"] if rule["source"]["name"] == "Carbon dioxide, fossil"
)
native_context = BackgroundContext(TechnosphereProfile("uvek", "2026", "cutoff"), BiosphereProfile("uvek", "2026"))
export_target = BackgroundContext(TechnosphereProfile("ecoinvent", "3.12", "cutoff"), native_context.biosphere)
document = InventoryDocument(
    data=[
        {
            **identity,
            "exchanges": [
                {**identity, "type": "production", "amount": 1},
                {
                    **native_rule["source"],
                    "type": "technosphere",
                    "amount": 2,
                    "uncertainty type": 3,
                    "loc": 2,
                    "scale": 0.25,
                },
                {**native_flow, "type": "biosphere", "amount": 3},
            ],
        }
    ],
    context=InventoryContext(FormatProfile("brightway_excel"), native_context),
)
original = document.data
result = execute_background_migration(document, export_target, provider)
assert result.succeeded, result.report.to_json()
assert result.value.context.background == export_target
assert document.data == original
converted = result.value.data[0]["exchanges"]
assert converted[0] == original[0]["exchanges"][0]
assert all(converted[1][field] == value for field, value in export_rule["target"].items())
assert (converted[1]["amount"], converted[1]["loc"], converted[1]["scale"]) == (2, 2, 0.25)
assert converted[2] == original[0]["exchanges"][2]
