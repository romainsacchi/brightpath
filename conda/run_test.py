"""Verify openLCA exchange and migration schemas using the installed packages."""

from pathlib import Path
from tempfile import TemporaryDirectory

from brightpath.background.uvek_export import load_uvek_export_resource
from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.migrations.resources import load_uvek_technosphere_resource
from brightpath.models import InventoryDocument

for resource in (load_uvek_technosphere_resource(), load_uvek_export_resource()):
    assert resource["schema"] == "brightpath.migration"
    assert resource["schema_version"] == 3
    assert resource["replace"]

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
