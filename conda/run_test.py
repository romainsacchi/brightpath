"""Verify openLCA exchange using the installed Conda packages."""

from pathlib import Path
from tempfile import TemporaryDirectory

from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, write_openlca_jsonld
from brightpath.models import InventoryDocument

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
