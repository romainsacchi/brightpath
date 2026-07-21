import subprocess
import sys


def test_in_memory_migration_does_not_import_bw2io():
    code = """
import sys
from brightpath import BackgroundContext, BiosphereProfile, BrightwayInventory, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.background.catalogs import InMemoryCatalogProvider

source = BackgroundContext(TechnosphereProfile('ecoinvent', '3.9', 'cutoff'), BiosphereProfile('ecoinvent', '3.9'))
target = BackgroundContext(TechnosphereProfile('ecoinvent', '3.10', 'cutoff'), BiosphereProfile('ecoinvent', '3.10'))
inventory = BrightwayInventory.from_data([], context=InventoryContext(format=FormatProfile('brightway_excel'), background=source))
inventory.migrate_background(target, catalog_provider=InMemoryCatalogProvider())
assert 'bw2io' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", code], text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
