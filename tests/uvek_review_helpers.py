"""Historical authoring evidence for tests; never used as an executable route."""

from copy import deepcopy
from functools import lru_cache
from pathlib import Path

from scripts.generate_uvek_migration_resources import _build_historical_review_resources


@lru_cache(maxsize=1)
def _historical_forward():
    root = Path(__file__).resolve().parents[1]
    resource, _ = _build_historical_review_resources(
        root / "brightpath/data/export/reference_catalogs",
        root / "brightpath/data/export/ecoinvent_to_uvek_mapping.csv",
        root / "brightpath/data/export/uvek_conversion_factors.yaml",
    )
    return resource


def historical_forward_resource():
    """Return independent archived-review inputs without restoring runtime support."""
    return deepcopy(_historical_forward())
