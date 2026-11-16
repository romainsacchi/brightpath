"""Historical recipes keep their own allocation authority and exact caller scope."""

from copy import deepcopy

import pytest
from test_uvek_cutoff_stocks import nested_stock_resource  # noqa: F401

from brightpath.exceptions import MigrationError
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_cutoff_stocks import zero_supplier_binding


@pytest.fixture
def historical_stock_resource(nested_stock_resource):  # noqa: F811
    resource = nested_stock_resource
    stock, parent = resource["replace"]
    model = {"family": "ecoinvent", "version": "3.9", "system_model": "cutoff"}
    stock_id = stock["target"]["recipe_ref"]
    historical = deepcopy(resource["recipes"]["parent"])
    historical["name"] = "Historical assembly model"
    historical["inputs"][0]["zero_burden_supplier"] = zero_supplier_binding(stock, model, stock_id)
    resource["recipes"]["historical-model"] = historical
    resource["recipes"]["parent"]["inputs"] = [
        {"recipe": "historical-model", "amount": 1, "inventory_source_profile": model}
    ]
    parent["reference_flow_sign_review"]["profiles"][0]["source_profile"] = {
        **model,
        "version": "3.7",
    }
    return resource


def test_explicit_historical_model_validates_against_its_own_stock_profile(historical_stock_resource):
    validate_migration_resource(historical_stock_resource)


@pytest.mark.parametrize(
    "change", ["missing_model", "wrong_version", "wrong_system", "digest", "unprofiled", "default"]
)
def test_historical_models_cannot_inherit_another_stock_authority(historical_stock_resource, change):
    resource = historical_stock_resource
    edge = resource["recipes"]["parent"]["inputs"][0]
    parent = resource["replace"][-1]
    if change == "missing_model":
        edge.pop("inventory_source_profile")
    elif change == "wrong_version":
        edge["inventory_source_profile"]["version"] = "3.8"
    elif change == "wrong_system":
        edge["inventory_source_profile"]["system_model"] = "consequential"
    elif change == "digest":
        resource["recipes"]["historical-model"]["inputs"][0]["zero_burden_supplier"]["profile_review_sha256"] = "0" * 64
    elif change == "unprofiled":
        parent.pop("reference_flow_sign_review")
    else:
        parent["reference_flow_sign_review"].update(status="verified", source_sign=1, target_sign=1)
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)
