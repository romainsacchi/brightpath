"""Coal-tar treatment omissions select exact profiles and preserve production mappings."""

from copy import deepcopy

import pytest
from test_uvek_reference_signs import FIELDS, consumer
from uvek_review_helpers import historical_forward_resource

from brightpath.exceptions import MigrationError
from brightpath.migrations.schema import validate_migration_resource
from scripts.review_uvek_reference_signs import COAL_TAR_SOURCE, COAL_TAR_TREATMENT_SOURCE


def coal_rules(resource):
    sources = {COAL_TAR_SOURCE, COAL_TAR_TREATMENT_SOURCE}
    return [r for r in resource["replace"] if tuple(r["source"][f] for f in FIELDS) in sources]


def coal_inventory(amount):
    data = consumer(dict(zip(FIELDS, COAL_TAR_SOURCE, strict=True)), amount, 2)
    exchange = consumer(dict(zip(FIELDS, COAL_TAR_TREATMENT_SOURCE, strict=True)), amount, 3, "substitution")
    data[0]["exchanges"].append(exchange[0]["exchanges"][1])
    data[0]["exchanges"].append(
        {
            "name": "Carbon dioxide, fossil",
            "categories": ["air"],
            "unit": "kilogram",
            "type": "biosphere",
            "amount": 0.5,
        }
    )
    for row in data[0]["exchanges"][1:3]:
        row["audit_note"] = "preserve this original metadata"
    return data


@pytest.mark.parametrize(
    "problem",
    ["factor", "unapproved", "pending", "missing_review", "empty_evidence", "wrong_operation", "orphan_marker"],
)
def test_invalid_profile_omission_decisions_cannot_load(problem):
    resource = deepcopy(historical_forward_resource())
    rule = coal_rules(resource)[0]
    entry = next(e for e in rule["reference_flow_sign_review"]["profiles"] if e.get("operation") == "delete")
    if problem == "factor":
        entry["conversion_factor"] = -1
    elif problem == "unapproved":
        entry["approved_omission"] = False
    elif problem == "pending":
        entry["review"]["status"] = "unresolved"
    elif problem == "missing_review":
        entry["review_ref"] = "unknown"
    elif problem == "empty_evidence":
        resource["reviews"][entry["review_ref"]]["evidence"] = []
    elif problem == "wrong_operation":
        entry["operation"] = "replace"
    else:
        del entry["operation"]
    with pytest.raises(MigrationError):
        validate_migration_resource(resource)
