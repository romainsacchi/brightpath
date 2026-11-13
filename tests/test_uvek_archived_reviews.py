"""Archiving retired cards cannot change executable rules or active reviews."""

import json
from copy import deepcopy

import pytest

from brightpath import DATA_DIR
from brightpath.migrations.resources import load_uvek_technosphere_resource
from scripts.archive_uvek_unreferenced_reviews import archive_unreferenced_reviews

ARCHIVED = json.loads((DATA_DIR / "export/uvek_archived_unreferenced_reviews.json").read_text())["reviews"]


@pytest.mark.parametrize("drift", [None, "changed", "referenced", "new_orphan"])
def test_archiving_is_atomic_and_preserves_all_executable_values(drift):
    resource = deepcopy(load_uvek_technosphere_resource())
    resource["reviews"].update(deepcopy(ARCHIVED))
    first = next(iter(ARCHIVED))
    if drift == "changed":
        resource["reviews"][first]["rationale"] += " changed"
    elif drift == "referenced":
        resource["replace"][0]["review_ref"] = first
    elif drift == "new_orphan":
        resource["reviews"]["unknown-orphan"] = deepcopy(ARCHIVED[first])
    before = deepcopy(resource)
    if drift:
        with pytest.raises(ValueError):
            archive_unreferenced_reviews(resource)
        assert resource == before
    else:
        archive_unreferenced_reviews(resource)
        assert resource == {**before, "reviews": {k: v for k, v in before["reviews"].items() if k not in ARCHIVED}}
        again = deepcopy(resource)
        archive_unreferenced_reviews(resource)
        assert resource == again
