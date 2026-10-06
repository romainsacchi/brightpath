import base64
import hashlib
import json
import re
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

import preservation_helpers
import pytest
from preservation_helpers import (
    IDENTITY_FIELDS,
    Profile,
    assert_preserved,
    identity,
    import_artifact,
    synthetic_snapshot,
)

from brightpath.exceptions import InventoryValidationError

UVEK = Profile("uvek", "2025")
SUPPLIER = {
    "name": "Electricity, low voltage, at grid",
    "reference product": "Electricity, low voltage, at grid",
    "location": "CH",
    "unit": "kilowatt hour",
}


@pytest.fixture
def snapshot():
    return synthetic_snapshot(SUPPLIER)


def export_snapshots(snapshots):
    return preservation_helpers.export_snapshots(snapshots, UVEK, "simapro")


def metadata_comment(payload):
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return f"Original comment [BrightPath activity v1:{encoded}]"


def raw_dataset(snapshot, comment):
    return {
        **{field: snapshot[field] for field in IDENTITY_FIELDS},
        "reference product": snapshot["name"],
        "simapro name": f"{snapshot['name']}/{snapshot['location']} U",
        "comment": comment,
    }


@pytest.mark.parametrize("name", ["BrightPath roundtrip fixture", "Production — α"])
def test_uvek_csv_round_trip_preserves_identity_and_original_inventory(snapshot, name):
    snapshot["name"] = name
    snapshot["exchanges"][0]["name"] = name
    before = deepcopy(snapshot)
    for _iteration in range(2):
        input_before = deepcopy(snapshot)
        exported = export_snapshots([snapshot])
        assert snapshot == input_before
        content = exported.content.decode("latin-1")
        assert content.count("[BrightPath activity v1:") == 1
        assert "Electricity, low voltage, at grid/CH U" in content
        restored = import_artifact(exported, UVEK)
        assert len(restored) == 1
        assert_preserved(before, restored[0])
        assert "BrightPath activity" not in restored[0]["comment"]
        assert restored[0]["comment"].startswith(before["comment"])
        snapshot = restored[0]


@pytest.mark.parametrize("reverse_order", [False, True])
def test_bundled_foreground_links_and_self_links_keep_distinct_products(snapshot, reverse_order):
    supplier = deepcopy(snapshot)
    supplier.update(name="Synthetic supplier", **{"reference product": "Intermediate product"})
    supplier["exchanges"][0].update({field: supplier[field] for field in IDENTITY_FIELDS})
    linked = {field: supplier[field] for field in IDENTITY_FIELDS}
    snapshot["exchanges"].append({**linked, "type": "technosphere", "amount": 12.0, "formula": "dose"})
    supplier["exchanges"].append({**linked, "type": "technosphere", "amount": 0.25})
    snapshots = [snapshot, supplier]
    if reverse_order:
        snapshots.reverse()
    before = deepcopy(snapshots)
    exported = export_snapshots(snapshots)
    restored = {identity(item): item for item in import_artifact(exported, UVEK)}
    assert len(restored) == 2
    for original in snapshots:
        assert_preserved(original, restored[identity(original)])
    assert snapshots == before


def test_unannotated_uvek_csv_keeps_legacy_parsing_without_inventing_a_product(
    snapshot,
):
    exported = export_snapshots([snapshot])
    content = re.sub(rb" \[BrightPath activity v1:[A-Za-z0-9+/=]+\]", b"", exported.content)
    legacy = SimpleNamespace(filename=exported.filename, content=content)
    restored = import_artifact(legacy, UVEK)[0]
    assert restored["reference product"] == restored["name"] == snapshot["name"]
    assert identity(restored["exchanges"][1]) == identity(SUPPLIER)


def test_native_product_edits_conflict_with_metadata_in_actual_upload(snapshot):
    exported = export_snapshots([snapshot])
    edited = SimpleNamespace(
        filename=exported.filename,
        content=exported.content.replace(b"BrightPath roundtrip fixture/GLO U", b"Edited process/GLO U"),
    )
    with pytest.raises(ValueError, match="metadata conflicts with the native SimaPro product row"):
        import_artifact(edited, UVEK)


@pytest.mark.parametrize("mutation", ["reference product", "unicode name"])
def test_export_refuses_colliding_foreground_link_names(snapshot, mutation):
    other = deepcopy(snapshot)
    if mutation == "reference product":
        other["reference product"] = "Different product"
    else:
        snapshot["name"] = "Process α"
        other["name"] = "Process β"
    for item in (snapshot, other):
        item["exchanges"][0].update({field: item[field] for field in IDENTITY_FIELDS})
    with pytest.raises(InventoryValidationError, match="Ambiguous foreground SimaPro product name"):
        export_snapshots([snapshot, other])


def test_export_refuses_a_background_link_that_collides_with_foreground(snapshot):
    snapshot.update(SUPPLIER)
    snapshot["reference product"] = "Different foreground product"
    snapshot["exchanges"][0].update({field: snapshot[field] for field in IDENTITY_FIELDS})
    with pytest.raises(InventoryValidationError, match="supplier conflicts with a foreground"):
        export_snapshots([snapshot])


def test_identity_metadata_only_restores_allowed_identity_fields(snapshot):
    from brightpath.formats.simapro_csv import _restore_activity_identity
    from brightpath.models import BackgroundProfile

    fields = {field: snapshot[field] for field in IDENTITY_FIELDS}
    dataset = raw_dataset(snapshot, metadata_comment(fields))
    dataset.update(amount=17.0, formula="dose", database="existing", unit="kg")
    assert _restore_activity_identity(dataset, BackgroundProfile(**asdict(UVEK)))
    assert dataset["reference product"] == snapshot["reference product"]
    assert dataset["amount"] == 17.0
    assert dataset["formula"] == "dose"
    assert dataset["database"] == "existing"
    assert dataset["unit"] == "kg"
    assert dataset["comment"] == "Original comment"


@pytest.mark.parametrize(
    "mutation",
    [
        "name",
        "location",
        "unit",
        "unknown",
        "missing",
        "blank",
        "nested",
        "too_long",
        "list",
    ],
)
def test_invalid_or_stale_identity_metadata_fails_without_mutating_input(snapshot, mutation):
    from brightpath.formats.simapro_csv import _restore_activity_identity
    from brightpath.models import BackgroundProfile

    fields = {field: snapshot[field] for field in IDENTITY_FIELDS}
    if mutation in {"name", "location", "unit"}:
        fields[mutation] = {"name": "Another process", "location": "CH", "unit": "kilowatt hour"}[mutation]
    elif mutation == "unknown":
        fields["amount"] = 500.0
    elif mutation == "missing":
        fields.pop("reference product")
    elif mutation == "blank":
        fields["reference product"] = " "
    elif mutation == "nested":
        fields["reference product"] = {"value": "unsafe"}
    elif mutation == "too_long":
        fields["name"] = "name" * 1025
    else:
        fields = []
    dataset = raw_dataset(snapshot, metadata_comment(fields))
    before = deepcopy(dataset)
    with pytest.raises(ValueError, match="identity metadata"):
        _restore_activity_identity(dataset, BackgroundProfile(**asdict(UVEK)))
    assert dataset == before


@pytest.mark.parametrize(
    "marker",
    [
        "[BrightPath activity v1:!invalid!]",
        "[BrightPath activity v1:a]",
        "[BrightPath activity v1:e30=] trailing edit",
        "[BrightPath activity v2:e30=]",
        "[BrightPath activity v1:" + "A" * 65540 + "]",
        "[BrightPath activity v1:bm90LWpzb24=]",
    ],
    ids=["invalid-base64", "truncated-base64", "trailing-edit", "unsupported-version", "oversized", "invalid-json"],
)
def test_malformed_metadata_is_not_silently_accepted(snapshot, marker):
    from brightpath.formats.simapro_csv import _restore_activity_identity
    from brightpath.models import BackgroundProfile

    dataset = raw_dataset(snapshot, "User comment " + marker)
    before = deepcopy(dataset)
    with pytest.raises(ValueError, match="identity metadata"):
        _restore_activity_identity(dataset, BackgroundProfile(**asdict(UVEK)))
    assert dataset == before


@pytest.mark.parametrize("family", ["uvek", "ecoinvent"])
def test_plain_comments_are_not_interpreted_as_identity_metadata(snapshot, family):
    from brightpath.formats.simapro_csv import _restore_activity_identity
    from brightpath.models import BackgroundProfile

    dataset = raw_dataset(snapshot, "Normal user comment")
    before = deepcopy(dataset)
    assert not _restore_activity_identity(dataset, BackgroundProfile(family, "2025" if family == "uvek" else "3.12"))
    assert dataset == before


def test_import_refuses_ambiguous_local_names(snapshot):
    from brightpath.formats.simapro_csv import _restore_local_activity_links

    dataset = raw_dataset(snapshot, "")
    other = deepcopy(dataset)
    other["reference product"] = "Conflicting product"
    with pytest.raises(ValueError, match="Ambiguous foreground"):
        _restore_local_activity_links([dataset, other], {id(dataset), id(other)})


def test_simapro_fix_changes_the_cached_export_fingerprint():
    from brightpath import DATA_DIR, __version__
    from brightpath.background import migration_resource_fingerprint

    previous = hashlib.sha256(
        __version__.encode()
        + b"uvek-export-v3-preserve-amount-formulas"
        + (DATA_DIR / "migrations/RESOURCE_MANIFEST.json").read_bytes()
        + (DATA_DIR / "export/reference_catalogs/RESOURCE_MANIFEST.json").read_bytes()
    ).hexdigest()
    assert migration_resource_fingerprint() != previous
