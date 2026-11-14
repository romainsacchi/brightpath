"""Keep verified retired reviews outside the executable migration resource."""

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_PATH = "brightpath/data/export/uvek_archived_unreferenced_reviews.json"
ARCHIVE_SHA256 = "572ca2038b3bce62ca3d9741abd5727e281c575c52b8c3bf137b7c501d4547f4"


def archive_unreferenced_reviews(resource, root=ROOT):
    """Remove only frozen, unreferenced cards after checking all candidates."""
    raw = (root / ARCHIVE_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != ARCHIVE_SHA256:
        raise ValueError("Archived review evidence changed; repeat review.")
    archived = json.loads(raw)["reviews"]
    references = {
        rule["review_ref"]
        for operation in ("replace", "disaggregate", "delete")
        for rule in resource.get(operation, [])
        if "review_ref" in rule
    }
    references.update(
        entry["review_ref"]
        for rule in resource["replace"]
        for entry in rule.get("reference_flow_sign_review", {}).get("profiles", [])
        if "review_ref" in entry
    )
    reviews = resource.get("reviews", {})
    if set(archived) & references:
        raise ValueError("An archived review is referenced; retain it and repeat review.")
    if set(reviews) - references - set(archived):
        raise ValueError("Additional unreferenced reviews need independent archival review.")
    if any(key in reviews and reviews[key] != expected for key, expected in archived.items()):
        raise ValueError("A retired review changed; repeat review before archiving.")
    resource["reviews"] = {key: value for key, value in reviews.items() if key not in archived}
