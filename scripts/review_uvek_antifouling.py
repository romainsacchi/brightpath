"""Bind approved marine antifouling-emission omissions to inspected inventories."""

import json
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
REVIEW_PATH = "brightpath/data/export/uvek_antifouling_omission_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
APPROVED_ANTIFOULING_OMISSIONS = {}
for row in APPROVAL["rules"]:
    source = tuple(row["source"][f] for f in FIELDS)
    market = source[0].startswith("market for")
    boundary = (
        "The inspected source market supplies 50% copper-based and 50% tin-based paint-emission services "
        "through PE and RoW suppliers, with no additional active exchanges."
        if market
        else "The inspected source represents marine paint releases through biosphere exchanges, "
        "without active technosphere processing inputs."
    )
    APPROVED_ANTIFOULING_OMISSIONS[source] = {
        "source_versions": ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"),
        "previous_target": tuple(row["previous_target"][f] for f in FIELDS),
        "previous_native_flow_type": row["previous_native_flow_type"],
        "profiles": row["profiles"],
        "review": {
            "rationale": (
                f"User-approved antifouling-paint emissions omission: omit the exact {source[0]} / "
                f"{source[2]} / kilogram external exchange and continue conversion with a warning. "
                "No suitable UVEK antifouling-emission service was established. The former uranium-milling "
                "and lorry tyre-wear targets do not represent marine paint releases. Earlier sign "
                "verification of the four direct mappings established coordinates only; their scientific "
                "correspondence is rejected. " + boundary + " Omission excludes the associated marine "
                "metal and organotin releases and may understate ecotoxicity for ordinary releases; "
                "positive avoided-release inputs lose their associated credits. Score effects are "
                "method-dependent and unquantified."
            ),
            "limitations": [
                "The associated marine releases, including metals and organotin compounds, are not represented "
                "in the target dataset. Ordinary paint releases may therefore have understated ecotoxicity "
                "impacts; positive avoided-release inputs also lose their associated credits. Score effects "
                "are method-dependent and unquantified. This is an explicit information loss, not a zero-impact "
                "service or an equivalent emissions proxy. Preserve paint-manufacturing inputs, matching "
                "foreground suppliers, production and separately listed foreground emissions. Retain the "
                "complete original exchange, including sign, uncertainty, formula and caller metadata, in the "
                "loss report. No helper dataset, keyword-based omission or reverse reconstruction is inferred."
            ],
            "evidence": [
                f"{REVIEW_PATH}: five exact source identities and 70 inspected source/profile inventory "
                "fingerprints across ecoinvent 3.6--3.12 cut-off and consequential; production -1 kg. "
                "Every market supplies four emission providers: each chemistry has 0.0014615 kg PE and "
                "0.4985385 kg RoW per kg reference. Direct Cu/Sn services have marine biosphere releases; "
                "3.12 also carries a disposal indicator. No complete inventories are published.",
                "Read-only inspection of the UVEK 2025 catalog found no dedicated antifouling-emission "
                "supplier. The prior tyre-wear service represents road-transport releases to air, groundwater "
                "and soil; the prior uranium-milling supplier includes unrelated mill inputs and emissions.",
                "User explicitly approved all five omissions and authorized independently inspected similar cases. "
                "Historical targets, factors and sign-review statuses remain in the sparse audit.",
            ],
        },
    }


def validate_antifouling_inventories(source, observations, inventories):
    """Require the same inspected profiles and inventories before replaying an omission."""
    expected = sorted((p["label"], p["inventory_sha256"]) for p in APPROVED_ANTIFOULING_OMISSIONS[source]["profiles"])
    if sorted(observations) != [(label, -1) for label, _ in expected] or sorted(inventories) != expected:
        raise ValueError("Antifouling emission inventory or profile changed; repeat the scientific review.")
