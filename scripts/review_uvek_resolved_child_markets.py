"""Compose inspected waste markets from reviewed treatment children and complete freight."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_resolved_child_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
if (
    len(SOURCES) != 11
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 11
    or APPROVAL["source_profile_count"] != 54
    or APPROVAL["approved_source_profile_count"] != 46
    or APPROVAL["pending_source_profile_count"] != 8
    or APPROVAL["inventory_count"] != 135
    or sum(len(r["profiles"]) for r in SOURCES.values()) != 46
    or sum(len(r["pending_profiles"]) for r in SOURCES.values()) != 8
    or sum(not r["pending_profiles"] for r in SOURCES.values()) != 9
    or any(not s[0].startswith("market for ") or s[3] != "kilogram" for s in SOURCES)
):
    raise ValueError("Resolved-child market scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {
        ("ecoinvent", v, "cutoff") for v in ("3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12")
    } | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed resolved-child market evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))
LIMITATIONS = (
    "Reviewed complete waste-market helper per one kg source waste. Retain the exact reviewed "
    "residual-landfill child, including waste-specific releases, stabilization, operation and construction. "
    "The source reference is -1 kg; the helper reference is +1 kg: apply factor -1 to caller demand and "
    "each child reference factor independently. Retain original positive road, rail and barge ton-km "
    "without mass/distance conversion or renormalization. Source markets have no direct biosphere "
    "exchanges. Do not add upstream incineration, wastewater treatment, alternative disposal, recovered "
    "products or credits. Scoped freight uses the complete RER UVEK road fleet, complete CH electric "
    "rail with shunting and diesel rail with particle filter at the original traction shares, and complete "
    "RER inland barge service. These include vehicles, maintenance and infrastructure; add no second "
    "operation or assets. Geographic conditions, fleet/class mix, gauge, filters, load, vintage, fuels, "
    "energy use and background differ. The barge inventories differ in load/capacity and lifetime; "
    "preserve ton-km without vessel-capacity or lifetime scaling. These are screening service proxies, "
    "not reconstructed regional fleets or equal LCIA scores. Do not substitute lorries for trains, "
    "coal freight for general freight, or diesel for steam. The eight older clinker-dependent profiles "
    "remain pending without omission or inferred mineral credits. Native child water/land refinements "
    "and optional inventory-indicator omissions retain their own warnings. Score/export all returned "
    "helpers. Preserve signed caller amounts, uncertainty, formulas, metadata, foreground suppliers, "
    "production and separately listed emissions. Internal source uncertainty/parameter formulas are "
    "not reconstructed. Only enumerated exact cut-off profiles are approved; no profile, patch, model, "
    "geography or inverse fallback is inferred. Global freight rules are unchanged."
)


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def transport(name, location):
    return dict(zip(FIELDS, (name, name, location, "ton kilometer"), strict=True))


ROAD = transport("Transport, freight, lorry, fleet average", "RER")
DIESEL = transport("Transport, freight, rail, diesel, with particle filter", "CH")
ELECTRIC = transport("Transport, freight, rail, electricity with shunting", "CH")
BARGE = transport("Transport, barge", "RER")


def inventory_record(source, blobs, comment, product_information):
    """Pin actual vectors, descriptions and reference magnitudes without source inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Resolved-child-market evidence reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Read original exact patch inventories, never premise scenarios or mutable projects."""
    records = []
    for entry in APPROVAL["extra_profiles"]:
        scope = EVIDENCE_SCOPE[tuple(entry["profile"])]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            found = set()
            for name in sorted({s[0] for s in scope}):
                for (raw,) in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    metadata = pickle.loads(raw)
                    source = identity(metadata)
                    if source not in scope:
                        continue
                    blobs = [
                        b
                        for b, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    records.append(
                        {
                            **inventory_record(
                                source, blobs, metadata.get("comment"), metadata.get("product_information")
                            ),
                            "label": "-".join(entry["profile"]),
                        }
                    )
                    found.add(source)
            if found != scope:
                raise ValueError("Resolved-child-market patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all actual source, freight and independently inspected reverse inventories."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 135 or sum(map(len, inventories.values())) != 135:
        raise ValueError("Resolved-child market inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        wanted = {(p["label"], -1) for p in [*row["profiles"], *row["pending_profiles"]]}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if wanted != observed or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Resolved-child market exact references changed; repeat the scientific review.")
    if len(APPROVAL["native_roles"]) != 8:
        raise ValueError("Resolved-child market native scope changed.")
    for row in APPROVAL["native_roles"]:
        source = identity(row["source"])
        if (
            native.get(source, {}).get("native_flow_type") != "PRODUCT_FLOW"
            or native[source].get("native_reference_is_input") is not False
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Resolved-child market native supplier role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze child recipes, full freight functions, exact shares and existing reverse choices."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Resolved-child market")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    local = APPROVAL["local_recipes"]
    if len(choices) != 44 or len(local) != 3:
        raise ValueError("Resolved-child market freight scope changed.")
    used, local_used = set(), set()
    for row in SOURCES.values():
        for profile in row["profiles"]:
            expected = []
            for entry in profile["freight_choices"]:
                key = profile["label"], tuple(entry["source"])
                choice = choices[key]
                if (
                    digest(choice) != entry["choice_sha256"]
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                    or choice["conversion_factor"] != 1
                    or entry["amount"] <= 0
                ):
                    raise ValueError("Resolved-child market freight evidence or quantity changed.")
                kind, source = choice["kind"], key[1]
                if kind in {"road", "barge"}:
                    target = ROAD if kind == "road" else BARGE
                    if ("lorry" if kind == "road" else "barge") not in source[0] or choice["target"] != target:
                        raise ValueError("Unreviewed resolved-child market freight function.")
                    expected.append({**target, "type": "technosphere", "amount": entry["amount"]})
                elif kind == "rail" and "train" in source[0]:
                    rid = choice["target"].get("recipe_ref")
                    if rid not in local or len(choice["members"]) != 2:
                        raise ValueError("Unreviewed resolved-child market rail mix.")
                    rail = []
                    for member in choice["members"]:
                        provider = tuple(member["source"])
                        traction = member["traction"]
                        if (
                            traction not in {"diesel", "electric"}
                            or traction not in provider[0]
                            or "steam" in provider[0]
                            or provider[3] != "ton kilometer"
                            or member["amount"] <= 0
                            or member["inventory_sha256"] != proofs[profile["label"], provider]["inventory_sha256"]
                        ):
                            raise ValueError("Resolved-child market rail traction or reference changed.")
                        rail.append(
                            {
                                **(DIESEL if traction == "diesel" else ELECTRIC),
                                "type": "technosphere",
                                "amount": member["amount"],
                            }
                        )
                    rail.sort(key=lambda e: e["name"])
                    if (
                        abs(sum(e["amount"] for e in rail) - 1) > 1e-12
                        or local[rid]["inputs"] != rail
                        or local[rid]["unit"] != "ton kilometer"
                    ):
                        raise ValueError("Resolved-child market rail weights changed; repeat the scientific review.")
                    expected.append({"recipe": rid, "amount": entry["amount"]})
                    local_used.add(rid)
                else:
                    raise ValueError("Unreviewed resolved-child market freight service.")
                used.add(key)
            actual = [e for e in profile["inputs"] if e.get("unit") == "ton kilometer" or e.get("recipe") in local]
            if actual != expected or any(e.get("type") == "biosphere" for e in profile["inputs"]):
                raise ValueError("Resolved-child market freight quantities or biosphere boundary changed.")
    if used != set(choices) or local_used != set(local):
        raise ValueError("Resolved-child market unused or missing freight choices.")
    reverse = json.loads((ROOT / "brightpath/data/migrations/uvek/uvek-2025-to-ecoinvent-3.12-cutoff.json").read_text())
    by_source = {identity(r["source"]): r for r in reverse["replace"]}
    if len(APPROVAL["reverse_bindings"]) != 4 or {identity(r["source"]) for r in APPROVAL["reverse_bindings"]} != {
        identity(t) for t in (ROAD, DIESEL, ELECTRIC, BARGE)
    }:
        raise ValueError("Resolved-child market independent reverse scope changed.")
    for entry in APPROVAL["reverse_bindings"]:
        source, target = identity(entry["source"]), identity(entry["target"])
        if (
            digest(by_source.get(source)) != entry["binding_sha256"]
            or entry["source_inventory_sha256"] != proofs["uvek-2025-cutoff", source]["inventory_sha256"]
            or entry["target_inventory_sha256"] != proofs["ecoinvent-3.12-cutoff", target]["inventory_sha256"]
            or proofs["ecoinvent-3.12-cutoff", target]["reference_amount"] != 1
        ):
            raise ValueError(
                "Resolved-child market independent reverse supplier changed; repeat the scientific review."
            )


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile only complete profiles; retain the older clinker-dependent profiles as pending."""
    source = identity(rule["source"])
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        deepcopy(original_review),
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="resolved-child-market",
        mapping_method="reviewed_resolved_child_market_service",
        profile_reason="Only exact independently reviewed complete market profiles are approved; older unresolved clinker/steam boundaries remain blocked.",
    )
    needed = {e["recipe"] for recipe in decision["recipes"].values() for e in recipe["inputs"] if "recipe" in e}
    decision["recipes"].update({k: deepcopy(v) for k, v in APPROVAL["local_recipes"].items() if k in needed})
    reasons = {p["label"]: p["reason"] for p in SOURCES[source]["pending_profiles"]}
    for profile in decision["review"]["profiles"]:
        if profile["review"]["status"] == "unresolved":
            label = "-".join(profile["source_profile"][f] for f in ("family", "version", "system_model"))
            profile["review"]["reason"] = reasons[label]
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision
