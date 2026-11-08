"""Review ash markets with exact child services and independently inspected freight."""

import hashlib
import json
import pickle
import sqlite3
from collections import Counter
from copy import deepcopy
from pathlib import Path

from brightpath.migrations.schema import intern_review

try:
    from review_uvek_market_compositions import composition_decision, validate_composition_bindings
except ModuleNotFoundError:
    from scripts.review_uvek_market_compositions import composition_decision, validate_composition_bindings

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_ash_market_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
VERSIONS = {"3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
METHOD = "reviewed_complete_electric_rail_reverse"


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def native_target(name, location):
    return dict(zip(FIELDS, (name, name, location, "ton kilometer"), strict=True))


ROAD = native_target("Transport, freight, lorry, fleet average", "RER")
DIESEL = native_target("Transport, freight, rail, diesel, with particle filter", "CH")
ELECTRIC = native_target("Transport, freight, rail, electricity with shunting", "CH")
REVERSE_TARGET = dict(
    zip(
        FIELDS,
        ("transport, freight, train, electric", "transport, freight, train, fleet average", "CH", "ton kilometer"),
        strict=True,
    )
)
SOURCES = {identity(row["source"]): row for row in APPROVAL["rules"]}
if (
    len(SOURCES) != 42
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 42
    or APPROVAL["source_profile_count"] != 176
    or APPROVAL["inventory_count"] != 231
    or sum(len(row["profiles"]) for row in SOURCES.values()) != 176
    or any(not s[0].startswith("market for bottom ash,") or s[3] != "kilogram" for s in SOURCES)
    or APPROVAL["reverse"]["source"] != ELECTRIC
    or APPROVAL["reverse"]["target"] != REVERSE_TARGET
):
    raise ValueError("Bottom-ash market scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed ash-market evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(identity(row["source"]))

LIMITATIONS = (
    "Reviewed bottom-ash market helper per one kg ash. Preserve the exact inspected negative "
    "ash-treatment demand through its independently reviewed child service, including waste-specific "
    "releases, facility and operating burdens. The market reference is -1 kg and the helper reference "
    "+1 kg: apply factor -1 to caller demand; do not use absolute values. Retain every original road "
    "and rail freight quantity in ton-km without kg conversion, scaling or renormalization. Incoming "
    "ash freight is distinct from the child facility's construction-material freight. Source markets "
    "contain no biosphere exchanges; do not add original-waste incineration, upstream wastewater "
    "treatment, recovery credits or other disposal fates. Scoped freight proxies use the inspected "
    "complete RER UVEK diesel lorry fleet average, whose class weights use Swiss 2020 shares, and the "
    "complete CH electric-with-shunting and diesel-with-particle-filter rail services at the exact "
    "source market shares. These complete rail cycles include rolling stock, maintenance, disposal "
    "and track infrastructure; add no second operation or infrastructure. This review does not "
    "rewrite generic freight mappings. Geography, vehicle classes, load, EURO standard, duty, vintage, "
    "energy use, assets and background differ; LCIA equivalence is not established. Native UVEK water "
    "proxies and permitted indicator omissions retain the child review's explicit warnings. Preserve "
    "caller signed quantities, uncertainty, formulas, metadata, foreground suppliers, production and "
    "separately listed emissions. Internal source-background uncertainty and formulas are not "
    "reconstructed. Score and export all returned helpers. Only enumerated exact cut-off profiles "
    "are approved; no patch, model or inverse fallback is inferred."
)
REVERSE_LIMITATIONS = (
    "Independently inspected full CH electric rail-freight service proxy, including shunting, "
    "rolling stock, maintenance, disposal and track infrastructure in both UVEK and ecoinvent 3.12 "
    "cut-off. Preserve signed ton-km with factor +1. UVEK uses 0.00046413 kg shunting diesel and "
    "0.039209 kWh per ton-km; the inspected ecoinvent service uses 0.00068 kg and 0.086124 kWh. "
    "Retain the target's full service without extra operation, infrastructure, energy scaling or "
    "a lorry substitute. Energy, vintage, asset and background differences are explicit information "
    "loss; equal LCIA scores are not established. Preserve caller uncertainty, formulas, metadata, "
    "foreground suppliers and separate emissions. This independently reviewed positive product "
    "correspondence does not infer an inverse for the ash-market or ash-treatment helper."
)


def inventory_record(source, blobs, comment, product_information):
    """Bind exact inventories, boundaries and unit-magnitude reference amounts."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] != (-1 if source in SOURCES else 1):
        raise ValueError("Ash-market evidence reference changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(comment or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(product_information or "").encode()).hexdigest(),
        "reference_amount": reference["amount"],
    }


def collect_extra_inventories(brightway_root):
    """Inspect exact original patch inventories through read-only SQLite connections."""
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
                        raw
                        for raw, in connection.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], metadata["code"]),
                        )
                    ]
                    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
                    if tuple(reference["input"]) != (entry["database"], metadata["code"]):
                        raise ValueError("Ash-market patch reference is not self production.")
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
                raise ValueError("Ash-market patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Require all 231 independent source, freight, traction and native fingerprints."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 231 or sum(map(len, inventories.values())) != 231:
        raise ValueError("Ash-market inventory or boundary metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        if {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        } != {(p["label"], -1) for p in row["profiles"]}:
            raise ValueError("Ash-market exact reference or profile changed; repeat the scientific review.")
    for profile, sources in EVIDENCE_SCOPE.items():
        for source in sources:
            if ("-".join(profile), -1 if source in SOURCES else 1) not in observations[profile[0]].get(source, set()):
                raise ValueError("Ash-market evidence reference changed; repeat the scientific review.")
            if profile[0] == "uvek":
                metadata = native.get(source, {})
                if (
                    metadata.get("native_flow_type") != "PRODUCT_FLOW"
                    or metadata.get("native_reference_is_input") is not False
                ):
                    raise ValueError("Ash-market native transport role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze child ash recipes and the exact helper-specific full-service freight choices."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Ash-market")
    local = APPROVAL["local_recipes"]
    if len(local) != 1:
        raise ValueError("Ash-market rail recipe scope changed.")
    ((rail_id, rail),) = local.items()
    if (
        rail["unit"] != "ton kilometer"
        or rail["location"] != "CH"
        or rail["inputs"]
        != [
            {**DIESEL, "type": "technosphere", "amount": 0.0412324596234946},
            {**ELECTRIC, "type": "technosphere", "amount": 0.958767540376505},
        ]
    ):
        raise ValueError("Ash-market full rail-service mix changed; repeat the scientific review.")
    choices = {(r["label"], identity(r["source"])): r for r in APPROVAL["freight_choices"]}
    if len(choices) != 36:
        raise ValueError("Ash-market scoped freight choices changed.")
    proofs = {(r["label"], identity(r["source"])): r for r in APPROVAL["inventories"]}
    used = set()
    for row in SOURCES.values():
        for profile in row["profiles"]:
            expected_inputs = []
            for entry in profile["freight_choices"]:
                key = profile["label"], tuple(entry["source"])
                choice = choices[key]
                if (
                    digest(choice) != entry["choice_sha256"]
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                ):
                    raise ValueError("Ash-market scoped freight evidence changed.")
                if choice["conversion_factor"] != 1 or entry["amount"] <= 0:
                    raise ValueError("Ash-market freight quantity changed.")
                if choice["kind"] == "road" and "lorry" in key[1][0] and choice["target"] == ROAD:
                    expected_inputs.append({**ROAD, "type": "technosphere", "amount": entry["amount"]})
                elif choice["kind"] == "rail" and "train" in key[1][0] and choice["target"] == {"recipe_ref": rail_id}:
                    expected_inputs.append({"recipe": rail_id, "amount": entry["amount"]})
                else:
                    raise ValueError("Unreviewed ash-market freight service.")
                used.add(key)
            actual_freight = [
                e for e in profile["inputs"] if e.get("unit") == "ton kilometer" or e.get("recipe") == rail_id
            ]
            if actual_freight != expected_inputs or any(e.get("type") == "biosphere" for e in profile["inputs"]):
                raise ValueError("Ash-market freight quantities or direct emissions changed.")
    if used != set(choices):
        raise ValueError("Ash-market freight choice scope changed.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compose exact negative-reference ash markets without changing freight or child demand."""
    source = identity(rule["source"])
    decision = composition_decision(
        rule,
        {(label, sign) for label, sign in observations if label.endswith("-cutoff")},
        [(r["label"], r["inventory_sha256"]) for r in inventories],
        binding,
        original_review,
        approval_row=SOURCES[source],
        rationale=LIMITATIONS,
        review_path=REVIEW_PATH,
        namespace="bottom-ash-market",
        mapping_method="reviewed_bottom_ash_market_service",
        profile_reason="Select only the enumerated independently inspected cut-off ash-market profiles.",
    )
    needed = {e["recipe"] for recipe in decision["recipes"].values() for e in recipe["inputs"] if "recipe" in e}
    decision["recipes"].update({k: deepcopy(v) for k, v in APPROVAL["local_recipes"].items() if k in needed})
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision


def reverse_addition():
    """Author one independent positive-product freight correspondence."""
    suffix = hashlib.sha256(json.dumps(identity(ELECTRIC)).encode()).hexdigest()[:16]
    reviews = {}
    reference = intern_review({"reviews": reviews}, {"rationale": REVERSE_LIMITATIONS, "evidence": [REVIEW_PATH]})
    return {
        "id": "uvek-ei312-" + suffix,
        "source": deepcopy(ELECTRIC),
        "target": deepcopy(REVERSE_TARGET),
        "conversion_factor": 1,
        "classification": "proxy",
        "mapping_method": METHOD,
        "technosphere_information_loss": REVERSE_LIMITATIONS,
        "review_ref": reference,
    }, reviews


def apply_reverse(resource, review, sources, targets):
    rule, reviews = reverse_addition()
    key = identity(rule["source"])
    if key not in sources:
        return
    if identity(rule["target"]) not in targets or any(identity(r["source"]) == key for r in resource["replace"]):
        raise ValueError("Complete electric rail reverse scope changed; repeat the independent review.")
    (entry,) = [r for r in review if identity(r["source"]) == key]
    if entry["disposition"] != "unresolved":
        raise ValueError("Complete electric rail reverse source is not unresolved.")
    entry.clear()
    entry.update(source=deepcopy(rule["source"]), disposition="proxy", rule_id=rule["id"])
    resource["replace"].append(rule)
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["reviews"].update(reviews)
    resource["coverage"].update(
        mapped_identities=resource["coverage"]["mapped_identities"] + 1,
        unresolved_identities=resource["coverage"]["unresolved_identities"] - 1,
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )


def legacy_reverse_projection(resource):
    """Remove only this exact independently reviewed addition for historical guard comparisons."""
    expected, reviews = reverse_addition()
    found = [r for r in resource["replace"] if r.get("mapping_method") == METHOD]
    if not found:
        return resource
    if found != [expected] or any(resource["reviews"].get(k) != v for k, v in reviews.items()):
        raise ValueError("Complete electric rail reverse mapping changed; repeat the independent review.")
    result = deepcopy(resource)
    result["replace"] = [r for r in result["replace"] if r["id"] != expected["id"]]
    result["reviews"] = {k: v for k, v in result["reviews"].items() if k not in reviews}
    result["coverage"]["mapped_identities"] -= 1
    result["coverage"]["unresolved_identities"] += 1
    result["coverage"]["dispositions"]["proxy"] -= 1
    result["coverage"]["dispositions"]["unresolved"] += 1
    return result
