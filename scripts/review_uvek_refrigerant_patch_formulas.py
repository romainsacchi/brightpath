"""Retain exact patch inventories, local parameters and signed regional formulas."""

import hashlib
import json
import pickle
import sqlite3
from copy import deepcopy
from pathlib import Path

FIELDS = ("name", "reference product", "location", "unit")
ROOT = Path(__file__).resolve().parents[1]
REVIEW_PATH = "brightpath/data/export/uvek_refrigerant_patch_formula_review.json"
REVIEW_SHA256 = "aa887ea77656ff5952231b116947924746282459824b8918eb2c52a3bc385bd6"
if hashlib.sha256((ROOT / REVIEW_PATH).read_bytes()).hexdigest() != REVIEW_SHA256:
    raise ValueError("Parameterized refrigerant review changed; repeat the scientific review.")
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
LIMITATIONS = APPROVAL["limitations"]
MARKER = "refrigerant_patch_extension"


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def base_decision(row, prior):
    base = deepcopy(prior.get(MARKER, {}).get("base_decision", prior))
    if digest(base) != row["prior_decision_sha256"]:
        raise ValueError("Existing independent profile decisions changed; repeat the refrigerant review.")
    return base


def profile_review(profile):
    return {
        "status": "verified",
        "source_sign": -1,
        "target_sign": 1,
        "reason": LIMITATIONS,
        "evidence": [f"{REVIEW_PATH}#{profile['label']}/{profile['inventory_sha256']}"],
    }


def make_decision(row, prior, rule=None, binding=None, original_review=None):
    """Extend explicit profiles without changing independently reviewed old profiles."""
    base = base_decision(row, prior)
    if row["mode"] == "augment":
        decision = deepcopy(base)
        old_profiles = decision["review"]["profiles"]
    else:
        rule = deepcopy(rule if rule is not None else row["base_original_rule"])
        decision = {
            "operation": "recipe",
            "original_rule": rule,
            "original_review": deepcopy(original_review),
            "original_factor": row["original_factor"],
            "factor": -1,
            "binding": binding or digest([rule["source"], rule["target"]]),
            "source_observations": deepcopy(base["source_observations"]),
            "uvek_reference_sign": 1,
            "uvek_native_flow_type": "GENERATED_SERVICE",
            "original_native_flow_type": row["previous_native_flow_type"],
            "target": {"recipe_ref": row["profiles"][0]["recipe_id"]},
            "recipes": {},
            "recipe_review": {"rationale": LIMITATIONS, "limitations": [LIMITATIONS], "evidence": [REVIEW_PATH]},
            "recipe_information_loss": LIMITATIONS,
            "mapping_method": "reviewed_refrigerant_formula_inventory",
            "review": {
                "status": "unresolved",
                "reason": "Select an exact inspected cut-off source profile; no missing-profile or cross-model fallback.",
                "evidence": [REVIEW_PATH],
                "profiles": [],
            },
        }
        old_profiles = decision["review"]["profiles"]
    old_keys = {tuple(p["source_profile"][f] for f in ("family", "version", "system_model")) for p in old_profiles}
    for p in row["profiles"]:
        profile = dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True))
        if tuple(profile[f] for f in ("family", "version", "system_model")) in old_keys:
            raise ValueError("A newly reviewed refrigerant profile would replace an old decision.")
        old_profiles.append(
            {
                "source_profile": profile,
                "target": {"recipe_ref": p["recipe_id"]},
                "conversion_factor": -1,
                "review": profile_review(p),
            }
        )
        if p["recipe_id"] in decision["recipes"]:
            raise ValueError("New refrigerant recipe collides with an existing helper.")
        decision["recipes"][p["recipe_id"]] = deepcopy(p["recipe"])
        observation = [p["label"], -1]
        if observation not in decision["source_observations"]:
            decision["source_observations"].append(observation)
    decision["source_observations"].sort()
    decision[MARKER] = {"review": REVIEW_PATH, "base_decision": base}
    return decision


def forward_decision(rule, observations, binding, original_review, prior):
    row = SOURCES[identity(rule["source"])]
    if rule["target"] != row["previous_target"] or rule.get("conversion_factor", 1) != row["original_factor"]:
        raise ValueError("Original refrigerant source, target or coordinate changed.")
    base = base_decision(row, prior)
    expected = {tuple(x) for x in base["source_observations"]} | {(p["label"], -1) for p in row["profiles"]}
    if set(observations) != expected:
        raise ValueError("Refrigerant exact source observation scope changed.")
    return make_decision(row, prior, rule, binding, original_review)


def inventory_record(label, md, blobs):
    es = [pickle.loads(b) for b in blobs]
    (ref,) = [e for e in es if e["type"] == "production"]
    if ref["input"] != ref["output"] or ref["amount"] not in (-1, 1):
        raise ValueError("Refrigerant/provider reference changed; repeat its inventory review.")
    return {
        "label": label,
        "source": {f: md[f] for f in FIELDS},
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(md.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(str(md.get("product_information") or "").encode()).hexdigest(),
        "reference_amount": ref["amount"],
        "total_exchanges": len(es),
        "formula_sha256": digest(sorted(e.get("formula") or "" for e in es)),
        "parameters_sha256": digest(md.get("parameters") or []),
    }


def collect_inventories(brightway_root):
    records = []
    for entry in APPROVAL["collection_profiles"]:
        label = "-".join(entry["profile"])
        wanted = {identity(r["source"]) for r in APPROVAL["inventories"] if r["label"] == label}
        found = set()
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as con:
            for name in sorted({s[0] for s in wanted}):
                for (raw,) in con.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                ):
                    md = pickle.loads(raw)
                    if identity(md) not in wanted:
                        continue
                    blobs = [
                        b
                        for b, in con.execute(
                            "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                            (entry["database"], md["code"]),
                        )
                    ]
                    records.append(inventory_record(label, md, blobs))
                    found.add(identity(md))
        if found != wanted:
            raise ValueError("Refrigerant/provider exact inventory scope changed.")
    return records


def extend_observations(records, observations):
    for row in records:
        source = identity(row["source"])
        if source in SOURCES:
            observations["ecoinvent"][source].add((row["label"], row["reference_amount"]))


def validate_inventories(records):
    if sorted(map(digest, records)) != sorted(map(digest, APPROVAL["inventories"])):
        raise ValueError("Refrigerant/provider complete inventory or metadata changed.")
    for path, expected in APPROVAL["canonical_identity_dependency_sha256"].items():
        if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != expected:
            raise ValueError("Canonical physical flow identity changed.")
    reverse = APPROVAL["independent_reverse"]
    if hashlib.sha256((ROOT / reverse["resource_path"]).read_bytes()).hexdigest() != reverse["resource_sha256"]:
        raise ValueError("Independent reverse resource changed.")


def validate_bindings(resource, prior_decisions):
    from brightpath import TechnosphereProfile
    from brightpath.background.reference_signs import select_profile_sign_rules
    from scripts.review_uvek_venting import compiled_venting_rule

    selected = {}
    previews = {}
    for source, row in SOURCES.items():
        decision = make_decision(row, prior_decisions[row["rule_id"]])
        previews[source] = compiled_venting_rule(decision)
    for row in SOURCES.values():
        for profile in row["profiles"]:
            label = profile["label"]
            context = TechnosphereProfile(*label.split("-"))
            if label not in selected:
                selected[label] = {
                    identity(r["source"]): r for r in select_profile_sign_rules(resource, context)["replace"]
                }
            for binding in profile["component_bindings"]:
                source = identity(binding["source"])
                actual = digest(selected[label].get(source))
                if binding["kind"] == "existing":
                    expected = {binding["binding_sha256"]}
                else:
                    (projected,) = select_profile_sign_rules({"replace": [previews[source]]}, context)["replace"]
                    expected = {binding["baseline_binding_sha256"], digest(projected)}
                if actual not in expected:
                    raise ValueError("Refrigerant component correspondence changed; repeat the review.")
