"""Preserve allocated sludge treatment and gas supply in each explicit direction."""

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
REVIEW_PATH = "brightpath/data/export/uvek_sewage_digestion_review.json"
APPROVAL = json.loads((Path(__file__).resolve().parents[1] / REVIEW_PATH).read_text())
SOURCES = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["rules"]}
MARKETS = {tuple(r["source"][f] for f in FIELDS): r for r in APPROVAL["market_rules"]}
METHOD = "reviewed_sewage_digestion_reverse"
COMPONENT_METHOD = "reviewed_sewage_digestion_component_reverse"
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
EXPECTED = {
    ("treatment of sewage sludge by anaerobic digestion", product, location, "cubic meter")
    for product in ("biogas", "sewage sludge")
    for location in ("CH", "RoW")
}
if (
    set(SOURCES) != EXPECTED
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_profile_count"] != 36
    or APPROVAL["inventory_count"] != 80
    or set(MARKETS)
    != {("market for sewage sludge", "sewage sludge", location, "cubic meter") for location in ("CH", "RoW")}
    or APPROVAL["market_source_count"] != 2
    or APPROVAL["market_source_profile_count"] != 18
):
    raise ValueError("Sewage digestion review scope changed; repeat the scientific review.")
for source, row in SOURCES.items():
    if {p["label"] for p in row["profiles"]} != {f"ecoinvent-{v}-cutoff" for v in VERSIONS} or len(
        row["profiles"]
    ) != 9:
        raise ValueError("Sewage digestion exact-version coverage changed.")
    if row["kind"] != ("allocated_gas_stock" if source[1] == "biogas" else "treatment_composition"):
        raise ValueError("Sewage digestion allocation function changed.")
for row in MARKETS.values():
    if {p["label"] for p in row["profiles"]} != {f"ecoinvent-{v}-cutoff" for v in VERSIONS} or len(
        row["profiles"]
    ) != 9:
        raise ValueError("Sewage-sludge market exact-version coverage changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    EVIDENCE_SCOPE.setdefault(tuple(row["label"].split("-")), set()).add(tuple(row["source"][f] for f in FIELDS))
SERVICE_LIMITATIONS = (
    "Reviewed cut-off sewage-sludge digestion operating helper: retain the exact inspected plant, "
    "inorganic chemicals, electricity, heat and regional lubricating-oil inputs through their existing "
    "independently checked UVEK correspondences. Retain source non-fossil methane and carbon-dioxide "
    "emissions and the older carbon-dioxide resource uptake. The negative one-m3 sludge reference becomes "
    "a positive one-m3 treatment service with factor -1. Sludge volume is not gas volume: no gas-yield, "
    "absolute-value or waste-indicator mass conversion is applied. Do not add the full UVEK biogas "
    "producer, whose operating burdens would duplicate these inputs; do not invent a gas recovery credit. "
    "The separate gas branch of the same cut-off activity has no active allocated burdens. Preserve the "
    "source inventory indicator where supported; otherwise omit only that inventory indicator with an "
    "explicit warning under the standing indicator policy. Source CH and RoW electricity/supply mixtures "
    "use the inspected component proxies, including Swiss heat and some electricity substitutions. "
    "The inherited heat-commodity proxy uses heat-only production and district distribution with "
    "1.11 MJ heat input per delivered MJ, whereas the inspected Swiss source market supplies "
    "co-generated heat and treats delivery transport/losses as irrelevant. These heat allocation "
    "and delivery differences are background approximations, not reconstructed source heat supply; "
    "technology, geography, vintage, material mixtures and background LCIA results differ. Internal "
    "background parameter formulas and uncertainty are not reconstructed. Preserve caller signed "
    "quantities, uncertainty, formulas, foreground suppliers, production and separate emissions. "
    "Only the enumerated exact cut-off profiles, including independently inspected patch releases, "
    "are approved. Score and export all returned helpers; no cross-system-model route is approved."
)
GAS_LIMITATIONS = (
    "Reviewed allocated cut-off sewage-gas supply: the positive one-m3 biogas branch and negative "
    "sludge-treatment branch share the same original activity identifier in each inspected profile. "
    "Operating inputs and emissions are allocated to the sludge-treatment branch; gas has no active "
    "non-production exchanges. Preserve this observed allocation rather than adding UVEK's burdened "
    "gas-production inventory. Retain the earlier zero-valued biosphere records; later production-only "
    "profiles use an explicit allocated-stock helper. This does not make actual digestion burden-free "
    "or infer zero for a missing inventory. Preserve signed gas m3, uncertainty, formulas, foreground "
    "ownership and separately listed treatment/emissions; add no recovery credit or gas/sludge volume "
    "conversion. Only enumerated cut-off versions and inspected patches are covered."
)
REVERSE_LIMITATIONS = (
    "Independently reviewed UVEK sewage-gas operating reconstruction per one m3 gas: preserve the UVEK "
    "plant, inorganic chemicals, low-voltage electricity, industrial natural-gas heat and its own direct "
    "non-fossil CO2/methane quantities using inspected ecoinvent 3.12 cut-off components. Do not substitute "
    "ecoinvent's empty allocated gas branch or convert gas volume to sludge volume. The CH plant, GLO "
    "generic inorganic production and CH low-voltage market are function/unit proxies; industrial heat "
    "uses Europe-without-Switzerland rather than CH. Technology, supply, allocation, lifetime, mixtures "
    "and background emissions may differ; equal LCIA scores are not established. No extra wastewater "
    "treatment, digested-sludge disposal, gas credit or forward-derived inverse is added. Preserve caller "
    "signs, supported uncertainty, formulas, foreground suppliers and separate emissions, and include "
    "all generated helpers in scoring/export. Internal background uncertainty is not reconstructed."
)
MARKET_LIMITATIONS = (
    "Reviewed CH/RoW sewage-sludge market alias: all nine inspected exact cut-off profiles have a "
    "negative one-m3 sludge reference and exactly one -1 m3 input to the same region's independently "
    "reviewed full anaerobic digestion service. There are no other exchanges, including zero-valued "
    "biosphere records or freight. Reuse the exact child helper with factor -1; do not add a duplicate "
    "market helper or substitute the former Biogas, from sewage sludge, at storage product. Sludge "
    "volume is not gas volume. Retain every child operating input and direct release, including "
    "the older carbon-dioxide resource uptake and permitted indicator warnings. Add no gas production, "
    "recovery credit, wastewater treatment, collection transport, dry-mass scaling or renormalization. "
    "Incoming positive credits and zero quantities retain their signed meaning. Child geography, "
    "technology, heat allocation and vintage approximations remain explicit; LCIA equivalence is "
    "not established. Preserve caller uncertainty, formulas, metadata, foreground suppliers and "
    "separately listed emissions. Score and export the reused child helper. Source internal formulas "
    "and uncertainty are not reconstructed. Only enumerated exact cut-off profiles are approved; "
    "no version, model, gas/sludge or inverse correspondence is inferred. " + SERVICE_LIMITATIONS
)
PLANT_REVERSE_LIMITATIONS = (
    "Independently inspected full CH sewage-sludge anaerobic-digestion plant correspondence. "
    "Both inventories retain 504 kg cast iron, 1970 kg 18/8 steel, 406 m3 concrete and 24300 kg "
    "reinforcing steel, a 30-year building lifetime and 10-year stationary-machine lifetime. "
    "Preserve signed plant units with factor +1; add no capacity, sludge-volume or gas-yield scaling. "
    "UVEK explicitly includes 19200 road ton-km, 13900 rail ton-km and 893000 kg concrete disposal; "
    "ecoinvent 3.12 cut-off uses material markets and its negative waste-concrete service link. "
    "Freight, waste linking, materials, land and background differ; equal LCIA scores are not "
    "established. This represents infrastructure only, without digestion operation, gas production "
    "or recovery credits. Preserve caller signs, uncertainty, formulas, metadata, foreground "
    "suppliers and separate emissions; no inverse is inferred."
)
ELECTRICITY_REVERSE_LIMITATIONS = (
    "Independently inspected delivered low-voltage electricity proxy in the same named geography: "
    "AU, NZ and RU use their national ecoinvent 3.12 cut-off markets; RAF, RAS, RLA and RNA use "
    "the corresponding Africa, Asia, Latin America/Caribbean and Northern America market groups. "
    "Both represent delivered electricity including low-voltage network and losses, directly or "
    "through their contained markets. Preserve signed delivered kWh with factor +1, retaining "
    "the caller's original regional demand weights; add no second distribution network, SF6 "
    "emission, voltage transformation, loss multiplier or normalization. UVEK's 2023 supply, Swiss "
    "network proxy, trade assumptions, generation mix, regional aggregation, direct emissions and "
    "loss allocation differ from the target electricity system and vintage. This is a whole-market "
    "background approximation, not reconstruction of the UVEK generation mix or proof of LCIA "
    "equivalence. Preserve caller signs, uncertainty, formulas, metadata, foreground suppliers "
    "and separately listed emissions. No forward-derived inverse is inferred."
)


def identity(row):
    return tuple(row.get(f, "") for f in FIELDS)


def inventory_record(source, blobs, metadata):
    """Fingerprint allocation branches, including inactive zero biosphere records."""
    exchanges = [pickle.loads(blob) for blob in blobs]
    (reference,) = [e for e in exchanges if e["type"] == "production"]
    if source in MARKETS:
        nonproduction = [e for e in exchanges if e["type"] != "production"]
        if (
            reference["amount"] != -1
            or tuple(reference["input"]) != tuple(reference["output"])
            or len(nonproduction) != 1
            or nonproduction[0]["type"] != "technosphere"
            or nonproduction[0]["amount"] != -1
        ):
            raise ValueError("Sewage-sludge market inventory changed; repeat the scientific review.")
    return {
        "source": list(source),
        "inventory_sha256": hashlib.sha256(b"".join(sorted(blobs))).hexdigest(),
        "activity_comment_sha256": hashlib.sha256(str(metadata.get("comment") or "").encode()).hexdigest(),
        "product_information_sha256": hashlib.sha256(
            str(metadata.get("product_information") or "").encode()
        ).hexdigest(),
        "reference_amount": reference["amount"],
        "activity": metadata.get("activity"),
        "total_exchanges": len(exchanges),
        "only_self_production": len(exchanges) == 1 and reference["input"][1] == metadata["code"],
        "active_nonproduction_count": sum(e["type"] != "production" and bool(e["amount"]) for e in exchanges),
    }


def collect_extra_inventories(brightway_root):
    """Read the two exact original patch databases, never a premise scenario."""
    rows = []
    for entry in APPROVAL["extra_profiles"]:
        profile = tuple(entry["profile"])
        scope = EVIDENCE_SCOPE[profile]
        path = brightway_root / entry["project"] / "lci/databases.db"
        with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
            metadata_rows = [
                pickle.loads(raw)
                for name in sorted({source[0] for source in scope})
                for raw, in connection.execute(
                    "SELECT data FROM activitydataset WHERE database=? AND name=?", (entry["database"], name)
                )
            ]
            if {identity(m) for m in metadata_rows} != scope or len(metadata_rows) != 6:
                raise ValueError("Sewage digestion patch database scope changed; repeat the scientific review.")
            for metadata in metadata_rows:
                blobs = [
                    raw
                    for raw, in connection.execute(
                        "SELECT data FROM exchangedataset WHERE output_database=? AND output_code=?",
                        (entry["database"], metadata["code"]),
                    )
                ]
                rows.append({**inventory_record(identity(metadata), blobs, metadata), "label": "-".join(profile)})
    return rows


def validate_inventories(inventories, observations, native):
    """Require both allocation branches and all independently checked component bounds."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {
        json.dumps({**r, "source": list(source)}, sort_keys=True) for source, rows in inventories.items() for r in rows
    }
    if actual != expected or len(expected) != 80 or sum(map(len, inventories.values())) != 80:
        raise ValueError("Sewage digestion inventory or metadata changed; repeat the scientific review.")
    for source, row in SOURCES.items():
        found = {r["label"]: r for r in inventories[source]}
        expected_sign = 1 if source[1] == "biogas" else -1
        if set(found) != {p["label"] for p in row["profiles"]} or any(
            r["reference_amount"] != expected_sign for r in found.values()
        ):
            raise ValueError("Sewage digestion allocation reference changed; repeat the scientific review.")
        if source[1] == "biogas":
            sibling = (source[0], "sewage sludge", source[2], source[3])
            siblings = {r["label"]: r for r in inventories[sibling]}
            if any(
                r["active_nonproduction_count"] or not r["activity"] or r["activity"] != siblings[label]["activity"]
                for label, r in found.items()
            ):
                raise ValueError("Sewage gas allocation changed; repeat the scientific review.")
    for source, row in MARKETS.items():
        found = {r["label"]: r for r in inventories[source]}
        if set(found) != {p["label"] for p in row["profiles"]} or any(
            r["reference_amount"] != -1 or r["total_exchanges"] != 2 or r["active_nonproduction_count"] != 1
            for r in found.values()
        ):
            raise ValueError("Sewage-sludge market reference or scope changed; repeat the scientific review.")
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        observed.update((label, r["reference_amount"]) for label, r in found.items())
        if observed != {(p["label"], -1) for p in row["profiles"]}:
            raise ValueError("Sewage-sludge market profile reference changed; repeat the scientific review.")
    for source in EVIDENCE_SCOPE[("uvek", "2025", "cutoff")]:
        meta = native.get(source, {})
        if (
            observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
            or meta.get("native_flow_type") != "PRODUCT_FLOW"
            or meta.get("native_reference_is_input") is not False
        ):
            raise ValueError("Sewage digestion UVEK component role changed; repeat the scientific review.")
    for row in APPROVAL["reverse_components"]:
        for side, label, family in (
            ("source", "uvek-2025-cutoff", "uvek"),
            ("target", "ecoinvent-3.12-cutoff", "ecoinvent"),
        ):
            source = identity(row[side])
            (record,) = [r for r in inventories[source] if r["label"] == label]
            if (
                record["inventory_sha256"] != row[f"{side}_inventory_sha256"]
                or record["reference_amount"] != 1
                or (label, 1) not in observations[family].get(source, set())
            ):
                raise ValueError("Sewage reverse component boundary changed; repeat the scientific review.")


def validate_bindings(resource):
    validate_composition_bindings(resource, approval=APPROVAL, label="Sewage digestion")
    market_approval = {"rules": APPROVAL["market_rules"], "recipe_dependencies": APPROVAL["market_recipe_dependencies"]}
    validate_composition_bindings(resource, approval=market_approval, label="Sewage-sludge market")
    by_label = {}
    for row in SOURCES.values():
        if row["kind"] == "treatment_composition":
            for profile in row["profiles"]:
                by_label[profile["label"], row["source"]["location"]] = profile
    for source, row in MARKETS.items():
        for profile in row["profiles"]:
            (binding,) = profile["bindings"]
            child = tuple(binding["source"])
            if child != (
                "treatment of sewage sludge by anaerobic digestion",
                "sewage sludge",
                source[2],
                "cubic meter",
            ):
                raise ValueError("Sewage-sludge market child function changed; repeat the scientific review.")
            recipe_id = profile["target"]["recipe_ref"]
            if recipe_id not in APPROVAL["market_recipe_dependencies"]:
                raise ValueError("Sewage-sludge market child recipe is not independently bound.")
            if resource["recipes"][recipe_id]["inputs"] != by_label[profile["label"], source[2]]["inputs"]:
                raise ValueError("Sewage-sludge market child inventory changed; repeat the scientific review.")


def declare_reviewed_profiles(resource, decisions):
    """Declare only the two independently inspected patch contexts; infer no reviews."""
    if resource.get("name") != "ecoinvent-3.x-to-uvek-2025-technosphere":
        return
    if not any(
        d.get("mapping_method")
        in {"reviewed_sewage_digestion_service", "reviewed_allocated_sewage_gas", "reviewed_sewage_sludge_market_alias"}
        for d in decisions.values()
    ):
        return
    for entry in APPROVAL["extra_profiles"]:
        profile = dict(zip(("family", "version", "system_model"), entry["profile"], strict=True))
        if profile not in resource["source_profiles"]:
            resource["source_profiles"].append(profile)


def forward_decision(rule, observations, inventories, binding, original_review):
    """Compile faithful operating and observed allocated-gas profiles separately."""
    source = identity(rule["source"])
    row = SOURCES[source]
    own = {r["label"]: r for r in inventories}
    extra = {(label, r["reference_amount"]) for label, r in own.items() if label.split("-")[1] in {"3.9.1", "3.10.1"}}
    cf = {(label, sign) for label, sign in observations if label.endswith("-cutoff")} | extra
    expected = {(p["label"], p["source_sign"]) for p in row["profiles"]}
    if cf != expected or rule["target"] != row["previous_target"] or rule.get("conversion_factor", 1) != 1:
        raise ValueError("Sewage digestion source correspondence changed; repeat the scientific review.")
    if row["kind"] == "treatment_composition":
        decision = composition_decision(
            rule,
            cf,
            [(r["label"], r["inventory_sha256"]) for r in inventories],
            binding,
            original_review,
            approval_row=row,
            rationale=SERVICE_LIMITATIONS,
            review_path=REVIEW_PATH,
            namespace="sewage-digestion",
            mapping_method="reviewed_sewage_digestion_service",
            profile_reason="Select only an exact inspected cut-off sludge-treatment profile; gas supply is a separate allocated function.",
        )
    else:
        recipes, profiles = {}, []
        for p in row["profiles"]:
            if any(e["type"] != "biosphere" or e["amount"] != 0 for e in p["inputs"]):
                raise ValueError("Allocated sewage gas acquired active burdens; repeat the scientific review.")
            key = (
                "allocated-sewage-gas-"
                + hashlib.sha256(json.dumps([source, p["inputs"]], sort_keys=True).encode()).hexdigest()[:20]
            )
            recipe = {
                "name": f"Allocated sewage gas, {source[2]}",
                "reference product": "biogas",
                "location": source[2],
                "unit": "cubic meter",
                "inputs": deepcopy(p["inputs"]),
                "rationale": GAS_LIMITATIONS,
                "evidence": [REVIEW_PATH],
            }
            if not recipe["inputs"]:
                recipe.update(zero_burden=True, zero_burden_reason=GAS_LIMITATIONS)
            recipes[key] = recipe
            profiles.append(
                {
                    "source_profile": dict(
                        zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)
                    ),
                    "target": {"recipe_ref": key},
                    "conversion_factor": 1,
                    "review": {
                        "status": "verified",
                        "source_sign": 1,
                        "target_sign": 1,
                        "reason": GAS_LIMITATIONS,
                        "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
                    },
                }
            )
        decision = {
            "operation": "recipe",
            "binding": binding,
            "original_rule": deepcopy(rule),
            "original_review": deepcopy(original_review),
            "original_factor": 1,
            "factor": 1,
            "uvek_reference_sign": 1,
            "uvek_native_flow_type": "GENERATED_PRODUCT",
            "original_native_flow_type": "PRODUCT_FLOW",
            "target": deepcopy(profiles[0]["target"]),
            "recipes": recipes,
            "recipe_review": {
                "rationale": GAS_LIMITATIONS,
                "limitations": [GAS_LIMITATIONS],
                "evidence": [REVIEW_PATH],
            },
            "recipe_information_loss": GAS_LIMITATIONS,
            "mapping_method": "reviewed_allocated_sewage_gas",
            "review": {
                "status": "unresolved",
                "reason": "Only enumerated inspected cut-off allocation profiles are approved.",
                "evidence": [REVIEW_PATH],
                "profiles": profiles,
            },
        }
    decision["source_observations"] = [list(pair) for pair in sorted(set(observations) | extra)]
    return decision


def market_decision(rule, observations, inventories, binding, original_review):
    """Reuse a sole reviewed digestion child without inventing extra market burdens."""
    source = identity(rule["source"])
    row = MARKETS[source]
    extra = {(r["label"], r["reference_amount"]) for r in inventories}
    observed = {(label, sign) for label, sign in observations if label.endswith("-cutoff")} | extra
    if (
        rule["target"] != row["previous_target"]
        or rule.get("conversion_factor", 1) != 1
        or observed != {(p["label"], -1) for p in row["profiles"]}
        or sorted((r["label"], r["inventory_sha256"]) for r in inventories)
        != sorted((p["label"], p["inventory_sha256"]) for p in row["profiles"])
    ):
        raise ValueError("Sewage-sludge market correspondence changed; repeat the scientific review.")
    profiles = [
        {
            "source_profile": dict(zip(("family", "version", "system_model"), p["label"].split("-"), strict=True)),
            "target": deepcopy(p["target"]),
            "conversion_factor": -1,
            "review": {
                "status": "verified",
                "source_sign": -1,
                "target_sign": 1,
                "reason": MARKET_LIMITATIONS,
                "evidence": [f"{REVIEW_PATH}#{p['label']}/{p['inventory_sha256']}"],
            },
        }
        for p in row["profiles"]
    ]
    return {
        "operation": "recipe",
        "binding": binding,
        "original_rule": deepcopy(rule),
        "original_review": deepcopy(original_review),
        "original_factor": 1,
        "factor": -1,
        "source_observations": [list(pair) for pair in sorted(set(observations) | extra)],
        "uvek_reference_sign": 1,
        "uvek_native_flow_type": "GENERATED_SERVICE",
        "original_native_flow_type": row["previous_native_flow_type"],
        "target": deepcopy(profiles[0]["target"]),
        "recipes": {},
        "recipe_review": {
            "rationale": MARKET_LIMITATIONS,
            "limitations": [MARKET_LIMITATIONS],
            "evidence": [REVIEW_PATH],
        },
        "recipe_information_loss": MARKET_LIMITATIONS,
        "mapping_method": "reviewed_sewage_sludge_market_alias",
        "review": {
            "status": "unresolved",
            "reason": "Select only an exact inspected cut-off sewage-sludge market and its independently reviewed full digestion child.",
            "evidence": [REVIEW_PATH],
            "profiles": profiles,
        },
    }


def reverse_addition():
    """Author one native operating function, independently of forward allocation."""
    row = APPROVAL["reverse"]
    source = row["source"]
    digest = hashlib.sha256(json.dumps(identity(source)).encode()).hexdigest()[:16]
    recipe_id = "reverse-sewage-gas-" + digest
    review = {"rationale": REVERSE_LIMITATIONS, "evidence": [REVIEW_PATH]}
    reviews = {}
    reference = intern_review({"reviews": reviews}, review)
    rule = {
        "id": "uvek-ei312-" + digest,
        "source": deepcopy(source),
        "target": {"recipe_ref": recipe_id},
        "conversion_factor": 1,
        "classification": "proxy",
        "mapping_method": METHOD,
        "review_ref": reference,
    }
    recipes = {
        recipe_id: {
            **deepcopy(source),
            "name": "Sewage gas, reconstructed UVEK operating process",
            "inputs": deepcopy(row["inputs"]),
            "rationale": REVERSE_LIMITATIONS,
            "evidence": [REVIEW_PATH],
        }
    }
    return rule, recipes, reviews


def apply_reverse(resource, review, sources, targets):
    apply_component_reverse(resource, review, sources, targets)
    rule, recipes, reviews = reverse_addition()
    key = identity(rule["source"])
    if key not in sources:
        return
    if any(identity(r["source"]) == key for r in resource["replace"]):
        raise ValueError("Sewage-gas reverse scope overlaps an existing correspondence.")
    for recipe in recipes.values():
        for e in recipe["inputs"]:
            if e["type"] == "technosphere" and identity(e) not in targets:
                raise ValueError("Sewage-gas reverse component is not in the exact target catalog.")
    (entry,) = [r for r in review if identity(r["source"]) == key]
    if entry["disposition"] != "unresolved":
        raise ValueError("Sewage-gas reverse source is not the independently reviewed unresolved identity.")
    entry.clear()
    entry.update(source=deepcopy(rule["source"]), disposition="recipe", rule_id=rule["id"])
    resource["replace"].append(rule)
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    resource["recipes"].update(recipes)
    resource["reviews"].update(reviews)
    resource["coverage"].update(
        mapped_identities=resource["coverage"]["mapped_identities"] + 1,
        unresolved_identities=resource["coverage"]["unresolved_identities"] - 1,
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )


def legacy_reverse_projection(resource):
    """Protect the previous independently reviewed reverse baseline exactly."""
    resource = legacy_component_projection(resource)
    expected, recipes, reviews = reverse_addition()
    found = [r for r in resource["replace"] if r.get("mapping_method") == METHOD]
    if not found:
        return resource
    if (
        found != [expected]
        or any(resource["recipes"].get(k) != v for k, v in recipes.items())
        or any(resource["reviews"].get(k) != v for k, v in reviews.items())
    ):
        raise ValueError("Sewage-gas reverse reconstruction changed; repeat the independent review.")
    result = deepcopy(resource)
    result["replace"] = [r for r in result["replace"] if r["id"] != expected["id"]]
    result["recipes"] = {k: v for k, v in result["recipes"].items() if k not in recipes}
    result["reviews"] = {k: v for k, v in result["reviews"].items() if k not in reviews}
    result["coverage"]["mapped_identities"] -= 1
    result["coverage"]["unresolved_identities"] += 1
    result["coverage"]["dispositions"]["recipe"] -= 1
    result["coverage"]["dispositions"]["unresolved"] += 1
    return result


def component_additions():
    """Author eight positive whole-supplier rules from independently inspected pairs."""
    rules, reviews = [], {}
    expected = {
        ("Electricity, low voltage, at grid", "Electricity, low voltage, at grid", location, "kilowatt hour")
        for location in ("AU", "NZ", "RU", "RAF", "RAS", "RLA", "RNA")
    } | {("Anaerobic digestion plant, sewage sludge", "Anaerobic digestion plant, sewage sludge", "CH", "unit")}
    if (
        len(APPROVAL["reverse_components"]) != 8
        or {identity(r["source"]) for r in APPROVAL["reverse_components"]} != expected
    ):
        raise ValueError("Sewage reverse component scope changed; repeat the independent review.")
    for row in APPROVAL["reverse_components"]:
        source = identity(row["source"])
        plant = source[3] == "unit"
        if plant:
            target = (
                "anaerobic digestion plant construction, for sewage sludge",
                "anaerobic digestion plant, for sewage sludge",
                "CH",
                "unit",
            )
        else:
            name = (
                "market for electricity, low voltage"
                if source[2] in {"AU", "NZ", "RU"}
                else "market group for electricity, low voltage"
            )
            target = (name, "electricity, low voltage", source[2], "kilowatt hour")
        if (
            identity(row["target"]) != target
            or row["conversion_factor"] != 1
            or row["kind"] != ("plant" if plant else "electricity")
        ):
            raise ValueError("Sewage reverse component function or unit changed; repeat the independent review.")
        limitation = PLANT_REVERSE_LIMITATIONS if plant else ELECTRICITY_REVERSE_LIMITATIONS
        reference = intern_review({"reviews": reviews}, {"rationale": limitation, "evidence": [REVIEW_PATH]})
        suffix = hashlib.sha256(json.dumps(source).encode()).hexdigest()[:16]
        rules.append(
            {
                "id": "uvek-ei312-" + suffix,
                "source": deepcopy(row["source"]),
                "target": deepcopy(row["target"]),
                "conversion_factor": 1,
                "classification": "proxy",
                "mapping_method": COMPONENT_METHOD,
                "technosphere_information_loss": limitation,
                "review_ref": reference,
            }
        )
    return sorted(rules, key=lambda r: identity(r["source"])), reviews


def apply_component_reverse(resource, review, sources, targets):
    """Add only available independently reviewed plant and grid sources to an unresolved catalog."""
    additions, reviews = component_additions()
    rules = [r for r in additions if identity(r["source"]) in sources]
    if not rules:
        return
    existing = {identity(r["source"]) for r in resource["replace"]}
    for rule in rules:
        key = identity(rule["source"])
        if key in existing or identity(rule["target"]) not in targets:
            raise ValueError("Sewage reverse component catalog changed; repeat the independent review.")
        (entry,) = [r for r in review if identity(r["source"]) == key]
        if entry["disposition"] != "unresolved":
            raise ValueError("Sewage reverse component is not independently unresolved.")
        entry.clear()
        entry.update(source=deepcopy(rule["source"]), disposition="proxy", rule_id=rule["id"])
    resource["replace"].extend(rules)
    resource["replace"].sort(key=lambda r: identity(r["source"]))
    used = {r["review_ref"] for r in rules}
    resource["reviews"].update({k: v for k, v in reviews.items() if k in used})
    resource["coverage"].update(
        mapped_identities=resource["coverage"]["mapped_identities"] + len(rules),
        unresolved_identities=resource["coverage"]["unresolved_identities"] - len(rules),
        dispositions=dict(Counter(r["disposition"] for r in review)),
    )


def legacy_component_projection(resource):
    """Remove only this exact eight-source addition before older historical fingerprints."""
    expected, reviews = component_additions()
    found = [r for r in resource["replace"] if r.get("mapping_method") == COMPONENT_METHOD]
    if not found:
        return resource
    if found != expected or any(resource["reviews"].get(k) != v for k, v in reviews.items()):
        raise ValueError("Sewage reverse component rules changed; repeat the independent review.")
    result = deepcopy(resource)
    ids = {r["id"] for r in expected}
    result["replace"] = [r for r in result["replace"] if r["id"] not in ids]
    result["reviews"] = {k: v for k, v in result["reviews"].items() if k not in reviews}
    result["coverage"]["mapped_identities"] -= 8
    result["coverage"]["unresolved_identities"] += 8
    result["coverage"]["dispositions"]["proxy"] -= 8
    result["coverage"]["dispositions"]["unresolved"] += 8
    return result
