"""Retain residual-landfill releases and stabilization within exact reviewed profiles."""

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
REVIEW_PATH = "brightpath/data/export/uvek_residual_landfill_review.json"
APPROVAL = json.loads((ROOT / REVIEW_PATH).read_text())
SOURCES = {tuple(row["source"][f] for f in FIELDS): row for row in APPROVAL["rules"]}
VERSIONS = {"3.6", "3.7", "3.8", "3.9", "3.9.1", "3.10", "3.10.1", "3.11", "3.12"}
if (
    len(SOURCES) != 25
    or APPROVAL["authorized_system_model"] != "cutoff"
    or APPROVAL["source_count"] != 25
    or APPROVAL["source_profile_count"] != 142
    or APPROVAL["approved_source_profile_count"] != 142
    or APPROVAL["pending_source_profile_count"] != 0
    or APPROVAL["inventory_count"] != 625
    or sum(len(row["profiles"]) for row in SOURCES.values()) != 142
    or sum(len(row["pending_profiles"]) for row in SOURCES.values()) != 0
    or sum(not row["pending_profiles"] for row in SOURCES.values()) != 25
    or any(
        not source[0].startswith("treatment of ")
        or not source[0].endswith("residual material landfill")
        or source[3] != "kilogram"
        for source in SOURCES
    )
):
    raise ValueError("Residual-landfill review scope changed; repeat the scientific review.")
EVIDENCE_SCOPE = {}
for row in APPROVAL["inventories"]:
    profile = tuple(row["label"].split("-"))
    if profile not in {("ecoinvent", v, "cutoff") for v in VERSIONS} | {("uvek", "2025", "cutoff")}:
        raise ValueError("Unreviewed residual-landfill evidence profile.")
    EVIDENCE_SCOPE.setdefault(profile, set()).add(tuple(row["source"][f] for f in FIELDS))

LIMITATIONS = (
    "Reviewed exact-profile residual-material-landfill helper per one kg source waste. Retain every "
    "inspected waste-specific release, reported stabilization-cement quantity and separate hydrated-"
    "cement disposal demand. Preserve original regional weights and facility coefficients without "
    "renormalization, moisture correction or capacity scaling. Map positive operating kg to the "
    "independently inspected complete Swiss residual-landfill operation and positive facility units "
    "to its complete 300000 m3 facility at 1600 kg/m3. Construction, use and aftercare have the "
    "inspected 5/30/40-year source phases. These components are separate from direct waste-specific "
    "leachate releases and from stabilization; add no second operating or construction burden. "
    "Source Swiss operation uses 0.027 MJ diesel, 0.0000563 kWh electricity and 0.0056776 MJ other "
    "heat per kg. UVEK uses the matching diesel/electricity quantities, 0.00604 MJ oil heat and an "
    "additional 0.000202 MJ waste-heat release. Its facility uses bitumen rather than source pitch, "
    "a different concrete supply and explicit construction freight; Swiss supply replaces other "
    "regional energy/material mixtures. These are disclosed whole-component background proxies, "
    "not separate pitch, concrete or heat correspondences. Cement uses the inspected complete "
    "Swiss unspecified-cement product; preserve incoming kg while disclosing blend, geography, "
    "market/freight and vintage differences. Hydrated-cement waste uses its dedicated Swiss service "
    "with the independently verified negative-source/positive-target factor -1; this is disposal "
    "demand rather than a cement-recovery credit. Preserve source mass without inferring water or "
    "dry-mass equivalence to the native 0%-water label. Supplier freight, land, energy, treatment "
    "boundaries and LCIA scores differ; equivalence is not established. Source references are -1 kg "
    "except the two active blast-furnace-slag 3.9 cut-off profiles, whose +1 references and burdens "
    "are retained with factor +1. No empty-stock inference, absolute value or blanket sign inversion "
    "is authorized. Real releases are retained; the explicitly marked hazardous/non-hazardous waste, "
    "total-waste-mass and organic-carbon inventory indicators remain where supported, otherwise "
    "only those indicators may be omitted with warnings under the standing policy. Exact published "
    "flow UUIDs normalize older names to canonical ecoinvent labels with unchanged unit/compartment; "
    "original labels and CAS identifiers remain audited. Approved native water proxies retain their "
    "chemical/receiving-compartment warnings. Preserve caller signs, uncertainty, formulas, metadata, "
    "foreground suppliers, production and separately listed emissions. Source internal uncertainty "
    "and formulas are not reconstructed. Score/export all returned helpers. No upstream incineration, "
    "wastewater treatment, collection freight, recovered product or substitution credit is added. "
    "Expanded modern profiles retain their own construction and operation inputs rather than "
    "inheriting older aggregate component totals. Scoped supplier corrections use refinery bitumen "
    "for the construction pitch commodity, normal ready-mix concrete for 25 MPa concrete, full PVC "
    "material from regional storage rather than polymerization-only service, and the existing curated "
    "RER heavy-fuel-oil industrial-heat proxy for the qualified non-natural-gas heat commodity. Pitch "
    "feedstock/allocation, concrete strength and mix, PVC technology and delivery, heat fuel/production "
    "mix and regional background differ; these are screening proxies, not reconstructed market mixes. "
    "Separate pipe extrusion remains separate; no monomer input or second polymerization is added. "
    "The user approved native-only residual-landfill classification for 36 generic from-dump "
    "transformation records: preserve the signed 5.7143e-5 m2/kg and the generic canonical label, "
    "without adopting the native operating inventory's 6.25e-5 m2/kg or inferring occupation-years. "
    "Every applied native refinement has its own warning. Other dump classes and arbitrary caller "
    "flows are not reclassified. No version, patch, system-model or inverse fallback is inferred."
)


def identity(row):
    return tuple(row[f] for f in FIELDS)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def inventory_record(source, blobs, comment, product_information):
    """Pin actual vectors, descriptions and reference magnitudes without source inventories."""
    (reference,) = [e for e in map(pickle.loads, blobs) if e["type"] == "production"]
    if reference["amount"] not in (-1, 1) or tuple(reference["input"]) != tuple(reference["output"]):
        raise ValueError("Residual-landfill evidence reference changed; repeat the scientific review.")
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
                raise ValueError("Residual-landfill patch scope changed; repeat the scientific review.")
    return records


def validate_inventories(inventories, observations, native):
    """Keep all 142 source profiles and all construction, operating and stabilizer evidence exact."""
    expected = {
        json.dumps({**r, "source": list(identity(r["source"]))}, sort_keys=True) for r in APPROVAL["inventories"]
    }
    actual = {json.dumps({**r, "source": list(s)}, sort_keys=True) for s, rows in inventories.items() for r in rows}
    if actual != expected or len(expected) != 625 or sum(map(len, inventories.values())) != 625:
        raise ValueError("Residual-landfill inventory or metadata changed; repeat the scientific review.")
    for row in APPROVAL["reverse_components"]:
        for side, label in (("source", "uvek-2025-cutoff"), ("target", "ecoinvent-3.12-cutoff")):
            source = identity(row[side])
            (record,) = [r for r in inventories[source] if r["label"] == label]
            if record["reference_amount"] != 1 or record["inventory_sha256"] != row[f"{side}_inventory_sha256"]:
                raise ValueError("Residual-landfill reverse inventory changed; repeat the independent review.")
    for source, row in SOURCES.items():
        all_profiles = [*row["profiles"], *row["pending_profiles"]]
        wanted = {(p["label"], p["source_sign"]) for p in all_profiles}
        observed = {
            (label, sign) for label, sign in observations["ecoinvent"].get(source, set()) if label.endswith("-cutoff")
        }
        if observed != wanted or {(r["label"], r["reference_amount"]) for r in inventories[source]} != wanted:
            raise ValueError("Residual-landfill exact source reference changed; repeat the scientific review.")
    if len(APPROVAL["native_roles"]) != 36:
        raise ValueError("Residual-landfill native supplier scope changed.")
    for row in APPROVAL["native_roles"]:
        source = identity(row["source"])
        metadata = native.get(source, {})
        if (
            metadata.get("native_flow_type") != row["native_flow_type"]
            or metadata.get("native_reference_is_input") is not row["native_reference_is_input"]
            or observations["uvek"].get(source) != {("uvek-2025-cutoff", 1)}
        ):
            raise ValueError("Residual-landfill native supplier role changed; repeat the scientific review.")


def validate_bindings(resource):
    """Freeze the used directional suppliers and the approved native water correspondences."""
    validate_composition_bindings(resource, approval=APPROVAL, label="Residual-landfill")
    native = json.loads(
        (ROOT / "brightpath/data/migrations/uvek/ecoinvent-3.10-biosphere-to-uvek-2026-biosphere.json").read_text()
    )

    def bio_key(row):
        return row["name"], tuple(row["categories"]), row["unit"]

    rules = {bio_key(r["source"]): r for r in native["replace"]}
    for entry in APPROVAL["biosphere_bindings"]:
        if digest(rules.get(bio_key(entry["source"]))) != entry["binding_sha256"]:
            raise ValueError("Residual-landfill native biosphere binding changed; repeat the scientific review.")
    validate_scoped_choices()
    catalog = json.loads((ROOT / "brightpath/data/export/reference_catalogs/ecoinvent__3.10__cutoff.json").read_text())[
        "biosphere"
    ]
    by_uuid = {r["flow_id"]: r for r in catalog}
    for alias in APPROVAL["biosphere_aliases"]:
        row = by_uuid.get(alias["target_uuid"], {})
        if (
            alias["source_uuid"] != alias["target_uuid"]
            or alias["conversion_factor"] != 1
            or alias["source"]["categories"] != alias["target"]["categories"]
            or alias["source"]["unit"] != alias["target"]["unit"]
            or not row
            or bio_key(row) != bio_key(alias["target"])
        ):
            raise ValueError("Residual-landfill biosphere UUID correspondence changed; repeat the scientific review.")


def forward_decision(rule, observations, inventories, binding, original_review):
    """Represent complete reviewed profiles and retain every unresolved modern profile explicitly."""
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
        namespace="residual-landfill",
        mapping_method="reviewed_residual_landfill_service",
        profile_reason="Only the enumerated independently reviewed residual-landfill profiles are approved; no other source, version, patch or model inherits approval.",
    )
    reasons = {p["label"]: p["reason"] for p in SOURCES[source]["pending_profiles"]}
    for profile in decision["review"]["profiles"]:
        if profile["review"]["status"] == "unresolved":
            label = "-".join(profile["source_profile"][f] for f in ("family", "version", "system_model"))
            profile["review"]["reason"] = reasons[label]
    decision["source_observations"] = [list(pair) for pair in sorted(observations)]
    return decision


def validate_scoped_choices():
    """Require the exact reviewed full-material suppliers and the user-approved native land context."""
    proofs = {(row["label"], identity(row["source"])): row for row in APPROVAL["inventories"]}
    choices = {(row["label"], identity(row["source"])): row for row in APPROVAL["supplier_choices"]}
    if len(choices) != 80:
        raise ValueError("Residual-landfill scoped supplier count changed; repeat the scientific review.")
    expected_names = {
        "pitch": {"market for pitch"},
        "concrete": {"market for concrete, 25MPa"},
        "pvc": {"market for polyvinyl chloride, unspecified polymerisation, weighted average"},
        "heat": {
            "market for heat, district or industrial, other than natural gas",
            "market group for heat, district or industrial, other than natural gas",
        },
    }
    used = set()
    for row in SOURCES.values():
        for profile in row["profiles"]:
            for binding in profile["supplier_choices"]:
                key = profile["label"], tuple(binding["source"])
                choice = choices[key]
                kind, source = choice["kind"], choice["source"]
                if source["name"] not in expected_names.get(kind, set()) or digest(choice) != binding["choice_sha256"]:
                    raise ValueError("Residual-landfill scoped supplier choice changed; repeat the scientific review.")
                name, location = {
                    "pitch": ("Bitumen, at refinery", "CH" if source["location"] == "CH" else "RER"),
                    "concrete": ("Concrete, normal, at plant", "CH"),
                    "pvc": ("Polyvinylchloride, at regional storage", "RER"),
                    "heat": ("Heat, heavy fuel oil, at industrial furnace 1MW", "RER"),
                }[kind]
                expected = dict(zip(FIELDS, (name, name, location, source["unit"]), strict=True))
                if (
                    choice["target"] != expected
                    or choice["conversion_factor"] != 1
                    or choice["source_inventory_sha256"] != proofs[key]["inventory_sha256"]
                    or choice["target_inventory_sha256"]
                    != proofs["uvek-2025-cutoff", identity(expected)]["inventory_sha256"]
                ):
                    raise ValueError(
                        "Residual-landfill supplier function, unit or inventory changed; repeat the scientific review."
                    )
                used.add(key)
    if used != set(choices):
        raise ValueError("Residual-landfill unused or missing supplier choice.")
    land = APPROVAL["native_land_context"]
    if (
        land["approval"] != "Approve scoped refinement with warnings"
        or land["approval_question_id"] != "call_95f827d200cf4991a94f64b4cfe56e9c"
    ):
        raise ValueError("Residual-landfill native land refinement is not approved.")
    expected_target = {
        "name": "Transformation, from dump site, residual material landfill",
        "categories": ["resources", "land"],
        "unit": "square meter",
    }
    expected_source = {
        "name": "Transformation, from dump site",
        "categories": ["natural resource", "land"],
        "unit": "square meter",
    }
    if (
        any(land["source"].get(k) != v for k, v in expected_source.items())
        or land["target"] != expected_target
        or land["target_profile"] != {"family": "uvek", "version": "2026"}
        or land["conversion_factor"] != 1
    ):
        raise ValueError("Residual-landfill approved land class, unit or target profile changed.")
    expected_cases = {(r["label"], identity(r["source"])) for r in land["cases"]}
    actual_cases = set()
    for source, row in SOURCES.items():
        for profile in row["profiles"]:
            for exchange in profile["inputs"]:
                if "biosphere_targets" not in exchange:
                    continue
                if {k: exchange[k] for k in expected_source} != expected_source or exchange["amount"] != 5.7143e-5:
                    raise ValueError(
                        "Residual-landfill source transformation or area changed; repeat the scientific review."
                    )
                (binding,) = exchange["biosphere_targets"]
                if (
                    binding["profile"] != land["target_profile"]
                    or binding["target"] != land["target"]
                    or not binding["information_loss"]
                ):
                    raise ValueError("Residual-landfill native land scope changed; repeat the scientific review.")
                actual_cases.add((profile["label"], source))
    if actual_cases != expected_cases or len(actual_cases) != 36 or len({s for _, s in actual_cases}) != 9:
        raise ValueError("Residual-landfill contextual land cases changed; repeat the scientific review.")


COMPONENT_METHOD = "reviewed_residual_landfill_component_reverse"


def component_additions():
    """Author seven positive whole-supplier rules from independently inspected inventories."""
    targets = {
        ("Excavation, hydraulic digger, average", "CH"): (
            "excavation, hydraulic digger",
            "excavation, hydraulic digger",
            "RER",
            "cubic meter",
        ),
        ("Gravel, round, at mine", "CH"): ("gravel and sand quarry operation", "gravel, round", "CH", "kilogram"),
        ("Sand, at mine", "CH"): ("gravel and sand quarry operation", "sand", "CH", "kilogram"),
        ("Bitumen, at refinery", "CH"): ("petroleum refinery operation", "pitch", "CH", "kilogram"),
        ("Bitumen, at refinery", "RER"): (
            "pitch production, petroleum refinery operation",
            "pitch",
            "Europe without Switzerland",
            "kilogram",
        ),
        ("Polyvinylchloride, at regional storage", "RER"): (
            "market for polyvinyl chloride, unspecified polymerisation, weighted average",
            "polyvinyl chloride, unspecified polymerisation, weighted average",
            "RER",
            "kilogram",
        ),
        ("Tap water, at user", "CH"): ("market for tap water", "tap water", "CH", "kilogram"),
    }
    rows = APPROVAL["reverse_components"]
    if len(rows) != 7 or {(r["source"]["name"], r["source"]["location"]) for r in rows} != targets.keys():
        raise ValueError("Residual-landfill reverse component scope changed; repeat the independent review.")
    rules, reviews = [], {}
    for row in rows:
        source = identity(row["source"])
        target = targets[source[0], source[2]]
        if (
            identity(row["target"]) != target
            or source[0] != source[1]
            or source[3] != target[3]
            or row["conversion_factor"] != 1
        ):
            raise ValueError(
                "Residual-landfill reverse component function or unit changed; repeat the independent review."
            )
        reference = intern_review({"reviews": reviews}, {"rationale": row["limitations"], "evidence": [REVIEW_PATH]})
        suffix = hashlib.sha256(json.dumps(source).encode()).hexdigest()[:16]
        rules.append(
            {
                "id": "uvek-ei312-" + suffix,
                "source": deepcopy(row["source"]),
                "target": deepcopy(row["target"]),
                "conversion_factor": 1,
                "classification": "proxy",
                "mapping_method": COMPONENT_METHOD,
                "technosphere_information_loss": row["limitations"],
                "review_ref": reference,
            }
        )
    return sorted(rules, key=lambda r: identity(r["source"])), reviews


def apply_component_reverse(resource, review, sources, targets):
    """Add only available independently reviewed construction/material sources to an unresolved catalog."""
    additions, reviews = component_additions()
    rules = [r for r in additions if identity(r["source"]) in sources]
    if not rules:
        return
    existing = {identity(r["source"]) for r in resource["replace"]}
    for rule in rules:
        key = identity(rule["source"])
        if key in existing or identity(rule["target"]) not in targets:
            raise ValueError("Residual-landfill reverse component catalog changed; repeat the independent review.")
        (entry,) = [r for r in review if identity(r["source"]) == key]
        if entry["disposition"] != "unresolved":
            raise ValueError("Residual-landfill reverse component is not independently unresolved.")
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
    """Remove only this exact seven-source addition before older historical fingerprints."""
    expected, reviews = component_additions()
    found = [r for r in resource["replace"] if r.get("mapping_method") == COMPONENT_METHOD]
    if not found:
        return resource
    if found != expected or any(resource["reviews"].get(k) != v for k, v in reviews.items()):
        raise ValueError("Residual-landfill reverse component rules changed; repeat the independent review.")
    result = deepcopy(resource)
    ids = {r["id"] for r in expected}
    result["replace"] = [r for r in result["replace"] if r["id"] not in ids]
    result["reviews"] = {k: v for k, v in result["reviews"].items() if k not in reviews}
    result["coverage"]["mapped_identities"] -= 7
    result["coverage"]["unresolved_identities"] += 7
    result["coverage"]["dispositions"]["proxy"] -= 7
    result["coverage"]["dispositions"]["unresolved"] += 7
    return result
