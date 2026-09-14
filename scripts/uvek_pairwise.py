"""Pairwise identity-level checks for CLIC versus existing UVEK candidates."""

from __future__ import annotations

import re

ALIASES = {
    "potatoes": "potato",
    "potatos": "potato",
    "soybeans": "soybean",
    "soy": "soybean",
    "hydropower": "hydro",
    "pv": "photovoltaic",
    "cogen": "chp",
    "cogeneration": "chp",
    "polyethylen": "polyethylene",
    "terephtalate": "terephthalate",
    "sulphate": "sulfate",
    "sulphuric": "sulfuric",
    "sulphur": "sulfur",
    "fibre": "fiber",
    "fibres": "fiber",
    "fibers": "fiber",
    "supercalendred": "supercalendered",
    "aluminum": "aluminium",
    "fertilizer": "fertiliser",
    "fertilizers": "fertiliser",
    "fertilisers": "fertiliser",
    "grains": "grain",
    "beets": "beet",
    "plastics": "plastic",
    "solvents": "solvent",
    "alcohols": "alcohol",
    "textiles": "textile",
    "residues": "residue",
}
STOP = set(
    "at from for of the to in and or with by production product market group plant regional storage storehouse operation processing process general unspecified average mix water without solution state liquid gaseous kg kilogram neutralising agent equivalent".split()
)
FUELS = {
    "natural_gas": r"\bnatural gas\b",
    "biogas": r"\bbiogas\b",
    "wood": r"\bwood|\bpellet",
    "hard_coal": r"\bhard coal\b",
    "lignite": r"\blignite\b",
    "oil": r"\boil\b|\bdiesel\b",
    "blast_gas": r"blast furnace gas|coke oven gas",
    "hydro": r"hydro(?:power)?\b|hydro power",
    "wind": r"\bwind\b",
    "photovoltaic": r"photovoltaic|\bpv\b",
    "nuclear": r"nuclear",
    "geothermal": r"geothermal",
    "waste": r"waste incineration|municipal.*incineration|from waste",
    "solar": r"solar thermal|solar collector",
    "heat_pump": r"heat pump",
}
TECHNOLOGIES = {
    "combined_cycle": r"combined cycle",
    "chp": r"co-generation|\bcogen|\bchp\b",
    "pumped_storage": r"pumped storage",
    "reservoir": r"reservoir",
    "run_of_river": r"run.of.river",
    "offshore": r"offshore",
    "onshore": r"onshore",
    "boiling_water": r"boiling water",
    "pressure_water": r"pressure water|pressurized water",
    "single_si": r"single.si",
    "multi_si": r"multi.si|\bmc.si",
    "roof": r"roof",
    "open_ground": r"open ground",
    "electric_steel": r"steel.*electric",
    "converter_steel": r"steel.*converter",
}
CHEM_ORGANIC = re.compile(
    r"methyl|ethyl|propyl|butyl|phenol|benzene|toluene|xylene|acet|alkyl|amine|amide|ester|ether|ketone|aldehyd|methan|ethan|ethen|propen|propan|butan|buten|butadien|glycol|glycer|acryl|phthal|styrene|vinyl|anilin|naphth|furan|pyridin|dioxan|hexan|heptan|octan|cyclo|cresol|formic|oxalic|citric|lactic|maleic|fumaric|adip|sorbic|stear|palmit|lauric|oleic|amino|alcohol"
)
CHEM_INORGANIC = re.compile(
    r"chloride|sulfate|sulphate|sulfite|nitrate|nitrite|hydroxide|oxide|carbonate|phosphate|silicate|peroxide|hypochlorite|chlorate|fluoride|bromide|iodide|cyanide|ammonia|ammonium|hydrochloric|sulfuric|sulphuric|nitric|phosphoric|borate|sulfide|silica"
)


def clean(text):
    text = re.sub(r"^x{2,}\s*", "", text.lower().strip())
    text = re.sub(r"\bito\b", "indium tin oxide", text)
    text = re.sub(r"nylon 6[- ]6|nylon 66", "nylon66", text)
    return text.replace("cross-laminted", "cross-laminated")


def terms(text):
    return {ALIASES.get(token, token) for token in re.findall(r"[a-z0-9]+", clean(text)) if token not in STOP}


def product_head(text):
    text = clean(text)
    text = re.split(r",?\s+(?:from|at|by|for reuse|to generic|in \d|produced)\b", text)[0]
    return text.strip(" ,")


def carrier(text):
    text = clean(text)
    if text.startswith("electricity"):
        return "electricity"
    if text.startswith(("heat", "district heat")):
        return "heat"
    if text.startswith("steam"):
        return "steam"
    return None


def tags(text, patterns):
    return {name for name, pattern in patterns.items() if re.search(pattern, clean(text))}


def energy_tags(source):
    text = source["name"] + " " + source["reference product"]
    return tags(text.replace("other than natural gas", ""), FUELS)


def waste_material(text):
    text = clean(text)
    if "sewage sludge" in text:
        return {"sewage", "sludge"}
    if "bottom ash" in text:
        return {"bottom", "ash"}
    if "fly ash" in text:
        return {"fly", "ash"}
    if text.startswith(("residue", "disposal, residue")):
        return {"residue"}
    if "wastewater" in text or "sewage" in text or "effluent" in text:
        return {"wastewater"}
    if "sludge" in text:
        return {"sludge"}
    if "ash" in text:
        return {"ash"}
    if "polyethylene terephthalate" in text:
        return {"pet"}
    return terms(re.split(r",?\s+(?:from|in|to|wwt)\b|\d+\s*%\s*water", text)[0]) - {
        "waste",
        "used",
        "disposal",
        "treatment",
        "scrap",
    }


def is_waste_product(text):
    return clean(text).startswith(
        ("waste", "used ", "sewage", "sludge", "scrap", "bottom ash", "fly ash", "hard coal ash", "residue")
    ) or "sludge" in clean(text)


def disposal(text):
    return clean(text).startswith(("disposal", "treatment", "recycling", "dismantling"))


def disposal_route(text):
    text = clean(text)
    if "landfill" in text or "deposit" in text or "slag compartment" in text:
        return "landfill"
    if "incineration" in text or "incinerator" in text:
        return "incineration"
    if "recycling" in text:
        return "recycling"
    if "landfarming" in text:
        return "landfarming"
    return None


def chemical_class(product):
    text = product_head(product)
    if text == "hydroxylamine":
        return "inorganic"
    if (
        is_waste_product(text)
        or carrier(text)
        or re.search(
            r"fertilis|fertiliz|ore\b|scrap|sludge|slag|fume|purge gas|alloy|protein|feed|pulp|paper|textile|fiber|wafer|battery|resin|polymer|plastic|polyester|polyethylene|polypropylene|polyvinyl|sputter|sinter|neutralising",
            text,
        )
    ):
        return None
    if CHEM_ORGANIC.search(text) or "allyl" in text:
        return "organic"
    if CHEM_INORGANIC.search(text):
        return "inorganic"
    return None


def product_overlap(source_product, target_product):
    source_terms = terms(product_head(source_product))
    target_terms = terms(product_head(target_product))
    if not source_terms:
        return 0.0
    return len(source_terms & target_terms) / len(source_terms)


def functional_improvement(source, previous, proposed):
    """Recognize inspected functional synonyms and wrong-product families."""

    product = clean(source["reference product"])
    old, new = clean(previous["reference product"]), clean(proposed["reference product"])
    if (
        product.startswith("operation, computer,")
        and new.startswith("use, laptop")
        and not re.search(r"off mode|standby|sleep", product)
    ):
        if not re.search(r"use, (?:laptop|desktop)|computer", old):
            return "Computer-use proxy replaces an unrelated industrial operation/network-only target.", 0.5
    if product.startswith("hydrogen,") and new.startswith("hydrogen production") and "storage tank" in old:
        return (
            "Hydrogen supply replaces storage-tank manufacture; production-route differences remain a proxy limitation.",
            0.5,
        )
    if product.startswith("transport, freight, light commercial vehicle") and "lorry" in new and "rail" in old:
        return "Road-freight proxy replaces the wrong transport mode (rail).", 0.5
    if (
        re.match(r"transport, freight, lorry (?:>32|28)\b", product)
        and "lorry, fleet average" in new
        and "3.5t gross" in old
    ):
        return "Fleet-average road freight avoids the existing explicitly undersized 3.5-tonne vehicle proxy.", 0.5
    if (
        "removed by drilling, computer numerical controlled" in product
        and "drilling, cnc" in new
        and "drilling, conventional" in old
    ):
        return "CNC drilling service replaces conventional drilling for the same material.", 0.7
    if product.startswith("blower and heat exchange unit") and "ventilation equipment" in new and "cogen" in old:
        model = product.rsplit(",", 1)[-1].strip()
        if model in new:
            return "Model-matched ventilation equipment replaces a cogeneration heat exchanger.", 0.7
    if (
        product.startswith("photovoltaic mounting system")
        and "open ground" in product
        and "open ground construction" in new
        and "solder" in old
    ):
        return "PV mounting infrastructure replaces unrelated electronics mounting.", 0.7
    if product.startswith("bentonite quarry infrastructure") and "mine, bentonite" in new and "cable car" in old:
        return "Bentonite-mine infrastructure replaces cable-car infrastructure.", 0.7
    if product.startswith("cement, type") and "cement" in new and "switch" in old:
        return (
            "Cement-family proxy replaces an unrelated electrical switch; exact cement formulation remains approximate.",
            0.5,
        )
    if product.startswith(("diode, auxilliaries", "resistor, auxilliaries")) and "production efforts" in new:
        return (
            "Manufacturing-effort service preserves auxiliary/energy scope rather than finished component supply.",
            0.6,
        )
    core = terms(product.split(",")[0])
    new_core = terms(product_head(new).split(",")[0])
    old_core = terms(product_head(old).split(",")[0])
    if (
        core
        and core == new_core
        and old_core > core
        and (old_core - core) & {"oil", "meal", "disk", "grading", "concentrate"}
    ):
        return (
            "Branch supplies the named commodity rather than its oil, meal, fabricated form, grading service, or concentrate.",
            0.75,
        )
    return None


def decide(current, candidate):
    """Return outcome, evidence, and an uncalibrated compatibility confidence cap."""

    source, previous, proposed = current["source"], current["target"], candidate["target"]
    product = source["reference product"]
    previous_product, proposed_product = previous["reference product"], proposed["reference product"]
    source_carrier = carrier(product)
    proposed_carrier, previous_carrier = carrier(proposed_product), carrier(previous_product)
    proposed_match = product_overlap(product, proposed_product)
    previous_match = product_overlap(product, previous_product)
    source_basis = re.search(r"\bas (k2o|p2o5|n)\b", clean(product))
    proposed_basis = re.search(r"\bas (k2o|p2o5|n)\b", clean(proposed_product))
    if proposed_basis and (not source_basis or proposed_basis.group(1) != source_basis.group(1)):
        return (
            "reject_nutrient_basis",
            "Branch target is per mass of nutrient, not the source's mass basis; a conversion factor is required.",
            None,
        )
    if clean(proposed_product).startswith("pitch desperg") and clean(product) == "pitch":
        return "reject_product_identity", "A pitch dispersant is not the pitch co-product.", None
    if clean(product) == "lime" and "hydrated" in clean(proposed_product) and "hydrated" not in clean(source["name"]):
        return "review_lime_hydration", "Hydrated lime and unspecified lime require a composition/process check.", None
    if "nylon" in clean(product) and ("nylon66" in clean(product)) != ("nylon66" in clean(proposed_product)):
        return "reject_product_identity", "Branch changes the nylon polymer identity.", None
    if "ethanol" in clean(product) and "from ethylene" in clean(product) and "biomass" in clean(proposed_product):
        return "retain_process_detail", "Branch changes the explicit fossil feedstock to biomass.", None
    if source_carrier:
        if proposed_carrier != source_carrier:
            return "reject_carrier", "Branch changes the reference-product energy carrier.", None
        if "allocation heat" in clean(proposed_product) and source_carrier == "electricity":
            return (
                "review_energy_allocation",
                "Electricity target specifies allocation to heat; verify its characterization before use.",
                None,
            )
        source_voltage = re.search(r"(low|medium|high) voltage", clean(product))
        proposed_voltage = re.search(r"(low|medium|high) voltage", clean(proposed_product))
        if source_voltage and proposed_voltage and source_voltage.group(0) != proposed_voltage.group(0):
            return "reject_voltage", "Branch explicitly changes the source voltage level.", None
        source_fuels = energy_tags(source)
        proposed_fuels = tags(proposed_product, FUELS)
        previous_fuels = tags(previous_product, FUELS)
        if source_fuels and (not proposed_fuels & source_fuels or proposed_fuels - source_fuels):
            return "retain_energy_fuel", "Branch loses or contradicts the source fuel/technology.", None
        source_tech = tags(source["name"], TECHNOLOGIES)
        proposed_tech, previous_tech = tags(proposed_product, TECHNOLOGIES), tags(previous_product, TECHNOLOGIES)
        for exclusive in (
            {"offshore", "onshore"},
            {"boiling_water", "pressure_water"},
            {"single_si", "multi_si"},
            {"pumped_storage", "reservoir", "run_of_river"},
        ):
            if source_tech & exclusive and proposed_tech & exclusive and not source_tech & proposed_tech & exclusive:
                return "reject_energy_subtype", "Branch contradicts an explicit source technology subtype.", None
        if (source_tech & previous_tech) - proposed_tech:
            return "retain_energy_detail", "Existing target preserves additional source technology details.", None
        if previous_carrier != source_carrier or (source_fuels and not previous_fuels & source_fuels):
            return (
                "accept_energy_fuel",
                "Branch preserves the output carrier and source fuel; previous target has the wrong carrier/fuel or a generic mix.",
                0.7,
            )
        if source_tech & proposed_tech and len(source_tech & proposed_tech) > len(source_tech & previous_tech):
            return (
                "accept_energy_technology",
                "Same carrier/fuel; branch additionally preserves source generation technology.",
                0.7,
            )
        return "retain_energy_tie", "No demonstrated carrier, fuel, or technology improvement.", None

    if proposed_carrier:
        return "reject_carrier", "An energy output cannot replace the non-energy reference product.", None

    if is_waste_product(product) or (disposal(source["name"]) and product.startswith(("treatment", "disposal"))):
        if not disposal(proposed_product):
            return "reject_waste_role", "Branch maps a waste-handling identity to a supplied product.", None
        material = waste_material(product)
        proposed_material, previous_material = waste_material(proposed_product), waste_material(previous_product)
        source_water = re.search(r"(\d+(?:\.\d+)?)% water", product)
        target_water = re.search(r"(\d+(?:\.\d+)?)% water", proposed_product)
        if (
            "sludge" in material
            and source_water
            and target_water
            and float(source_water.group(1)) != float(target_water.group(1))
        ):
            return (
                "review_sludge_moisture",
                "Sludge water fractions differ; equal mass units do not establish a dry/wet-mass conversion.",
                None,
            )
        if not material or not material & proposed_material:
            return (
                "reject_waste_material",
                "Branch does not preserve the waste material; origin tokens are not evidence of a match.",
                None,
            )
        if (
            material & {"sludge", "ash", "wastewater"}
            and not material & {"sludge", "ash", "wastewater"} <= proposed_material
        ):
            return "reject_waste_material", "Branch loses the sludge, ash, or wastewater identity.", None
        if material & {"ash", "residue"}:
            return (
                "review_residue_composition",
                "Ash/residue composition and treatment origin need explicit review; shared ash/residue tokens are insufficient.",
                None,
            )
        if not material & {"sludge", "wastewater"} and len(material & proposed_material) / len(material) < 0.5:
            return "reject_waste_material", "Insufficient waste-material correspondence beyond generic terms.", None
        if "plasterboard" in clean(product) and "concrete" in clean(proposed_product):
            return "reject_waste_material", "Reinforced plasterboard is not reinforced concrete.", None
        route = disposal_route(source["name"])
        if route and disposal_route(proposed_product) not in (None, route):
            return "reject_waste_route", "Branch contradicts the explicit treatment route.", None
        if not disposal(previous_product) or not material & previous_material:
            return (
                "accept_waste_material",
                "Branch supplies waste handling for the named material rather than an unrelated supplied product/material.",
                0.6,
            )
        if route and disposal_route(proposed_product) == route and disposal_route(previous_product) != route:
            return (
                "accept_waste_route",
                "Branch preserves waste material and the source's explicit treatment route.",
                0.6,
            )
        return "retain_waste_tie", "Both targets offer related waste handling; superiority remains uncertain.", None

    if disposal(proposed_product):
        return "reject_product_role", "Branch maps a supplied reference product to disposal.", None

    if clean(proposed_product).startswith("chemicals "):
        chemical = chemical_class(product)
        if chemical is None or not clean(proposed_product).startswith("chemicals " + chemical):
            return (
                "reject_chemical_class",
                "Chemical family must follow the reference product, not the activity name.",
                None,
            )
        if clean(product) == "butane" and "butane-1,4-diol" in clean(previous_product):
            return (
                "accept_chemical_proxy",
                "Generic organic-chemical proxy replaces a wrong named compound (butane is not butane-1,4-diol). More specific catalog candidates still merit review.",
                0.4,
            )
        if previous_match >= 0.5:
            return "retain_specific_chemical", "Existing target retains substantial reference-product identity.", None
        source_head, previous_head = product_head(product), product_head(previous_product).split(",")[0]
        if len(previous_head) >= 5 and (previous_head in source_head or source_head in previous_head):
            return (
                "retain_related_chemical",
                "Existing target is a related named chemical; generic superiority is uncertain.",
                None,
            )
        if current["confidence"] < 0.35:
            return (
                "accept_chemical_proxy",
                "Low-confidence named target does not preserve the source chemical identity; branch provides the correct broad chemical family. Coarse proxy only.",
                0.4,
            )
        return (
            "review_chemical_proxy",
            "Insufficient evidence to replace this named target with a generic chemical.",
            None,
        )

    nutrient = re.search(r"\bas (k2o|p2o5|n)\b", clean(product))
    if nutrient:
        basis = nutrient.group(0)
        if basis not in clean(proposed_product):
            return "reject_nutrient_basis", "Branch loses the declared nutrient mass basis.", None
        if basis not in clean(previous_product):
            return (
                "accept_nutrient_basis",
                "Branch preserves the reference-product nutrient mass basis missing from the existing target.",
                0.6,
            )
        return "retain_nutrient_tie", "Both targets preserve nutrient basis; their formulation requires review.", None

    source_tech = tags(source["name"], TECHNOLOGIES)
    if (source_tech & tags(previous_product, TECHNOLOGIES)) - tags(proposed_product, TECHNOLOGIES):
        return "retain_process_detail", "Existing target preserves an explicit source process technology.", None
    improvement = functional_improvement(source, previous, proposed)
    if improvement:
        return "accept_functional_match", *improvement
    if proposed_match >= 0.75 and proposed_match >= previous_match + 0.25:
        return (
            "accept_product_identity",
            "Branch has stronger reference-product token coverage under the same normalization, without losing a checked process distinction.",
            0.75,
        )
    if "foam glass" in clean(product) and clean(proposed_product).startswith("foam glass") and previous_match < 0.5:
        return "accept_product_proxy", "Branch preserves foam-glass material instead of an unrelated product.", 0.6
    if previous_match >= 0.75:
        return (
            "retain_product_identity",
            "Existing mapping preserves substantial product identity; no established improvement.",
            None,
        )
    return "review_pair", "Neither candidate establishes a sufficiently better product/process match.", None
