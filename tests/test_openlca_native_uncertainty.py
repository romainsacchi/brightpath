import json
import math
import zipfile
from copy import deepcopy

import pytest
from olca_schema.zipio import ZipWriter

from brightpath.core import BackgroundContext, BiosphereProfile, FormatProfile, InventoryContext, TechnosphereProfile
from brightpath.formats.openlca_jsonld import load_openlca_jsonld, render_openlca_jsonld_package
from brightpath.formats.openlca_native_uncertainty import (
    COMPATIBILITY,
    NativeUncertaintyError,
    apply,
    decode,
    prepare,
)
from brightpath.models import InventoryDocument


def document(rows=None, parameters=None):
    context = InventoryContext(
        format=FormatProfile("openlca_jsonld"),
        background=BackgroundContext(
            technosphere=TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
            biosphere=BiosphereProfile("ecoinvent", "3.12"),
        ),
    )
    production = {
        "name": "Synthetic gate",
        "reference product": "Synthetic gate product",
        "location": "GLO",
        "unit": "kilogram",
        "type": "production",
        "amount": 1.0,
    }
    exchanges = [production]
    if rows is None:
        rows = [{"amount": 100.0, "loc": math.log(150), "scale": 0.3}]
    for i, values in enumerate(rows):
        exchanges.append(
            {
                "name": f"Synthetic elementary {i}",
                "unit": "kilogram",
                "categories": ("air",),
                "type": "biosphere",
                "uncertainty type": 2,
                **values,
            }
        )
    activity = {
        "code": "c74250ac-5bf0-4b30-aab6-b0114defc401",
        "name": "Synthetic gate",
        "reference product": "Synthetic gate product",
        "location": "GLO",
        "unit": "kilogram",
        "exchanges": exchanges,
    }
    if parameters is not None:
        activity["parameters"] = parameters
    return InventoryDocument(data=[activity], context=context, database_name="synthetic-native-gate")


def render(doc):
    stripped, specs = prepare(doc, compatibility=COMPATIBILITY)
    return apply(render_openlca_jsonld_package(stripped), specs)


def archive(rendered, path, *, strip_descriptions=False):
    with ZipWriter(path) as writer:
        for store in vars(rendered).values():
            for entity in store.values():
                writer.write(entity)
    if strip_descriptions:
        with zipfile.ZipFile(path) as reader:
            items = {n: reader.read(n) for n in reader.namelist()}

        def strip(value):
            if isinstance(value, dict):
                return {k: strip(v) for k, v in value.items() if k not in ("description", "otherProperties")}
            if isinstance(value, list):
                return [strip(v) for v in value]
            return value

        with zipfile.ZipFile(path, "w") as writer:
            for name, raw in items.items():
                writer.writestr(name, json.dumps(strip(json.loads(raw))) if name.endswith(".json") else raw)
    return path


def loaded(doc, tmp_path, rendered=None):
    return load_openlca_jsonld(
        archive(rendered or render(doc), tmp_path / "graph.zip"),
        context=doc.context,
        decode_native_uncertainty=False,
    )


@pytest.mark.parametrize(
    "values",
    [
        {"amount": 100, "loc": math.log(150), "scale": 0.3},
        {"amount": -7, "loc": math.log(10), "scale": 0.2},
        {"amount": 0, "loc": math.log(2), "scale": 0.4},
        {"amount": math.exp(-200), "loc": 0, "scale": 1},
        {"amount": 7, "loc": math.log(10), "scale": 0.2, "negative": True},
        {"amount": 7, "loc": math.log(10), "scale": 0.2, "negative": 0},
    ],
)
def test_exact_native_graph_roundtrip(values, tmp_path):
    doc = document([values])
    before = deepcopy(doc.data)
    result = decode(loaded(doc, tmp_path)).data[0]
    exchange = result["exchanges"][1]
    for key in ("amount", "loc", "scale"):
        assert exchange[key] == values[key]
    assert exchange["negative"] == values.get("negative", values["amount"] < 0)
    assert exchange["uncertainty type"] == 2
    assert "formula" not in exchange
    assert "parameters" not in result
    assert doc.data == before


def test_default_geom_mean_writer_remains_unchanged():
    doc = document()
    raw = render_openlca_jsonld_package(doc)
    exchange = next(iter(raw.processes.values())).exchanges[1]
    assert exchange.uncertainty.geom_mean == pytest.approx(150)
    assert exchange.uncertainty.geom_sd == pytest.approx(math.exp(0.3))
    assert exchange.amount_formula is None
    assert next(iter(raw.processes.values())).parameters is None
    with pytest.raises(TypeError):
        prepare(doc)
    with pytest.raises(NativeUncertaintyError):
        prepare(doc, compatibility="generic")


def test_prepare_apply_and_decode_copy_and_idempotence(tmp_path):
    doc = document()
    before = doc.data
    stripped, specs = prepare(doc, compatibility=COMPATIBILITY)
    assert stripped.data[0]["exchanges"][1]["amount"] == 100
    assert "uncertainty type" not in stripped.data[0]["exchanges"][1]
    raw = render_openlca_jsonld_package(stripped)
    snapshot = deepcopy(raw)
    applied = apply(raw, specs)
    assert raw == snapshot
    imported = loaded(doc, tmp_path, applied)
    imported_before = imported.data
    decoded = decode(imported)
    assert imported.data == imported_before
    assert decode(decoded).data == decoded.data
    assert doc.data == before
    assert prepare(stripped, compatibility=COMPATIBILITY)[1] == ()


def test_native_parameters_and_formulas_survive_without_comments(tmp_path):
    doc = document()
    path = archive(render(doc), tmp_path / "bare.zip", strip_descriptions=True)
    result = decode(load_openlca_jsonld(path, context=doc.context)).data[0]["exchanges"][1]
    assert result["loc"] == math.log(150)
    assert result["scale"] == 0.3
    assert result["amount"] == 100


@pytest.mark.parametrize("formula", ["alpha*beta", " (alpha + beta) * 1.2 ", "6", "(6)"])
def test_preserves_exact_verified_source_formula(formula, tmp_path):
    doc = document(
        [{"amount": 6, "loc": math.log(9), "scale": 0.5, "formula": formula}],
        parameters=[{"name": "alpha", "amount": 2}, {"name": "beta", "amount": 3}],
    )
    result = decode(loaded(doc, tmp_path)).data[0]
    assert result["exchanges"][1]["formula"] == formula
    assert [p["name"] for p in result["parameters"]] == ["alpha", "beta"]


@pytest.mark.parametrize(
    "patch,match",
    [
        ({"minimum": 0}, "bound"),
        ({"maximum": 10}, "bound"),
        ({"minimum": -math.inf}, "bound"),
        ({"shape": 2}, "shape"),
        ({"negative": "false"}, "Boolean"),
        ({"scale": -0.3}, "nonnegative"),
        ({"loc": math.nan}, "finite"),
        ({"formula": "1+1"}, "stored amount"),
        ({"formula": "unknown*2"}, "missing"),
        ({"formula": "EXP(1)"}, "arithmetic"),
    ],
)
def test_rejects_real_contract_limits(patch, match):
    doc = document([{"amount": 100, "loc": math.log(150), "scale": 0.3, **patch}])
    before = doc.data
    with pytest.raises(NativeUncertaintyError, match=match):
        prepare(doc, compatibility=COMPATIBILITY)
    assert doc.data == before or (math.isnan(patch.get("loc", 0)) and len(doc.data) == 1)


def test_refuses_active_lognormal_parameter_without_silent_native_mismatch():
    doc = document(parameters=[{"name": "p", "amount": 2, "uncertainty type": 2, "loc": math.log(2), "scale": 0.3}])
    with pytest.raises(NativeUncertaintyError, match="input parameters"):
        prepare(doc, compatibility=COMPATIBILITY)


def test_refuses_nominal_formula_using_uncertain_parameter():
    doc = document(
        [{"amount": 2, "loc": math.log(3), "scale": 0.3, "formula": "p"}],
        parameters=[{"name": "p", "amount": 2, "uncertainty type": 3, "loc": 2, "scale": 0.3}],
    )
    with pytest.raises(NativeUncertaintyError, match="uncertain parameter"):
        prepare(doc, compatibility=COMPATIBILITY)


def test_fresh_helpers_do_not_share_latents_or_shadow_caller_names(tmp_path):
    doc = document(
        [{"amount": 1, "loc": 0, "scale": 0.3}] * 2, parameters=[{"name": "bp_olca262_existing", "amount": 3}]
    )
    stripped, specs = prepare(doc, compatibility=COMPATIBILITY)
    assert len({s.latent for s in specs}) == 2
    assert len({s.gate for s in specs}) == 1
    result = decode(loaded(doc, tmp_path)).data[0]
    assert [p["name"] for p in result["parameters"]] == ["bp_olca262_existing"]


@pytest.mark.parametrize(
    "mutation,match",
    [
        ("gate_amount", "helper|gate"),
        ("gate_bounds", "gate"),
        ("latent_kind", "distribution"),
        ("latent_bounds", "bounds"),
        ("latent_formula", "distribution"),
        ("scope", "scope|input parameter"),
        ("second_uncertainty", "second"),
        ("nominal", "stored amount"),
        ("extra_dependency", "extra"),
        ("bad_function", "Unrecognized"),
    ],
)
def test_strict_native_graph_mutation_rejection(mutation, match, tmp_path):
    doc = document()
    parsed = loaded(doc, tmp_path)
    data = parsed.data
    process = data[0]
    gate, latent = process["parameters"]
    exchange = process["exchanges"][1]
    if mutation == "gate_amount":
        gate["amount"] = 1
    elif mutation == "gate_bounds":
        gate["minimum"] = 0
    elif mutation == "latent_kind":
        latent["uncertainty type"] = 4
    elif mutation == "latent_bounds":
        latent["maximum"] = 100
    elif mutation == "latent_formula":
        latent["formula"] = "1+1"
    elif mutation == "scope":
        latent["openlca parameter"]["parameterScope"] = "GLOBAL_SCOPE"
    elif mutation == "second_uncertainty":
        exchange.update({"uncertainty type": 3, "loc": 100, "scale": 1})
    elif mutation == "nominal":
        exchange["amount"] = 101
    elif mutation == "extra_dependency":
        process["parameters"].append({"name": "other", "formula": latent["name"] + "+1"})
    elif mutation == "bad_function":
        exchange["formula"] = exchange["formula"].replace("EXP(", "LN(")
    altered = parsed.replace(data=data)
    with pytest.raises(NativeUncertaintyError, match=match):
        decode(altered)
    assert altered.data == data


def test_decodes_actual_changed_native_mean_and_sigma_not_original_metadata(tmp_path):
    doc = document()
    parsed = loaded(doc, tmp_path)
    data = parsed.data
    latent = data[0]["parameters"][1]
    latent.update({"amount": 7, "loc": 7, "scale": 0.7})
    latent["openlca parameter"]["value"] = 7
    latent["openlca parameter"]["uncertainty"].update({"mean": 7, "sd": 0.7})
    result = decode(parsed.replace(data=data)).data[0]["exchanges"][1]
    assert (result["loc"], result["scale"]) == (7, 0.7)


def test_shared_latent_rejected(tmp_path):
    doc = document([{"amount": 1, "loc": 0, "scale": 0.3}] * 2)
    parsed = loaded(doc, tmp_path)
    data = parsed.data
    rows = data[0]["parameters"]
    first = rows[1]["name"]
    second = rows[2]["name"]
    data[0]["exchanges"][2]["formula"] = data[0]["exchanges"][2]["formula"].replace(second, first)
    with pytest.raises(NativeUncertaintyError, match="shared"):
        decode(parsed.replace(data=data))


def test_apply_detects_changed_prepared_render():
    stripped, specs = prepare(document(), compatibility=COMPATIBILITY)
    rendered = render_openlca_jsonld_package(stripped)
    next(iter(rendered.processes.values())).exchanges[1].amount = 999
    with pytest.raises(NativeUncertaintyError, match="stripped"):
        apply(rendered, specs)


def test_tiny_positive_scale_never_collapses_to_zero(tmp_path):
    doc = document([{"amount": 1, "loc": 0, "scale": 1e-14}])
    rendered = render(doc)
    latent = next(iter(rendered.processes.values())).parameters[1]
    assert latent.uncertainty.sd == 1e-14
    assert decode(loaded(doc, tmp_path, rendered)).data[0]["exchanges"][1]["scale"] == 1e-14


def test_existing_exact_helper_like_names_are_retained_without_shadowing(tmp_path):
    _, first = prepare(document(), compatibility=COMPATIBILITY)
    names = [first[0].gate, first[0].latent.upper()]
    doc = document(parameters=[{"name": name, "amount": 3} for name in names])
    _, specs = prepare(doc, compatibility=COMPATIBILITY)
    assert not {specs[0].gate.casefold(), specs[0].latent.casefold()} & {n.casefold() for n in names}
    actual = decode(loaded(doc, tmp_path)).data[0]
    assert [p["name"] for p in actual["parameters"]] == names


def test_normal_and_inactive_exchanges_are_unchanged_by_native_apply():
    doc = document(
        [{"amount": 2, "loc": 3, "scale": 0.2, "uncertainty type": 3}, {"amount": 4, "loc": 5, "scale": 0.2}]
    )
    stripped, specs = prepare(doc, compatibility=COMPATIBILITY)
    raw = render_openlca_jsonld_package(stripped)
    result = apply(raw, specs)
    assert next(iter(raw.processes.values())).exchanges[1] == next(iter(result.processes.values())).exchanges[1]


def test_raw_native_projection_disagreement_cannot_restore_uncertainty(tmp_path):
    doc = document()
    parsed = loaded(doc, tmp_path)
    data = parsed.data
    data[0]["parameters"][1]["openlca parameter"]["uncertainty"]["sd"] = 0.9
    with pytest.raises(NativeUncertaintyError, match="projection"):
        decode(parsed.replace(data=data))


@pytest.mark.parametrize("patch", [{"loc": 1000}, {"loc": -1000}, {"scale": 1000}])
def test_existing_finite_geometric_guards_are_not_weakened(patch):
    doc = document([{"amount": 1, "loc": 0, "scale": 0.3, **patch}])
    with pytest.raises(NativeUncertaintyError, match="geometric"):
        prepare(doc, compatibility=COMPATIBILITY)


def test_database_project_shadowing_is_a_reported_native_contract_limit():
    original = document()
    doc = InventoryDocument(
        data=original.data,
        context=original.context,
        database_parameters=[{"name": "p", "amount": 1}],
        project_parameters=[{"name": "P", "amount": 2}],
    )
    with pytest.raises(NativeUncertaintyError, match="global scope"):
        prepare(doc, compatibility=COMPATIBILITY)


def test_formula_parameter_graph_reuses_shared_dependencies():
    from brightpath.formats.openlca_native_uncertainty import _arithmetic

    parameters = {"p0": {"amount": 1}}
    for index in range(1, 41):
        parameters[f"p{index}"] = {"formula": f"p{index - 1}+p{index - 1}"}
    assert _arithmetic("p40", parameters) == 2**40


def test_deep_parameter_dependency_is_a_controlled_refusal():
    from brightpath.formats.openlca_native_uncertainty import _arithmetic

    parameters = {"p0": {"amount": 1}}
    for index in range(1, 401):
        parameters[f"p{index}"] = {"formula": f"p{index - 1}"}
    with pytest.raises(NativeUncertaintyError, match="work/depth"):
        _arithmetic("p400", parameters)


def test_ordinary_document_without_native_graph_is_not_reinterpreted():
    doc = document(parameters=[{"name": "p", "amount": 1}, {"name": "p", "amount": 2}])
    assert decode(doc).data == doc.data


def test_document_reader_decodes_compatibility_graph_by_default(tmp_path):
    doc = document()
    path = archive(render(doc), tmp_path / "compatibility.zip")
    loaded = load_openlca_jsonld(path, context=doc.context).data[0]
    assert loaded["exchanges"][1]["uncertainty type"] == 2
    assert loaded["exchanges"][1]["loc"] == math.log(150)
    assert not loaded.get("parameters")


@pytest.mark.parametrize("explicit_context", [True, False])
def test_upload_analysis_decodes_native_graph_before_validation(tmp_path, explicit_context):
    from brightpath.analysis.analyzer import analyze_inventory
    from brightpath.models import BackgroundProfile

    doc = document()
    path = archive(render(doc), tmp_path / "upload.zip")
    options = (
        {"source_context": doc.context}
        if explicit_context
        else {"source_profile": BackgroundProfile("ecoinvent", "3.12", "cutoff")}
    )
    result = analyze_inventory(path=path, **options)
    assert len(result.inventory_data) == 1
    actual = result.inventory_data[0]
    assert not actual.get("parameters")
    exchange = actual["exchanges"][1]
    assert exchange["amount"] == 100
    assert exchange["loc"] == math.log(150)
    assert exchange["scale"] == 0.3
    assert exchange["uncertainty type"] == 2
    assert not exchange.get("formula")


@pytest.mark.parametrize("formula,amount", [("2**3", 8), ("2^3", 8), ("2^3^2", 512), ("-2^2", -4)])
def test_unverified_native_power_syntax_and_precedence_are_refused(formula, amount):
    doc = document([{"amount": amount, "loc": math.log(abs(amount)), "scale": 0.3, "formula": formula}])
    before = doc.data
    with pytest.raises(NativeUncertaintyError, match="power syntax/precedence"):
        prepare(doc, compatibility=COMPATIBILITY)
    assert doc.data == before


def test_power_in_nominal_parameter_dependency_is_also_refused():
    doc = document(
        [{"amount": 8, "loc": math.log(8), "scale": 0.3, "formula": "dose"}],
        parameters=[{"name": "dose", "formula": "2^3", "amount": 8}],
    )
    with pytest.raises(NativeUncertaintyError, match="power syntax/precedence"):
        prepare(doc, compatibility=COMPATIBILITY)
