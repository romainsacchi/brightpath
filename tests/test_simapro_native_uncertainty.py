"""Native Boolean uncertainty graphs preserve independent input distributions."""

import ast
import csv
import math
from copy import deepcopy

import pytest

from brightpath.formats.simapro_native_uncertainty import NativeUncertaintyError as GateError
from brightpath.formats.simapro_native_uncertainty import decode, prepare, render
from brightpath.models import BackgroundProfile, InventoryDocument, InventoryFormat


def test_literal_unary_depth_is_controlled_without_python_recursion():
    from brightpath.formats.simapro_native_uncertainty import _constant, _graph

    node = ast.Constant(value=100)
    for _ in range(1500):
        node = ast.UnaryOp(op=ast.USub(), operand=node)
    with pytest.raises(GateError, match="depth limit"):
        _constant(node)
    source = prepare(document())[0].data
    expression = source[0]["exchanges"][1]["formula"].replace(",100)", "," + "-" * 1500 + "100)")
    with pytest.raises(GateError):
        _graph(expression)
    source[0]["exchanges"][1]["formula"] = expression
    assert decode(source) == source  # Unknown shape is retained, never fabricated.


def test_source_structure_is_validated_before_uncertainty_is_encoded():
    source = document()
    data = source.data
    data[0]["exchanges"] = data[0]["exchanges"][1:]
    invalid = source.replace(data=data)
    before = invalid.data
    with pytest.raises(GateError, match="Original source validation"):
        prepare(invalid)
    assert invalid.data == before


@pytest.mark.parametrize("value", [float("inf"), float("nan"), 10**1000])
def test_nonfinite_source_uncertainty_is_refused_without_mutation(value):
    source = document([dict(amount=100, loc=value, scale=0.3)])
    with pytest.raises(GateError, match="finite"):
        prepare(source)


def test_centered_source_sign_mismatch_cannot_skip_validation():
    source = document([dict(amount=100, loc=math.log(100), scale=0.3, negative=True)])
    before = source.data
    with pytest.raises(GateError, match="sign mismatch"):
        prepare(source)
    assert source.data == before


def test_active_zero_nominal_is_an_explicit_scope_limit():
    with pytest.raises(GateError, match="zero-nominal"):
        prepare(document([dict(amount=0, loc=0, scale=0.3)]))


@pytest.mark.parametrize("mu", [-1000, 1000])
def test_native_exp_centre_cannot_underflow_to_zero_or_overflow(mu):
    source = document([dict(amount=1, loc=mu, scale=0.3)])
    with pytest.raises(GateError, match="EXP centre"):
        prepare(source)


@pytest.mark.parametrize("mu", [-1000, 1000])
def test_changed_native_latent_centre_does_not_decode_as_valid_zero_or_infinity(mu):
    data = prepare(document())[0].data
    data[0]["parameters"][1].update(amount=mu, loc=mu)
    with pytest.raises(GateError, match="EXP centre"):
        decode(data)


def test_repeated_prepare_preserves_actual_graph_and_render_precision():
    source = document([dict(amount=0.036000000000000004, loc=-4.422848629194137, scale=0.1312205922929929)])
    first, specs = prepare(source)
    second, repeated_specs = prepare(first)
    assert second.data == first.data
    assert repeated_specs == specs
    rows = render(second, repeated_specs)
    latent = next(row for row in rows if row and row[0] == specs[0].latent)
    assert float(latent[3]) / 2 == 0.1312205922929929
    assert decode(second).data[0]["exchanges"][1]["loc"] == -4.422848629194137


def test_existing_centered_negative_triangle_is_left_to_the_ordinary_codec():
    source = document([dict(amount=-2, loc=-2, minimum=-3, maximum=-1, **{"uncertainty type": 5})])
    prepared, specs = prepare(source)
    assert prepared.data == source.data and specs == ()


def test_render_requires_complete_nonduplicated_specs_to_keep_exact_fields():
    prepared, specs = prepare(document())
    for wrong in ((), specs + specs):
        with pytest.raises(GateError, match="Render specs"):
            render(prepared, wrong)


def test_render_retains_process_scope_when_imported_helper_names_repeat():
    first, _ = prepare(document())
    second, _ = prepare(document([dict(amount=100, loc=math.log(250), scale=0.712345678912345)]))
    other = second.data[0]
    other.update(code="second-native-process", name="Second native process", **{"reference product": "Second product"})
    other["exchanges"][0].update(name=other["name"], **{"reference product": other["reference product"]})
    source = first.replace(data=[first.data[0], other])
    prepared, specs = prepare(source)
    assert specs[0].latent == specs[1].latent  # Legitimate independent process scopes.
    rows = render(prepared, specs)
    actual = [row for row in rows if row and row[0] == specs[0].latent and len(row) == 8]
    assert [float(row[1]) for row in actual] == [math.log(150), math.log(250)]
    assert [float(row[3]) / 2 for row in actual] == [0.3, 0.712345678912345]


def test_unknown_conditional_with_both_original_role_prefixes_is_not_claimed():
    source = document(
        [
            dict(amount=4, formula="iff(abs(bpspg0)>0,bpspl0+1,9)", **{"uncertainty type": 0}),
            dict(amount=100, loc=math.log(150), scale=0.3),
        ],
        parameters=[dict(name="bpspg0", amount=2), dict(name="bpspl0", amount=3)],
    )
    prepared, _ = prepare(source)
    result = decode(prepared.data)[0]
    assert result["exchanges"][1]["formula"] == source.data[0]["exchanges"][1]["formula"]
    assert result["parameters"] == source.data[0]["parameters"]
    assert result["exchanges"][2]["loc"] == math.log(150)


def test_document_decode_and_render_preserve_formula_cell_markers():
    from brightpath.formats.simapro_csv import _Formula, _latin1_safe_cell

    source = document([dict(amount=-7, loc=math.log(10), scale=0.2)])
    prepared, specs = prepare(source)
    result = decode(prepared)
    assert isinstance(result, InventoryDocument)
    assert result.data[0]["exchanges"][1]["amount"] == -7
    rows = render(prepared, specs)
    expression = next(row[3] for row in rows if row and row[0] == "Carbon dioxide, fossil")
    assert isinstance(expression, _Formula)
    assert _latin1_safe_cell(expression) == expression
    assert "iff(abs(" in expression and "=0" not in expression


def test_real_bp_loader_roundtrip_after_sign_and_water_unit_normalization(tmp_path):
    from brightpath.background.catalogs import PackageCatalogProvider
    from brightpath.formats.simapro_csv import _latin1_safe_cell, load_simapro_csv

    source = document([dict(name="Water", unit="cubic meter", amount=-0.1, loc=math.log(0.15), scale=0.3)])
    prepared, specs = prepare(source)
    rows = render(prepared, specs)
    path = tmp_path / "native-water.csv"
    with path.open("w", encoding="latin-1", newline="") as handle:
        csv.writer(handle, delimiter=";").writerows([[_latin1_safe_cell(value) for value in row] for row in rows])
    loaded = load_simapro_csv(path, context=source.context, catalog_provider=PackageCatalogProvider())
    before = loaded.data
    result = decode(loaded).data[0]
    exchange = next(e for e in result["exchanges"] if e["type"] == "biosphere")
    assert exchange["unit"] == "cubic meter"
    assert exchange["amount"] == -0.1 and exchange["loc"] == math.log(0.15)
    assert exchange["scale"] == 0.3 and exchange["negative"] is True
    assert "formula" not in exchange and "parameters" not in result
    assert loaded.data == before


def parse_native_rows(rows):
    """Minimal native graph reader, not a replacement background-identity reader."""
    processes, process, section = ([], None, None)
    exchange_sections = {
        "Emissions to air",
        "Emissions to water",
        "Emissions to soil",
        "Resources",
        "Materials/fuels",
        "Electricity/heat",
    }
    for row in rows:
        if not row or not any((str(x) for x in row)):
            continue
        if len(row) == 1:
            if row[0] == "Process":
                process = {"parameters": [], "exchanges": []}
                processes.append(process)
                section = None
            elif row[0] == "End":
                process = None
            elif process is not None:
                if section == "Process name":
                    process["name"] = row[0]
                    section = None
                else:
                    section = row[0]
            continue
        if process is None:
            continue
        if section == "Input parameters":
            if row[2] == "Normal":
                parameter = {"name": row[0], "amount": float(row[1]), "uncertainty type": 3, "scale": float(row[3]) / 2}
                if float(row[4]) != 0 or float(row[5]) != 0:
                    raise GateError("Unexpected Normal bounds")
            elif row[2] == "Triangle":
                parameter = {
                    "name": row[0],
                    "amount": float(row[1]),
                    "uncertainty type": 5,
                    "minimum": float(row[4]),
                    "maximum": float(row[5]),
                }
            else:
                raise GateError("This narrow reader supports Normal/Triangle parameters only")
            process["parameters"].append(parameter)
        elif section in exchange_sections:
            bio = section not in {"Materials/fuels", "Electricity/heat"}
            ui, ai = (2, 3) if bio else (1, 2)
            amount = str(row[ai])
            e = {"name": row[0], "unit": row[ui], "type": "biosphere" if bio else "technosphere", "uncertainty type": 0}
            if row[ai + 1] != "Undefined":
                raise GateError("This narrow reader expects genuine formula-based uncertainty")
            try:
                e["amount"] = float(amount)
            except ValueError:
                e["formula"] = amount
            process["exchanges"].append(e)
    return processes


def document(rows=None, parameters=None):
    if rows is None:
        rows = [{"amount": 100.0, "loc": math.log(150), "scale": 0.3}]
    flows = ["Carbon dioxide, fossil", "Methane, fossil", "Dinitrogen monoxide", "Sulfur dioxide", "Water"]
    activity = {
        "code": "private-direct-gate",
        "name": "Private direct gate",
        "reference product": "Private product",
        "unit": "kilogram",
        "location": "GLO",
        "comment": "Private synthetic foreground",
        "exchanges": [
            {
                "type": "production",
                "name": "Private direct gate",
                "reference product": "Private product",
                "unit": "kilogram",
                "location": "GLO",
                "amount": 1,
                "simapro category": "Materials/Test",
            }
        ],
    }
    for i, values in enumerate(rows):
        activity["exchanges"].append(
            {
                "name": flows[i],
                "type": "biosphere",
                "unit": "kilogram",
                "categories": ("air",),
                "uncertainty type": 2,
                **values,
            }
        )
    if parameters:
        activity["parameters"] = parameters
    return InventoryDocument(
        data=[activity],
        background_profile=BackgroundProfile("ecoinvent", "3.12", "cutoff"),
        inventory_format=InventoryFormat.SIMAPRO_CSV,
    )


def native_rows(doc, **kwargs):
    prepared, specs = prepare(doc)
    return render(prepared, specs, **kwargs)


@pytest.mark.parametrize(
    "amount,mu,sigma",
    [(100, math.log(150), 0.3), (-7, math.log(10), 0.2), (math.exp(-200), 0, 1), (100, math.log(150), 1e-14)],
)
def test_real_renderer_native_graph_decode_immutability(amount, mu, sigma):
    doc = document([dict(amount=amount, loc=mu, scale=sigma)])
    before = doc.data
    prepared, specs = prepare(doc)
    augmented = prepared.data
    rows = render(prepared, specs)
    read = parse_native_rows(rows)
    untouched = deepcopy(read)
    result = decode(read)[0]["exchanges"][0]
    assert result["amount"] == amount
    assert result["loc"] == mu and result["scale"] == sigma
    assert result["negative"] == (amount < 0)
    assert "parameters" not in decode(read)[0]
    assert read == untouched and doc.data == before and (prepared.data == augmented)
    assert all(("BrightPath parameter" not in str(r) for r in rows if r and str(r[0]).startswith("bpsp")))
    assert all(("=0" not in str(r) for r in rows))
    assert prepare(doc)[0].data == augmented


def test_native_water_unit_wrapper_scales_nominal_and_mu_once():
    doc = document([dict(amount=0.1, loc=math.log(0.15), scale=0.3, unit="cubic meter", name="Water")])
    result = decode(parse_native_rows(native_rows(doc)))[0]["exchanges"][0]
    assert result["unit"] == "kg"
    assert result["amount"] == pytest.approx(100)
    assert result["loc"] == pytest.approx(math.log(150))
    assert result["scale"] == 0.3
    assert result["amount"] / 1000 == pytest.approx(doc.data[0]["exchanges"][1]["amount"])
    assert result["loc"] - math.log(1000) == pytest.approx(doc.data[0]["exchanges"][1]["loc"])


def test_fresh_latents_shared_gate_and_collision_free_names():
    doc = document([dict(amount=100, loc=math.log(150), scale=0.3)] * 2, parameters=[dict(name="bpspg0", amount=0)])
    prepared, specs = prepare(doc)
    assert specs[0].gate == specs[1].gate != "bpspg0"
    assert specs[0].latent != specs[1].latent
    assert len({p["name"] for p in prepared.data[0]["parameters"]}) == 4
    assert len(decode(prepared.data)[0]["parameters"]) == 1


@pytest.mark.parametrize(
    "field,value",
    [("minimum", 0), ("maximum", 100), ("shape", 1), ("formula", "100*1"), ("negative", True), ("scale", -0.3)],
)
def test_unsupported_source_contract_is_refused_without_mutation(field, value):
    row = dict(amount=100, loc=math.log(150), scale=0.3)
    row[field] = value
    doc = document([row])
    before = doc.data
    with pytest.raises(GateError):
        prepare(doc)
    assert doc.data == before


@pytest.mark.parametrize(
    "mutation",
    [
        "gate_value",
        "gate_spread",
        "latent_distribution",
        "bounds",
        "missing",
        "reuse",
        "bad_nominal",
        "bad_sign",
        "power",
        "duplicate",
        "leak",
        "latent_centre",
    ],
)
def test_graph_recognition_uses_actual_native_values_and_rejects_invalid_graphs(mutation):
    data = prepare(document())[0].data
    a = data[0]
    g, p = a["parameters"]
    e = a["exchanges"][1]
    if mutation == "gate_value":
        g["amount"] = 0.1
    elif mutation == "gate_spread":
        g["scale"] = 0.9
    elif mutation == "latent_distribution":
        p["uncertainty type"] = 2
    elif mutation == "bounds":
        p["minimum"] = 0
    elif mutation == "missing":
        a["parameters"].pop()
    elif mutation == "reuse":
        a["exchanges"].append(deepcopy(e))
    elif mutation == "bad_nominal":
        e["formula"] = e["formula"].replace(",100)", ",unknown)")
    elif mutation == "bad_sign":
        e["formula"] = e["formula"].replace("exp(", "-exp(")
    elif mutation == "power":
        e["formula"] = e["formula"].replace(",100)", ",2**999999999)")
    elif mutation == "duplicate":
        a["parameters"].append(deepcopy(p))
    elif mutation == "leak":
        a["parameters"].append(dict(name="extra", formula=g["name"] + "+1"))
    elif mutation == "latent_centre":
        p["loc"] += 0.1
    before = deepcopy(data)
    if mutation in {"bad_nominal", "power"}:
        assert decode(data) == data
    else:
        with pytest.raises(GateError):
            decode(data)
    assert data == before


def test_decoder_reports_changed_real_latent_instead_of_restoring_source():
    data = prepare(document())[0].data
    data[0]["parameters"][1].update(amount=math.log(200), loc=math.log(200), scale=0.7, comment="restore original .3")
    got = decode(data)[0]["exchanges"][1]
    assert got["loc"] == math.log(200) and got["scale"] == 0.7


def test_ordinary_documents_and_centered_inputs_unchanged():
    from brightpath.formats.simapro_csv import render_simapro_rows

    doc = document([dict(amount=100, loc=math.log(100), scale=0.3)])
    prepared, specs = prepare(doc)
    assert specs == () and prepared.data == doc.data
    assert decode(doc.data) == doc.data
    assert render(prepared, specs) == render_simapro_rows(doc).rows


def triangle_technosphere_document():
    doc = document([])
    data = doc.data
    consumer = data[0]
    consumer.update(
        code="private-triangle-consumer",
        name="Private triangle consumer",
        **{"reference product": "Private triangle consumer product"},
    )
    consumer["exchanges"][0].update(name=consumer["name"], **{"reference product": consumer["reference product"]})
    for i, (_, _, nominal, mode, lo, hi) in enumerate(TRIANGLES):
        provider = document([dict(amount=1, **{"uncertainty type": 0, "loc": 1, "scale": 0})]).data[0]
        provider.update(
            code=f"private-tri-provider-{i}",
            name=f"Private provider {i}",
            **{"reference product": f"Private provider product {i}"},
        )
        provider["exchanges"][0].update(name=provider["name"], **{"reference product": provider["reference product"]})
        provider["exchanges"][1].update(name=["Carbon dioxide, fossil", "Methane, fossil", "Dinitrogen monoxide"][i])
        for key in ("uncertainty type", "loc", "scale"):
            provider["exchanges"][1].pop(key, None)
        consumer["exchanges"].append(
            dict(
                name=provider["name"],
                location="GLO",
                unit="kilogram",
                amount=nominal,
                loc=mode,
                minimum=lo,
                maximum=hi,
                type="technosphere",
                **{"reference product": provider["reference product"], "uncertainty type": 5},
            )
        )
        data.append(provider)
    return doc.replace(data=data)


def test_genuine_foreground_provider_triangle_inputs_render_and_decode():
    doc = triangle_technosphere_document()
    before = doc.data
    rows = native_rows(doc)
    result = decode(parse_native_rows(rows))
    consumer = next((p for p in result if "Private triangle consumer" in p.get("name", "")))
    assert len(consumer["exchanges"]) == 3
    assert all((e["type"] == "technosphere" for e in consumer["exchanges"]))
    for e, source in zip(consumer["exchanges"], TRIANGLES, strict=True):
        assert (e["amount"], e["loc"], e["minimum"], e["maximum"]) == source[2:]
    assert doc.data == before


@pytest.mark.parametrize("delta", [1e-10, 1e-11, 1e-13])
def test_no_small_logcentre_difference_is_waived(delta):
    mu = math.log(100) + delta
    doc = document([dict(amount=100, loc=mu, scale=0.3)])
    prepared, specs = prepare(doc)
    assert len(specs) == 1
    assert decode(prepared.data)[0]["exchanges"][1]["loc"] == mu


def test_explicit_boolean_dialect_roundtrips_actual_formulas():
    rows = native_rows(document())
    flow = next((r for r in rows if r and r[0] == "Carbon dioxide, fossil"))
    assert ("abs(" in flow[3].lower()) == ("openlca-boolean" == "openlca-boolean")
    assert decode(parse_native_rows(rows))[0]["exchanges"][0]["loc"] == math.log(150)


TRIANGLES = [
    ("2118", 1, 1.0207169085868795, 1.0207169087028014, 0.7145018360919598, 1.3269319813136347),
    ("2121", 2, 0.07345187508221337, 0.07345187546198374, 0.05141631282338868, 0.0954874381005789),
    ("7290", 2, 27.223366499999994, 27.227078786589338, 19.05895515061251, 35.395202422566165),
]


@pytest.mark.parametrize("case", TRIANGLES)
def test_actual_three_triangle_modes_and_bounds_are_native_parameters(case):
    root, ei, nominal, mode, lo, hi = case
    doc = document([dict(amount=nominal, loc=mode, minimum=lo, maximum=hi, **{"uncertainty type": 5})])
    before = doc.data
    rows = native_rows(doc)
    result = decode(parse_native_rows(rows))[0]["exchanges"][0]
    assert result["amount"] == nominal != mode
    assert (result["loc"], result["minimum"], result["maximum"]) == (mode, lo, hi)
    assert result["uncertainty type"] == 5 and "scale" not in result
    assert doc.data == before


def test_triangle_native_unit_factor_changes_all_dimensionful_fields_once():
    doc = document(
        [
            dict(
                name="Water",
                amount=0.1,
                loc=0.15,
                minimum=0.05,
                maximum=0.25,
                unit="cubic meter",
                **{"uncertainty type": 5},
            )
        ]
    )
    result = decode(parse_native_rows(native_rows(doc)))[0]["exchanges"][0]
    assert result["unit"] == "kg"
    assert (result["amount"], result["loc"], result["minimum"], result["maximum"]) == (100, 150, 50, 250)


def test_original_prefix_parameter_formula_survives_mixed_generated_graph():
    source = document(
        [dict(amount=4, formula="2*bpspg0", **{"uncertainty type": 0}), dict(amount=100, loc=math.log(150), scale=0.3)],
        parameters=[dict(name="bpspg0", amount=2)],
    )
    before = source.data
    prepared, specs = prepare(source)
    original = prepared.data
    decoded = decode(original)
    assert decoded[0]["exchanges"][1]["formula"] == "2*bpspg0"
    assert decoded[0]["exchanges"][1]["amount"] == 4
    assert decoded[0]["parameters"] == [dict(name="bpspg0", amount=2)]
    assert specs[0].gate != "bpspg0"
    assert decoded[0]["exchanges"][2]["loc"] == math.log(150)
    assert source.data == before and prepared.data == original


@pytest.mark.parametrize("role", ["gate", "latent"])
def test_recognized_generated_helper_leak_is_still_refused(role):
    prepared, specs = prepare(document())
    data = prepared.data
    name = getattr(specs[0], role)
    data[0]["exchanges"].append(dict(name="ordinary", type="biosphere", unit="kilogram", amount=0, formula="2*" + name))
    with pytest.raises(GateError):
        decode(data)


def test_ordinary_both_prefix_names_without_conditional_are_not_claimed():
    data = [
        dict(
            parameters=[dict(name="bpspg0", amount=2), dict(name="bpspl0", amount=3)],
            exchanges=[dict(amount=5, type="biosphere", formula="bpspg0+bpspl0")],
        )
    ]
    assert decode(data) == data


@pytest.mark.parametrize(
    "wrapper,factor",
    [
        ("-(-({graph}))", 1),
        ("(-({graph}))", -1),
        ("(({graph}) * 1000) * 0.001", 1),
        ("({graph}) / 1000", 0.001),
        ("1000 * ({graph})", 1000),
    ],
)
def test_exact_normalized_conditional_and_signed_unit_wrappers(wrapper, factor):
    prepared, specs = prepare(document())
    data = prepared.data
    s = specs[0]
    graph = f"(exp({s.latent}) if abs({s.gate})>0 else 100)"
    data[0]["exchanges"][1]["formula"] = wrapper.format(graph=graph)
    result = decode(data)[0]["exchanges"][1]
    assert result["amount"] == 100 * factor
    assert result["loc"] == math.log(150) + math.log(abs(factor))
    assert result["scale"] == 0.3 and result["negative"] == (factor < 0)


@pytest.mark.parametrize(
    "wrapper", ["({graph})+1", "1/({graph})", "({graph})/0", "({graph})*0", "(" + "-" * 34 + "({graph}))"]
)
def test_unknown_or_degenerate_shapes_are_preserved_without_fabricated_uncertainty(wrapper):
    data = prepare(document())[0].data
    data[0]["exchanges"][1]["formula"] = wrapper.format(graph=data[0]["exchanges"][1]["formula"])
    before = deepcopy(data)
    assert decode(data) == before
    assert data == before
