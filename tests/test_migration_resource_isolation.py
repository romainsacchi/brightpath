"""Migration outputs must never alias cached rule values or alter resource verbs."""

from copy import deepcopy

import pytest

from brightpath.background.execution import _apply_biosphere_step, _apply_technosphere_step, _resource_for_step
from brightpath.background.migration import MigrationRouteStep
from brightpath.core.policies import MigrationPolicy, PolicyAction


@pytest.mark.parametrize("direction", ["forward", "backward"])
@pytest.mark.parametrize("factor", [None, 0.001])
def test_biosphere_replacements_do_not_mutate_cached_rules_or_alias_output(direction, factor):
    source = {"name": "source", "categories": ["air"], "unit": "Becquerel" if factor else "kilogram"}
    target = {"name": "target", "categories": ["water"], "unit": "kilo Becquerel" if factor else "kilogram"}
    rule = {"source": source, "target": target}
    if factor:
        rule["conversion_factor"] = factor
    resource = {"replace": [rule], "delete": [{"source": {"name": "absent"}}]}
    snapshot = deepcopy(resource)
    forward = direction == "forward"
    step = MigrationRouteStep(
        "biosphere", "3.10" if forward else "3.11", "3.11" if forward else "3.10", direction, "isolation"
    )
    local = _resource_for_step(step, {("3.10", "3.11"): resource})
    data = [{"exchanges": [{**deepcopy(source if forward else target), "type": "biosphere", "amount": 2}]}]
    report, _losses = _apply_biosphere_step(data, local, step, MigrationPolicy(), 0)
    assert not any(issue.severity == "error" for issue in report.issues)
    assert data[0]["exchanges"][0]["name"] == ("target" if forward else "source")
    data[0]["exchanges"][0]["categories"].append("caller mutation")
    assert resource == snapshot
    assert local == snapshot


@pytest.mark.parametrize("direction", ["forward", "backward"])
@pytest.mark.parametrize("verb", ["replace", "disaggregate"])
def test_technosphere_replacement_and_aggregation_outputs_do_not_alias_cached_rules(direction, verb):
    def supplier(name):
        return {"name": name, "reference product": name, "location": "GLO", "unit": "kilogram", "custom": [name]}

    source, target = supplier("source"), supplier("target")
    rule = (
        {"source": source, "target": target}
        if verb == "replace"
        else {"source": source, "targets": [{**target, "allocation": 1}]}
    )
    resource = {verb: [rule]}
    snapshot = deepcopy(resource)
    forward = direction == "forward"
    step = MigrationRouteStep(
        "technosphere", "3.10" if forward else "3.11", "3.11" if forward else "3.10", direction, "isolation"
    )
    local = _resource_for_step(step, {("3.10", "3.11"): resource})
    data = [{"exchanges": [{**deepcopy(source if forward else target), "type": "technosphere", "amount": 2}]}]
    report, _losses = _apply_technosphere_step(
        data, local, step, MigrationPolicy(on_information_loss=PolicyAction.WARN), 0
    )
    assert not any(issue.severity == "error" for issue in report.issues)
    assert data[0]["exchanges"][0]["name"] == ("target" if forward else "source")
    data[0]["exchanges"][0]["custom"].append("caller mutation")
    assert resource == snapshot
