from copy import deepcopy

import pytest

from brightpath.background.catalogs import (
    BiosphereCatalog,
    CatalogProvider,
    InMemoryCatalogProvider,
    TechnosphereCatalog,
)
from brightpath.background.validation import validate_background_links
from brightpath.core.context import BackgroundContext, BiosphereProfile, TechnosphereProfile
from brightpath.core.reports import StageKind


def background_context(
    *,
    technosphere_family="ecoinvent",
    technosphere_version="3.10.1",
    biosphere_family="ecoinvent",
    biosphere_version="3.10.1",
):
    return BackgroundContext(
        technosphere=TechnosphereProfile(technosphere_family, technosphere_version, "cutoff"),
        biosphere=BiosphereProfile(biosphere_family, biosphere_version),
    )


def dataset(*exchanges):
    return {
        "name": "foreground process",
        "reference product": "foreground product",
        "location": "CH",
        "unit": "kilogram",
        "exchanges": list(exchanges),
    }


def technosphere_exchange(name="background process", product="background product"):
    return {
        "name": name,
        "reference product": product,
        "location": "GLO",
        "unit": "kilogram",
        "type": "technosphere",
        "amount": 1,
    }


def biosphere_exchange(name="Carbon dioxide, fossil"):
    return {
        "name": name,
        "categories": ("air", "urban air close to ground"),
        "unit": "kilogram",
        "type": "biosphere",
        "amount": 1,
    }


def test_validates_independent_exact_technosphere_and_biosphere_profiles():
    context = background_context(
        technosphere_family="uvek",
        technosphere_version="2025.0",
        biosphere_version="3.10.1",
    )
    foreground_link = technosphere_exchange("foreground process", "foreground product")
    foreground_link["location"] = "CH"
    provider = InMemoryCatalogProvider(
        technosphere=[
            TechnosphereCatalog(
                context.technosphere,
                {("background process", "background product", "GLO", "kilogram")},
                digest="a" * 64,
                source="test-technosphere.json",
            )
        ],
        biosphere=[
            BiosphereCatalog(
                context.biosphere,
                {("Carbon dioxide, fossil", ("air", "urban air close to ground"), "kilogram")},
                digest="b" * 64,
                source="test-biosphere.json",
            )
        ],
    )

    report = validate_background_links(
        [dataset(foreground_link, technosphere_exchange(), biosphere_exchange())],
        context,
        provider,
    )

    assert report.stage is StageKind.BACKGROUND_VALIDATION
    assert report.label == "background links"
    assert not report.has_errors
    assert report.metrics["context"]["technosphere"] == {
        "family": "uvek",
        "version": "2025.0",
        "system_model": "cutoff",
    }
    assert report.metrics["context"]["biosphere"] == {
        "family": "ecoinvent",
        "version": "3.10.1",
    }
    assert report.metrics["technosphere"]["foreground_links"] == 1
    assert report.metrics["technosphere"]["catalog_links"] == 1
    assert report.metrics["technosphere"]["coverage"] == 1.0
    assert report.metrics["biosphere"]["coverage"] == 1.0
    assert report.metrics["technosphere"]["catalog"]["digest"] == "a" * 64


def test_reports_each_unresolved_axis_at_a_stable_canonical_path():
    context = background_context()
    provider = InMemoryCatalogProvider(
        technosphere=[TechnosphereCatalog(context.technosphere, set())],
        biosphere=[BiosphereCatalog(context.biosphere, set())],
    )

    report = validate_background_links(
        [dataset(technosphere_exchange(), biosphere_exchange())],
        context,
        provider,
    )

    by_code = {issue.code: issue for issue in report.issues}
    assert by_code["background.technosphere_link_unresolved"].path == "datasets[0].exchanges[0]"
    assert by_code["background.biosphere_link_unresolved"].path == "datasets[0].exchanges[1]"
    assert by_code["background.technosphere_link_unresolved"].details["profile"]["version"] == "3.10.1"
    assert report.metrics["technosphere"]["unresolved_links"] == 1
    assert report.metrics["biosphere"]["unresolved_links"] == 1
    assert report.metrics["technosphere"]["coverage"] == 0.0
    assert report.metrics["technosphere"]["validation_coverage"] == 1.0
    with pytest.raises(TypeError):
        by_code["background.biosphere_link_unresolved"].details["identity"]["name"] = "changed"


def test_distinguishes_missing_technosphere_and_biosphere_catalogs():
    context = background_context()

    report = validate_background_links(
        [dataset(technosphere_exchange(), biosphere_exchange())],
        context,
        InMemoryCatalogProvider(),
    )

    assert {issue.code for issue in report.issues} == {
        "background.technosphere_catalog_missing",
        "background.biosphere_catalog_missing",
    }
    assert {issue.path for issue in report.issues} == {"background.technosphere", "background.biosphere"}
    for axis in ("technosphere", "biosphere"):
        assert report.metrics[axis]["catalog"]["status"] == "missing"
        assert report.metrics[axis]["unchecked_links"] == 1
        assert report.metrics[axis]["validation_coverage"] == 0.0


def test_one_missing_catalog_does_not_prevent_validation_of_the_other_axis():
    context = background_context()
    provider = InMemoryCatalogProvider(
        biosphere=[
            BiosphereCatalog(
                context.biosphere,
                {("Carbon dioxide, fossil", ("air", "urban air close to ground"), "kilogram")},
            )
        ]
    )

    report = validate_background_links(
        [dataset(technosphere_exchange(), biosphere_exchange())],
        context,
        provider,
    )

    assert [issue.code for issue in report.issues] == ["background.technosphere_catalog_missing"]
    assert report.metrics["technosphere"]["catalog"]["status"] == "missing"
    assert report.metrics["biosphere"]["catalog"]["status"] == "loaded"
    assert report.metrics["biosphere"]["resolved_links"] == 1


def test_additional_foreground_targets_are_read_only_and_do_not_require_a_catalog():
    context = background_context()
    inventory = [dataset(technosphere_exchange())]
    original = deepcopy(inventory)

    report = validate_background_links(
        inventory,
        context,
        InMemoryCatalogProvider(),
        foreground_technosphere_targets=[
            ("background process", "background product", "GLO", "kilogram"),
        ],
    )

    assert inventory == original
    assert not report.issues
    assert report.metrics["technosphere"]["foreground_links"] == 1
    assert report.metrics["technosphere"]["catalog"]["status"] == "not_required"
    assert report.metrics["technosphere"]["coverage"] == 1.0
    assert report.metrics["biosphere"]["catalog"]["status"] == "not_required"


class WrongProfileProvider(CatalogProvider):
    def load_technosphere(self, profile):
        wrong = TechnosphereProfile(profile.family, "3.9", profile.system_model)
        return TechnosphereCatalog(wrong, set())

    def load_biosphere(self, profile):
        wrong = BiosphereProfile(profile.family, "3.9")
        return BiosphereCatalog(wrong, set())

    def technosphere_profiles(self):
        return ()

    def biosphere_profiles(self):
        return ()


def test_rejects_catalogs_for_a_different_exact_profile_in_the_report():
    context = background_context()

    report = validate_background_links(
        [dataset(technosphere_exchange(), biosphere_exchange())],
        context,
        WrongProfileProvider(),
    )

    assert {issue.code for issue in report.issues} == {
        "background.technosphere_catalog_invalid",
        "background.biosphere_catalog_invalid",
    }
    assert report.metrics["technosphere"]["catalog"]["status"] == "invalid"
    assert report.metrics["biosphere"]["catalog"]["status"] == "invalid"
    assert report.metrics["technosphere"]["unchecked_links"] == 1


@pytest.mark.parametrize(
    "targets",
    ["not-an-identity", [("only", "three", "fields")]],
)
def test_rejects_invalid_additional_foreground_target_contracts(targets):
    context = background_context()

    with pytest.raises(TypeError, match="four-field identities"):
        validate_background_links(
            [],
            context,
            InMemoryCatalogProvider(),
            foreground_technosphere_targets=targets,
        )
