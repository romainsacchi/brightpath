from brightpath.capabilities import capability_snapshot, format_capabilities, migration_capabilities


def test_format_capabilities_are_derived_from_production_adapters():
    capabilities = {value.format_id: value for value in format_capabilities()}

    assert set(capabilities) == {
        "brightway_excel",
        "brightway_csv",
        "brightway_tsv",
        "ecospold1",
        "ecospold2",
        "ilcd",
        "openlca_jsonld",
        "simapro_csv",
    }
    assert capabilities["brightway_excel"].read == ("file",)
    assert capabilities["openlca_jsonld"].write == ("file",)
    assert capabilities["ecospold1"].write == ("file",)
    assert capabilities["ecospold2"].read == ("directory", "file")
    assert capabilities["ecospold2"].write == ("directory", "file")
    assert capabilities["simapro_csv"].write == ("file",)
    assert "openlca_excel" not in capabilities


def test_migration_capabilities_advertise_only_active_routes():
    capabilities = migration_capabilities()
    edges = {(value.axis, value.source_series, value.target_series) for value in capabilities}

    assert ("technosphere", "3.11", "3.12") in edges
    assert ("biosphere", "3.10", "3.11") in edges
    assert ("biosphere", "3.11", "3.12") in edges
    assert ("biosphere", "3.10", "3.10.1") in edges
    assert ("technosphere", "3.9", "3.9.1") in edges
    assert {value.family for value in capabilities} == {"ecoinvent", "uvek"}
    exports = [
        value
        for value in capabilities
        if value.family == "uvek" and value.target_family == "ecoinvent" and value.axis == "technosphere"
    ]
    assert [(value.source_series, value.target_series, value.target_family) for value in exports] == [
        ("2025", "3.12", "ecoinvent")
    ]
    uvek = [
        value
        for value in capabilities
        if value.target_family == "uvek" and value.family == "ecoinvent" and value.axis == "technosphere"
    ]
    assert {(value.source_series, value.target_series) for value in uvek} == {
        (version, "2025") for version in ("3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12")
    }
    assert {value.system_model for value in uvek} == {"cutoff", "consequential"}
    assert {value.reverse for value in uvek} == {"unavailable"}
    release_edges = [value for value in capabilities if value.source_series == "2026" or value.target_series == "2026"]
    assert {
        (value.axis, value.family, value.source_series, value.target_family, value.target_series)
        for value in release_edges
    } == {
        ("technosphere", "uvek", "2025", "uvek", "2026"),
        ("technosphere", "uvek", "2026", "uvek", "2025"),
        ("biosphere", "uvek", "2026", "ecoinvent", "3.10"),
        ("biosphere", "ecoinvent", "3.10", "uvek", "2026"),
    }


def test_capability_snapshot_lists_catalog_axes_independently():
    snapshot = capability_snapshot()

    assert snapshot["formats"]
    assert snapshot["migrations"]
    assert {item["family"] for item in snapshot["catalogs"]["technosphere"]} == {
        "ecoinvent",
        "uvek",
    }
    assert {item["family"] for item in snapshot["catalogs"]["biosphere"]} == {
        "ecoinvent",
        "uvek",
    }
