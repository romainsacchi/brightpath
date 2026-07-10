from brightpath.capabilities import capability_snapshot, format_capabilities, migration_capabilities


def test_format_capabilities_are_derived_from_production_adapters():
    capabilities = {value.format_id: value for value in format_capabilities()}

    assert set(capabilities) == {
        "brightway_excel",
        "brightway_csv",
        "brightway_tsv",
        "simapro_csv",
    }
    assert capabilities["brightway_excel"].read == ("file",)
    assert capabilities["simapro_csv"].write == ("file",)
    assert "openlca_excel" not in capabilities
    assert "ecospold2" not in capabilities


def test_migration_capabilities_do_not_advertise_missing_or_placeholder_routes():
    capabilities = migration_capabilities()
    edges = {(value.axis, value.source_series, value.target_series) for value in capabilities}

    assert ("technosphere", "3.11", "3.12") in edges
    assert ("biosphere", "3.10", "3.11") in edges
    assert ("biosphere", "3.11", "3.12") not in edges
    assert {value.family for value in capabilities} == {"ecoinvent"}
    assert all(value.reverse == "inferred_policy_controlled" for value in capabilities)


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
