import math
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from brightpath import BrightwayInventory, FormatProfile, InventoryContext
from brightpath.analysis import analyze_inventory
from brightpath.formats.openlca_jsonld import write_openlca_jsonld
from brightpath.models import InventoryDocument

SCOPES = ("parameters", "database parameters", "project parameters")
IDENTITY_FIELDS = ("name", "reference product", "location", "unit")


@dataclass(frozen=True)
class Profile:
    family: str
    version: str
    system_model: str = "cutoff"

    @property
    def key(self):
        return f"{self.family}/{self.version}/{self.system_model}"

    @property
    def biosphere(self):
        return ("ecoinvent", "3.10") if self.family == "uvek" else (self.family, self.version)


def identity(record):
    return tuple(record.get(key, "") for key in IDENTITY_FIELDS)


def background_context(profile):
    from brightpath import BackgroundContext, BiosphereProfile, TechnosphereProfile

    return BackgroundContext(TechnosphereProfile(**asdict(profile)), BiosphereProfile(*profile.biosphere))


def synthetic_snapshot(supplier):
    activity = {
        "name": "BrightPath roundtrip fixture",
        "reference product": "synthetic output",
        "location": "GLO",
        "unit": "kilogram",
    }
    return {
        **activity,
        "comment": "Synthetic matrix fixture; no background inventory is redistributed.",
        "parameters": [
            {
                "name": "efficiency",
                "amount": 2.0,
                "unit": "dimensionless",
                "comment": "Synthetic assumption",
            },
            {"name": "dose", "amount": 12.0, "formula": "shared * efficiency"},
        ],
        "project parameters": [{"name": "factor", "amount": 2.0}],
        "database parameters": [
            {
                "name": "rate",
                "amount": 3.0,
                "uncertainty type": 3,
                "loc": 3.0,
                "scale": 0.2,
            },
            {"name": "shared", "amount": 6.0, "formula": "factor * rate"},
        ],
        "exchanges": [
            {
                **activity,
                "type": "production",
                "amount": 1.0,
                "formula": "efficiency / 2",
                "simapro category": "Materials/Other",
            },
            {
                **supplier,
                "type": "technosphere",
                "amount": 0.25,
                "uncertainty type": 3,
                "loc": 0.25,
                "scale": 0.01,
            },
            {
                "type": "biosphere",
                "name": "Carbon dioxide, fossil",
                "unit": "kilogram",
                "categories": ["air"],
                "amount": 12.0,
                "formula": "dose",
            },
        ],
    }


def assert_number(actual, expected, label):
    if (
        type(actual) not in (int, float)
        or not math.isfinite(actual)
        or not math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    ):
        raise AssertionError(f"Changed {label}: {actual!r} instead of {expected!r}.")


def assert_parameters_preserved(expected, actual):
    for scope in SCOPES:
        parameters = {parameter["name"]: parameter for parameter in actual.get(scope, [])}
        expected_parameters = expected.get(scope, [])
        if set(parameters) != {parameter["name"] for parameter in expected_parameters}:
            raise AssertionError(f"Changed {scope} names.")
        for parameter in expected_parameters:
            for key, value in parameter.items():
                if key in {"openlca parameter", "brightpath parameter target", "group"}:
                    continue
                found = parameters[parameter["name"]].get(key)
                if type(value) in (int, float):
                    assert_number(found, value, f"{scope}.{parameter['name']}.{key}")
                elif found != value:
                    raise AssertionError(f"Changed {scope}.{parameter['name']}.{key}: {found!r} instead of {value!r}.")


def exchange_identity(exchange):
    categories = tuple(exchange.get("categories") or ()) if exchange.get("type") == "biosphere" else ()
    return exchange.get("type"), identity(exchange), categories


def assert_preserved(expected, actual):
    if identity(actual) != identity(expected):
        raise AssertionError(f"Changed activity identity: {identity(actual)} instead of {identity(expected)}.")
    assert_parameters_preserved(expected, actual)
    if len(actual.get("exchanges", [])) != len(expected.get("exchanges", [])):
        raise AssertionError("Changed the number of exchanges.")
    remaining = list(actual["exchanges"])
    for exchange in expected["exchanges"]:
        candidates = [item for item in remaining if exchange_identity(item) == exchange_identity(exchange)]
        if len(candidates) != 1:
            raise AssertionError(f"Exchange identity lost or ambiguous: {exchange_identity(exchange)}.")
        found = candidates[0]
        remaining.remove(found)
        assert_number(found.get("amount"), exchange["amount"], "exchange amount")
        if found.get("formula", "") != exchange.get("formula", ""):
            raise AssertionError(f"Changed exchange formula on {exchange['name']}.")
        for key in ("uncertainty type", "loc", "scale", "shape", "minimum", "maximum"):
            if exchange.get(key) is not None:
                assert_number(found.get(key), exchange[key], f"exchange {key}")


def export_snapshots(snapshots, profile, software):
    data = deepcopy(snapshots)
    shared = {}
    for scope in SCOPES[1:]:
        shared[scope.replace(" ", "_")] = data[0].get(scope, [])
        for activity in data:
            assert activity.pop(scope, []) == shared[scope.replace(" ", "_")]
    selected = InventoryContext(FormatProfile("brightway_excel"), background_context(profile))
    inventory = BrightwayInventory.from_data(data, context=selected, **shared)
    with TemporaryDirectory(prefix="brightpath-roundtrip-") as directory:
        destination = Path(directory)
        if software == "brightway":
            path = inventory.write_excel(destination / "inventory.xlsx")
        elif software == "simapro":
            path = inventory.to_simapro().write_csv(destination / "inventory.csv")
        else:
            selected = InventoryContext(FormatProfile("openlca_jsonld"), selected.background)
            document = InventoryDocument(data=data, context=selected, **shared)
            path = write_openlca_jsonld(document, destination / "inventory.zip")
        return SimpleNamespace(filename=path.name, content=path.read_bytes())


def import_artifact(artifact, profile):
    formats = {".xlsx": "brightway_excel", ".csv": "simapro_csv", ".zip": "openlca_jsonld"}
    selected = InventoryContext(FormatProfile(formats[Path(artifact.filename).suffix]), background_context(profile))
    with TemporaryDirectory(prefix="brightpath-roundtrip-") as directory:
        path = Path(directory) / artifact.filename
        path.write_bytes(artifact.content)
        result = analyze_inventory(path=path, source_context=selected)
    issues = [issue for issue in result.file_issues if issue.severity == "error"]
    issues.extend(issue for candidate in result.candidates for issue in candidate.issues if issue.severity == "error")
    if issues or not result.candidates:
        raise ValueError("Import rejected: " + "; ".join(issue.message for issue in issues))
    data = deepcopy(result.inventory_data)
    for activity in data:
        activity["database parameters"] = deepcopy(result.database_parameters)
        activity["project parameters"] = deepcopy(result.project_parameters)
    return data
