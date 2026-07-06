# BrightPath
[![License: BSD 3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue?style=flat-square)](https://opensource.org/license/bsd-3-clause)

BrightPath bridges life-cycle assessment (LCA) data between
[Brightway2](https://brightway.dev/) and [SimaPro](https://simapro.com/). It
bundles the mappings that are required to translate units, flow names and
metadata between both tools and exposes high-level helpers to perform the
conversion in either direction.

## Features

* Convert Brightway2 inventories exported as Excel spreadsheets to the SimaPro
  CSV format.
* Import SimaPro CSV exports and normalise them so that they can be registered
  as Brightway databases.
* Ship curated mappings for biosphere flows, technosphere exchanges,
  sub-compartments and blacklist entries required during the conversion.

## Installation

Install BrightPath from PyPI using [pip](https://pip.pypa.io/):

```bash
pip install brightpath
```

## Usage

### Convert Brightway inventories to SimaPro CSV

```python
from brightpath import BrightwayConverter

converter = BrightwayConverter(
    filepath="/path/to/brightway-export.xlsx",
    metadata="/path/to/metadata.yaml",  # optional
    ecoinvent_version="3.9",
)

# Write the converted inventory to a CSV file (defaults to the current
# working directory unless ``export_dir`` is provided during initialisation).
output_path = converter.convert_to_simapro(database="ecoinvent")
print(output_path)
```

The converter also accepts inventory data that has already been loaded into
memory via the ``data`` argument and can return the converted rows directly by
calling ``convert_to_simapro(format="data")``.

### Convert SimaPro CSV exports to Brightway datasets

```python
from brightpath import SimaproConverter

converter = SimaproConverter(
    filepath="/path/to/simapro-export.csv",
    ecoinvent_version="3.9",
    db_name="my-simapro-import",
)

# Normalise exchange names, locations and metadata so that they align with
# Brightway conventions.
converter.convert_to_brightway()

# The processed data lives on ``converter.i`` (an instance of
# ``bw2io.SimaProCSVImporter``). You can now write the database to your
# Brightway project if desired:
# converter.i.write_database()
```

You can also write a Brightway Excel workbook directly, without first creating
a Brightway database:

```python
path = converter.convert_to_brightway(
    format="excel",
    filename="lci-my-simapro-import.xlsx",
)
print(path)
```

### Analyze uploaded inventories without converting them

BrightPath also exposes an additive analysis API that returns structured file
issues and candidate dataset summaries. This is intended for intake workflows
such as upload validation in web applications.

```python
from brightpath.analysis import analyze_inventory, validate_inventory
from brightpath.models import BackgroundProfile

result = analyze_inventory(
    path="/path/to/brightway-export.xlsx",
    source_profile=BackgroundProfile(
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
    ),
)

for candidate in result.candidates:
    print(candidate.name, candidate.reference_product, candidate.location)

validate_inventory(
    path="/path/to/brightway-export.xlsx",
    source_profile=BackgroundProfile(
        family="ecoinvent",
        version="3.10",
        system_model="cutoff",
    ),
)
```

The first analysis milestone currently supports:

* Brightway Excel workbooks (`.xlsx`)
* Brightway delimited exports in the `bw2io` block format (`.csv`, `.tsv`)
* SimaPro CSV exports (`.csv`)

For Brightway analysis, BrightPath validates the inventory as Brightway data,
can infer the intended background family/version/system model from local
reference catalogs, and raises explicit validation errors when technosphere or
biosphere exchanges do not link to the uploaded inventory or the selected
background catalog. When several local reference catalogs match equally well,
BrightPath defaults to the most recent matching version and prefers `cutoff`,
while returning a warning so the calling workflow can let the user override
that choice. SimaPro-specific metadata such as `simapro category` on
production exchanges is enforced only in actual Brightway-to-SimaPro
conversion paths, not during upload-intake analysis.

## Development

* Source code is formatted with standard Python tools and tested with
  `pytest`.
* Data files required for the conversions live under `brightpath/data`.
* Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on running tests
  and submitting pull requests.

## License

BrightPath is distributed under the
[BSD-3-Clause license](LICENSE).
