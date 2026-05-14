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

## Development

* Source code is formatted with standard Python tools and tested with
  `pytest`.
* Data files required for the conversions live under `brightpath/data`.
* Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidance on running tests
  and submitting pull requests.

## License

BrightPath is distributed under the
[BSD-3-Clause license](LICENSE).

