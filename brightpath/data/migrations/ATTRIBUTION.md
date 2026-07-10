# Migration Resource Attribution

The ecoinvent migration JSON files in `ecoinvent/cutoff/` and `ecoinvent/biosphere/` were copied
from the `premise` repository at commit `43355ef7c21e8587812d1615cc956252c5a7c4e6`:

`premise/data/utils/import/migrations/`

Each JSON resource contains its own contributors, creation timestamp, generator version, homepage,
and license metadata. At the time of import, these resources identify `ecoinvent_migrate` as their
generator and declare the Creative Commons Attribution 4.0 International (`CC-BY-4.0`) license.

The inventory data represented by these mappings is not included. BrightPath packages only mapping
rules describing identity changes, disaggregation, aggregation, and biosphere migration between
ecoinvent releases.

The file under `uvek/` is a BrightPath-authored placeholder. It deliberately contains no usable
ecoinvent-to-UVEK mappings and is not registered as a migration route.
