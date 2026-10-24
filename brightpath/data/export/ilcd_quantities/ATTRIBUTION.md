# ILCD quantity-reference provenance

`openlca_2_6_2.json` records public reference identifiers, unit names, and
conversion factors extracted from the `units.zolca` template shipped with
openLCA 2.6.2. It contains no inventory exchanges, database coefficients,
characterization factors, or proprietary background inventories.

The source template SHA-256 and generator are recorded in the JSON. Generate
it with `scripts/generate_ilcd_quantity_catalog.py` from an explicit extract;
BrightPath never reads a local openLCA installation at runtime. The adjacent
manifest verifies the packaged bytes.

The selections and XML rendering are maintained by BrightPath. openLCA's
EcoSpold import ID convention is documented in the GreenDelta source:
https://github.com/GreenDelta/olca-modules/blob/master/olca-io/README.md
and `org.openlca.util.KeyGen`. This is reference metadata, not a claim of EF
compliance or compatibility with arbitrary database builds.
