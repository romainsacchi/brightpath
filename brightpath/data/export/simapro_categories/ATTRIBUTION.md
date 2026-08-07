# SimaPro Category Catalog Provenance

The CSV resource in this directory contains aggregate links between reference
product identities, units, process roles, and SimaPro category paths. It does
not contain exchange amounts, process comments, geography-specific inventories,
or complete background datasets.

`ecoinvent__3.9.1__cutoff.csv` was generated with
`scripts/generate_simapro_category_catalog.py` from a locally exported,
licensed ecoinvent 3.9.1 cut-off database in SimaPro 9.5.0.2. The source export
is not distributed with BrightPath.

`RESOURCE_MANIFEST.json` records the exact SHA-256 hash, size, source profile,
category count, and aggregate row count. Its `legal_review_required` status is
intentional: maintainers must confirm that distribution of this derived
identity/category catalog is permitted under the applicable source-data
agreements before a stable public release. The manifest is an integrity and
provenance record, not a license grant.
