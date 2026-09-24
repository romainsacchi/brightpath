Export UVEK inventories to ecoinvent
====================================

BrightPath provides a reviewed, directional technosphere route from UVEK 2025
cut-off to ecoinvent 3.12 cut-off, composed with ecoinvent reverse migrations for
3.6, 3.7, 3.8, 3.9.1, 3.10.1 and 3.11 cut-off. This does not invert the heuristic
ecoinvent-to-UVEK mapping or claim scientific equivalence between databases.

Older targets require ``on_inferred_reverse=PolicyAction.WARN`` as well as
``on_information_loss=PolicyAction.WARN``. Ambiguous suppliers and missing links
remain errors. Route availability is not a promise that every inventory converts.
In the archived CLIC CdTe basket, targets 3.10.1 through 3.12 pass; 3.6 through
3.9.1 currently encounter an ambiguous reverse mapping for waste incineration.

Patch routes preserve shared identities and validate against exact catalogs.
The 3.10/3.10.1 identity sets match; background coefficients and LCIA results do
not. The 3.9.1 catalog removes 17 supplier identities and adds nine biosphere
identities relative to the packaged 3.9 catalog. Removed suppliers fail validation,
even under permissive policy. ``docs/patch-catalog-comparison.json`` records counts
and digests; ``scripts/build_patch_catalogs.py`` reproduces this comparison.

The 3.12 unspecified-organic-chemical market maps backwards to the older generic
organic-chemical market as an explicit, policy-controlled proxy. Its composition
is different; the change is not an equivalence claim. The evidence and rationale
are recorded in the packaged compatibility resource and conversion report.

Generated helpers start in the reviewed recipe's 3.12 background and are migrated
with the rest of the converted inventory when an older target is requested.

.. code-block:: python

   from brightpath import (
       BackgroundContext, BiosphereProfile, BrightwayInventory,
       FormatProfile, InventoryContext, TechnosphereProfile,
   )
   from brightpath.core.policies import MigrationPolicy, PolicyAction

   source = InventoryContext(
       FormatProfile("brightway_excel"),
       BackgroundContext(
           TechnosphereProfile("uvek", "2025", "cutoff"),
           BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   inventory = BrightwayInventory.from_excel("uvek-inventory.xlsx", context=source)
   converted = inventory.migrate_background(
       BackgroundContext(
           TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
           BiosphereProfile("ecoinvent", "3.12"),
       ),
       policy=MigrationPolicy(on_information_loss=PolicyAction.WARN),
   )
   converted.write_excel("ecoinvent-inventory.xlsx")

The warning policy explicitly accepts documented proxies, not unresolved
suppliers. Strict policy rejects proxy use. Missing directional rules remain
errors, including under permissive diagnostic policy. An unused unresolved
catalog entry does not prevent conversion of an otherwise covered inventory.

Coverage and review
-------------------

All 11,747 packaged UVEK identities have a disposition: 167 reviewed
correspondences, 180 documented proxies, seven public supporting recipes, and
11,393 unresolved identities. Most of the catalog still needs substantive
directional evidence review. The unresolved disposition is not a scientific
review or approval of its suggested candidates.

The initial approved evidence comes from the IEA PVPS T12-33:2026 conversion
ledger and public supporting recipes, plus three explicitly documented freight
proxies. European and Swiss weight-specific freight may fall back to a European
unspecified-size diesel fleet market; payload, vehicle size and Swiss geography
are not reproduced. Those coarse decisions are warnings, not equivalent matches.

``scripts/generate_uvek_export.py`` regenerates the resource, catalog review,
and integrity manifest from ``docs/uvek_export_evidence.json`` and
``docs/uvek_export_overrides.json``. Optional explicit ``--pv-*`` inputs rebuild
the evidence from local source artifacts. Neither execution nor ordinary resource
generation needs a local premise checkout. Candidate suggestions from the
ecoinvent-to-UVEK mapping never become approved rules automatically.

Generated foreground helpers
----------------------------

One-to-many recipes create export-only foreground datasets. The original datasets
remain first in ``converted.data``; required helpers are appended in stable order.
Consumers keep their original quantity and uncertainty on the helper exchange.
Recipe quantities and uncertainty are recorded inside the helper. Helpers are
deduplicated, carry deterministic codes and ``brightpath_conversion`` provenance,
and are validated against the target profile. Applications must export the entire
returned inventory, not only ``converted.data[0]``.

Original dataset identities, production exchanges, and foreground links remain
unchanged. Migration works on a copy and rolls back on error. Changed units or
waste signs require explicit factors, including consistent uncertainty scaling.
Only uncertainty types 0 through 4 support non-unit scaling; other cases fail
rather than preserving inconsistent distribution parameters.

Reports record each applied rule and helper, proxy limitations, source/target
catalog digests, and resource provenance. The public
``brightpath.background.migration_resource_fingerprint()`` identifies the engine
revision and resource manifest for application caches. Increment the execution
revision when changing migration semantics without a package-version change.

Only foreground inventories are exported. Users must install their licensed
ecoinvent 3.12 cut-off background separately. Regional water detail already lost
during source normalization is not reconstructed by this conversion.

PV regression baseline
----------------------

The source-backed 317-dataset PV draft converts to 322 datasets, including five
required helpers. All 1,577 original foreground links remain unchanged, and all
7,334 resulting exchanges pass the checked Excel identity/amount/uncertainty
round trip. CLIC exports the complete package as Brightway Excel, SimaPro CSV,
openLCA JSON-LD and Brightway JSON 1.1.

After expanding supporting recipes, 286 of the original 317 datasets match the
previously prepared ecoinvent workbook's aggregated exchange identities and
amounts. The remaining 31 differ in exactly two documented groups: 19 freight
supplier choices affected by the canonical UVEK proxies, and 12 aqueous copper
flow labels (``Copper ion`` rather than ``Copper``). Both copper names exist in
the target catalog; their characterization equivalence is not established by
link validation. No values were adjusted to force agreement. This comparison
does not establish LCIA equivalence or validate the scientific proxy assumptions.
