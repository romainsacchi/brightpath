Recycled tissue-paper output migration
======================================

Scope and cause
---------------

UVEK 2025 ``Paper, recycling, with deinking, at plant`` (RER, kilogram)
already has an approved mapping to ecoinvent 3.12 ``tissue paper production,
recycled`` (reference product ``tissue paper``, RER, kilogram). The composed
reverse route failed at 3.10/3.10.1 because the packaged 3.10-to-3.11
resource only renamed the activity for reference product ``waste paper,
sorted``. That is a different exchange identity. The missing tissue-paper
output rule also blocked the forward 3.10/3.10.1-to-3.11 route.

The correction adds exactly one reviewed replacement to
``ecoinvent/cutoff/ecoinvent-3.10-cutoff-ecoinvent-3.11-cutoff.json``:

* Source: ``tissue paper production``, ``tissue paper``, RER, kilogram.
* Target: ``tissue paper production, recycled``, ``tissue paper``, RER, kilogram.

The two upstream waste-paper rules remain unchanged. This is not a generic
name-only match, a switch to virgin paper, or a geographic fallback. It does
not add mappings for other products, locations, units or consequential models.

Evidence reviewed on 2026-10-08
-------------------------------

The licensed, unmodified databases ``ecoinvent-3.10-cutoff`` and
``ecoinvent-3.11-cutoff`` were inspected read-only in their corresponding
Brightway projects. Each exact name/product/location selected one activity.
Metadata establishes recycled, deinked tissue-paper production in both
releases, with the same product UUID, geography, unit and system boundaries.
The full process descriptions match after whitespace normalization; only
their digest and sparse identity metadata are recorded here, not proprietary
inventories or full descriptions.

* 3.10 activity UUID: ``45fec413-e9f2-5f91-8b12-47fd57e1eb08``.
* 3.11 activity UUID: ``50250165-b753-5764-831a-54321c63620c``.
* Shared tissue-paper product UUID: ``e04720d0-1b18-41bd-b729-93cfc5f862ab``.
* Whitespace-normalized UTF-8 description SHA-256:
  ``39e96448ebec23d4fdd3698cb6c4875f04730f3785226238a012e9dbea1982a9``.

To repeat the review, select the exact cut-off databases and identities above,
require one match in each, and compare the ``activity``, ``flow``, ``unit``,
``location`` and ``comment`` metadata. Normalize each description with
``" ".join(comment.split())`` before hashing. The original EcoSpold filename
is the activity UUID, an underscore, the product UUID and ``.spold``.
Independently verify the source identity in both packaged 3.10 and 3.10.1
catalogs and the target identity in the 3.11 and 3.12 catalogs. A separate
licensed 3.10.1 process inventory was not used: its exact supplier identity
is validated through the existing patch route, without claiming coefficient
equivalence to 3.10.

Safeguards
----------

The replacement retains quantities, uncertainty and exchange direction and
works in both directions through the existing migration engine. Backward
migrations still require explicit permission for inferred reverse routes.
Exact-target validation and transactional rollback remain mandatory. This
identity correction does not establish equal background coefficients or
LCIA scores across releases, nor change the limitations of the original
UVEK-to-ecoinvent mapping.

The migration resource manifest and exhaustive audit baseline must be
updated together after reviewing the affected cases. The new manifest also
changes CLIC's conversion fingerprint when the corrected dependency is
installed. Source and installed-wheel tests cover native forward/reverse
routes, composed UVEK routes, unreviewed identities, negative substitutions,
uncertainty, idempotence and source immutability. No published CLIC snapshot
or Django schema is changed.

Validation evidence
-------------------

The source suite passes 1,377 tests, including 35 new paper regressions;
88 focused tests also pass against the separately installed candidate wheel.
Formatting, lint, security checks and the Sphinx warning-as-error build pass.
The complete source and installed-wheel audits agree on every probe. Seven
previously blocked paper cases now pass; all other 42,437 cases are unchanged.
The reviewed baseline comparison passes for both reports, with no new
collisions or unexpected exceptions. See :doc:`migration-audit` for remaining
known limitations rather than interpreting a green gate as universal support.

CLIC also passes 261 download/dependency tests against the candidate wheel.
The public Zenodo archive 22850224 contains two direct paper consumers:
published versions 3940 (DE) and 3941 (CN), both named ``photovoltaic cell
production, single-crystalline silicon, topcon, simulated data``. Their
20-version foreground closure fails against released 1.0.0a5 at 3.10.1,
then passes all nine combinations of Brightway, SimaPro and openLCA at
3.10.1, 3.11 and 3.12 with the candidate. Each includes two helpers;
Brightway round trips preserve 832 / 833 / 833 exchanges respectively,
including uncertainty, while source snapshots remain unchanged.

A separate synthetic paper foreground passes all three export/reimport
formats across seven CLIC targets, including parameter and uncertainty
preservation. This correction is included in BrightPath 1.0.0a6. Publishing
the dependency does not change a running CLIC deployment: its dependency pin
and both web and export-worker images must also be updated.
