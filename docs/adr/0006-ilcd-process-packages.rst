ADR 0006: Exchange ILCD 1.1 single-reference processes
======================================================

Status
------

Accepted for the first process adapter.

Decision
--------

Register ``ilcd`` independently of background family, exact version and
system model. Read ZIPs, package directories and standalone XML with an
explicit local reference directory. Write deterministic ZIPs and new package
directories. Validate with pinned, integrity-checked schemas offline.

Support one product reference output or waste reference input per process.
Encode negative canonical production as a waste input with positive magnitude,
and linked treatment coefficients as waste outputs with the opposite sign.
Reverse this coordinate change on import, including formulas and uncertainty.
Preserve unit process versus aggregated-inventory declarations without computing either.
Reject allocation, reminder exchanges, non-reference waste inputs,
additional product outputs, metadata-only inventories, eILCD models and EPDs.

Resolve references by dataset type, UUID and exact dataset version, checking
relative URI consistency. Missing reference versions may resolve only when
unique. Background identities use exact catalog evidence, explicit mappings or
preserved canonical evidence followed by independent background validation.
For the openLCA 2.6.2 target, generate reference XML only for used exchanges from
packaged ecoinvent UUID catalogs or the verified UVEK openLCA build. Explicit and
preserved mappings take precedence. Convert quantities and uncertainties to the
reference unit together, retaining canonical conversion evidence for reimport.

The ``core`` dialect rejects selected-supplier loss. The ``openlca`` dialect
uses the documented default-provider attributes to retain selected suppliers.
Neither dialect redistributes background process inventories or promises
automatic EF-to-ecoinvent matching or ILCD Data Network compliance.

The openLCA dialect uses its direct normal/geometric-standard-deviation
convention; core uses the ILCD 95-percent field definitions. Source inference
is reported and can be overridden for unmarked inputs. Exported XML declares
the convention. Only default-provider and triangular-mode exchange extensions are emitted, since
complete openLCA exchange extensions bypass its standard uncertainty importer.
Triangular mode uses the native ``mostLikelyValue`` attribute in the ``openlca``
dialect, independently of the nominal amount. Core triangular export remains
unsupported. No native dispersion extension exists in openLCA 2.6.2; rounding
the standard dispersion field would lose uncertainty even with a warning.

Foreground tonne-kilometres use the pinned native mass-transport quantity with
``t*km`` reference unit and factor 1. Standard cubic metres remain unmapped
without explicit verified evidence; the native volume template does not define
standard conditions.

Consequences
------------

Each process snapshot retains its XML and reference closure in a JSON-compatible
``ilcd`` extension, allowing CLIC to export independently stored processes.
Canonical fields control exported quantities; XML templates preserve unmapped
metadata. Edited inventories cannot reissue stale reviews, compliance claims
or LCIA results without explicit correction.

Numeric and mapping failures remain errors under permissive conversion policy.
The standard uncertainty field has limited decimal precision; unsupported
dispersion precision is an error by default. An explicit programmatic
``uncertainty_relative_tolerance`` can authorize openLCA export rounding up to
0.5% relative change in log-standard-deviation or normal standard deviation.
It cannot replace positive spread with zero or relax other scientific guards.
Rounding follows native unit/sign scaling, is reported in structured preflight
warnings and an optional transactional list collector, and preserves original
parameters in native process comments. Readers always return actual emitted
uncertainty, never original parameters from audit metadata. Core remains strict.
