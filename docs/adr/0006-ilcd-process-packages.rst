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

Support one positive product reference output per process. Preserve unit
process versus aggregated-inventory declarations without computing either.
Reject allocation, reminder exchanges, waste-treatment reference inputs,
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
the convention. Only default-provider exchange extensions are emitted, since
complete openLCA exchange extensions bypass its standard uncertainty importer.

Consequences
------------

Each process snapshot retains its XML and reference closure in a JSON-compatible
``ilcd`` extension, allowing CLIC to export independently stored processes.
Canonical fields control exported quantities; XML templates preserve unmapped
metadata. Edited inventories cannot reissue stale reviews, compliance claims
or LCIA results without explicit correction.

Numeric and mapping failures remain errors under permissive conversion policy.
The standard uncertainty field has limited decimal precision; unsupported
dispersion precision is an error rather than an undocumented rounding change.
