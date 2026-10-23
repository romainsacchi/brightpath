ADR 0004: Restrict ecoSpold1 to single-output processes
========================================================

Status
------

Accepted.

Decision
--------

BrightPath registers ``ecospold1`` as an XML file adapter for process datasets
with exactly one reference production exchange and no allocation records. One
XML file may contain several datasets when every dataset satisfies this rule.
Directory, ZIP, multi-output, elementary-flow definition, and LCIA artifacts
remain outside the supported subset.

The reader validates XML against the ecoSpold1 XSD and retains provenance in a
namespaced ``ecospold1`` dataset mapping. The writer resolves imported metadata
with explicit document defaults and exact per-dataset overrides, validates the
rendered XML, and writes atomically.

Mandatory publication, person, classification, and temporal fields are never
derived from free-form ``comment`` or ``description`` text. Missing fields are
hard conversion errors under both strict and permissive policies. A caller may
provide explicit placeholder text, but BrightPath does not invent it.

Consequences
------------

The supported subset avoids ambiguous allocation semantics and produces
schema-valid artifacts with auditable provenance. Converting an inventory that
lacks structured metadata requires a sidecar profile. Multi-output files can be
read and inspected, but source-format validation prevents conversion through
the supported adapter contract.
