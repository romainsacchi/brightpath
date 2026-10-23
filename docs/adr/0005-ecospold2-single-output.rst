ADR 0005: Restrict ecoSpold2 to single-output activities
========================================================

Status
------

Accepted.

Decision
--------

BrightPath registers ``ecospold2`` for XSD-valid activity files, ZIP packages,
and package directories. Each activity must have exactly one reference
production exchange. Child datasets, allocations, transfer coefficients,
substitution exchanges, multi-output activities, LCIA datasets, and master-data
definition imports remain outside the supported conversion route.

The writer creates one activity file per dataset and minimal intermediate-flow
master data. Foreground UUIDs are preserved or generated deterministically.
External activity, product, and elementary-flow UUIDs must come from preserved
fields or an exact reference catalog; names are never treated as UUID evidence.

Required time period, macroeconomic scenario, and person fields come from
structured imported metadata, document defaults, or exact dataset overrides.
They are never inferred from comments or descriptions. Missing required fields
and unresolved external UUIDs are hard errors under every conversion policy.

Consequences
------------

The route is reproducible, schema-valid, and compatible with bw2io's
single-output importer without pretending to reconstruct unsupported ecoinvent
semantics. Users converting generic Excel or CSV inventories must provide a
metadata profile and, whenever external exchanges lack preserved UUIDs, a
reference catalog for the intended target database version.
