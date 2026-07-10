ADR 0002: Strict loss and migration policy
==========================================

Status
------

Accepted for the BrightPath 1.0 pre-release series.

Decision
--------

Conversion and migration preflight in strict mode before creating an output.
The following conditions are errors unless a caller explicitly permits the
corresponding stable loss or migration code:

* ambiguous format detection or mapping rules;
* unsupported or omitted fields and exchanges;
* unresolved source or target background links;
* unit changes without finite amount-conversion factors;
* partial reverse aggregation;
* irreversible deletion;
* missing catalog or migration resources.

Conversion applies ambiguous-mapping policy during adapter-owned preflight.
When target validation is enabled, it runs the target adapter's independent
format hook after the context change and applies invalid-target policy to that
stage.

Every operation returns an immutable, deterministic, JSON-serializable report.
Reports contain stage, code, canonical path, source and target contexts,
changes, losses, metrics, policy snapshot, and resource identifiers. Public
exceptions expose the same report through ``.report``.

Target validation coverage is based on background-link occurrences for each
axis:

.. code-block:: text

   resolved target links / all target links

Foreground technosphere links are counted as resolved before catalog lookup.
Catalog validation coverage is reported separately from transformation counts
because stable links may need no rule. Capability discovery advertises
non-placeholder resource edges; a full route is executable only when every
changed axis has a route and the selected source/target catalogs and policy
checks succeed.
