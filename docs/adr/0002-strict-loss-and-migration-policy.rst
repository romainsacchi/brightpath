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

Every operation returns an immutable, deterministic, JSON-serializable report.
Reports contain stage, code, canonical path, source and target contexts,
changes, losses, metrics, policy snapshot, and resource identifiers. Public
exceptions expose the same report through ``.report``.

Migration coverage is based on background-link occurrences:

.. code-block:: text

   (changed + valid unchanged in target) / source-valid background occurrences

Rule-match percentage is reported separately because stable links may need no
rule. A route is advertised only when all required resources are released and
strict source/target validation can succeed.
