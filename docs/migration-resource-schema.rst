Shared migration resource schema
================================

Both UVEK technosphere directions use ``schema: brightpath.migration`` and
``schema_version: 3``. The packaged definition is
``brightpath/data/schemas/migration-v3.schema.json``. Validation is offline and
uses only local schema references.

The structure follows the attributed ecoinvent migration resources:
``replace``, ``disaggregate`` and ``delete`` contain operation-specific rules.
BrightPath adds profile scopes, stable rule identifiers, quantity factors,
optional mapping assessments, shared evidence and foreground recipes. The
resource's ``schema_version`` describes this structure; an upstream ``version``
field continues to identify the upstream resource revision.

Resource contract
-----------------

The common required fields are:

* ``schema``, ``schema_version``, ``name``, ``status`` and ``axis``;
* ``source_profiles`` (a nonempty list) and ``target_profile`` (one destination);
* ``reverse_policy``;
* ``replace``, ``disaggregate`` and ``delete`` lists, including when empty;
* ``reviews`` and ``recipes`` objects, including when empty;
* ``coverage``, which can be empty for incremental upstream rules.

Profiles declare ``family`` and ``version``. Technosphere profiles additionally
require ``system_model``; biosphere profiles are independent of system models.
The source declarations identify supported routing scopes. Inventory contexts
retain exact versions, and patch-to-series resolution is separately reported by
the planner. Resource parsing never rewrites an exact inventory version.

Optional metadata includes ``description``, ``quality``, ``source_id``,
``target_id``, contributors, licenses and creation time. Profile-shaped source/target identifiers
must agree with the corresponding declarations. Unknown metadata is preserved.

``reverse_policy: explicit_only`` requires an independently authored reverse
resource. Both UVEK directions use it. Standard ecoinvent resources normalize
with ``infer_with_policy``: existing ambiguity, deletion, aggregation and loss
policies still govern any inferred reverse execution. A shared schema never
turns directional proxy decisions into reversible equivalences.

Rules and assessments
---------------------

A replacement contains ``source`` and ``target`` objects. A technosphere
identity contains ``name``, ``reference product``, ``location`` and ``unit``.
Biosphere sources contain ``name``, ``categories`` and ``unit``; upstream UUIDs
and abbreviated target identities are preserved for existing ecoinvent rules.

A rule can additionally contain:

* ``id``: a stable identifier. Existing reverse-direction identifiers are
  retained; forward identifiers are derived deterministically from the resource
  name and source identity.
* ``conversion_factor``: a finite, nonzero amount multiplier; absence means 1.
* ``mapping_method``: the procedure responsible for the selection.
* ``confidence``: an optional finite number between 0 and 1, inclusive.
* ``classification``: optional ``correspondence`` or ``proxy``.
* ``review_ref``: an identifier referencing an entry in ``reviews``.

Missing confidence means no score was provided. It stays absent through
normalization and serialization. An explicit zero stays zero. Existing scores
are compatibility indicators, not probabilities or proof of scientific
equivalence. Classification and confidence are independent: reviewed reverse
rules retain their classifications without receiving invented scores.

Packaged runtime reviews contain ``rationale`` and optional ``evidence`` and
``limitations``. Detailed review history, comparison-branch methods, revision
identifiers, activity codes and evidence hashes stay in the authoring ledgers
and review documents. Catalog filenames are already represented by profile
declarations; generation methodology and source attribution are documented in
``data/migrations/ATTRIBUTION.md``. Documented proxies
require a rationale and evidence. Resources marked ``quality: reviewed`` require
an identifier, classification and supported rationale for every replacement.

Disaggregation rules contain ``source`` and ``targets``. Targets retain the
upstream ``allocation`` convention, including optional explicit conversion
factors. Allocations must be finite and nonnegative with a positive total.
Deletion rules contain ``source``. Their execution remains policy-controlled;
merely loading a deletion rule does not delete an exchange.

Foreground recipes
------------------

A replacement can explicitly target ``{"recipe_ref": "helper-id"}`` instead of
a background supplier. These two target forms cannot be mixed. The reference
must resolve to ``recipes["helper-id"]``.

Recipes retain their output identity, ``inputs``, ``rationale`` and ``evidence``.
Input quantities are per unit of helper output and preserve exchange uncertainty
and unknown metadata. Supporting recipe dependencies use the existing
``recipe`` reference inside ``inputs``. Dependencies must exist and be acyclic.
Recipe quantities are inventory coefficients and need not sum to one; recipes
remain distinct from disaggregation shares.

The seven current recipes are unchanged, preserving generated helper names,
codes, quantities, consumer uncertainty and provenance. The complete returned
foreground inventory, including generated helpers, must still be exported.
The registered reviewed UVEK exporter executes replacement and helper targets;
the heuristic forward route executes background replacements. Route bindings
reject unsupported recipe targets or operation types before transformation.

Coverage and validation
-----------------------

For a nonempty ``coverage`` object, the common fields are:

* ``source_identities``: distinct source identities in the declared coverage set;
* ``mapped_identities``: distinct source identities handled by the rules;
* ``unresolved_identities``: the remainder, whose absence from the rule lists
  never authorizes conversion.

The counts must agree. Optional ``target_identities`` describes the available
target catalog, ``mapping_methods`` counts selection procedures, and
``dispositions`` distinguishes correspondences, proxies, recipe mappings and
unresolved entries. ``mean_confidence`` averages only provided scores and is
absent when no scores were supplied.

JSON Schema validates field types and target alternatives. Semantic validation
additionally checks finite values, duplicate rule identifiers and sources,
profile consistency, shared references, recipe units and cycles, evidence
requirements and coverage summaries. Parsing and validation do not mutate caller
input. Source/target catalog validation, migration policies and transactional
rollback remain separate execution stages.

Loading, writing and upgrading
------------------------------

``load_migration_resource()`` verifies the manifest, normalizes supported legacy
resources and returns a validated v3 representation. The existing UVEK loading
functions delegate to this common loader. Upstream ecoinvent contributors,
licenses, XPath mappings, graph metadata and rule bodies survive normalization.
The BrightPath tissue-paper supplement retains its rationale and evidence link;
its detailed review identifiers stay in :doc:`recycled-paper-migration`.

``normalize_migration_resource()`` returns an independent validated copy.
``rule_review()`` returns an independently owned review record.
``dumps_migration_resource()`` writes deterministic JSON with one rule per line,
inline identity objects and shared review records. It excludes only the internal
loader-added ``_path`` field. Both resource generators use the common writer.

The two incompatible UVEK version-2 layouts are upgraded explicitly, rather than
being interpreted as interchangeable runtime schemas:

.. code-block:: console

   python -m scripts.upgrade_uvek_migration_schema old-resource.json new-resource.json
   python -m scripts.generate_migration_manifest

The upgrade translates:

.. list-table::
   :header-rows: 1

   * - Earlier field or representation
     - Common v3 representation
   * - Reverse ``rules``
     - ``replace``
   * - Reverse ``factor``
     - ``conversion_factor``
   * - Reverse rule ``recipe``
     - ``target.recipe_ref``
   * - ``source_profile`` with scalar or plural selectors
     - Explicit ``source_profiles`` list
   * - Reverse ``coverage.approved_identities``
     - ``coverage.mapped_identities``
   * - Forward shared ``targets`` references
     - Inline target identity objects
   * - ``review_provenance`` and inline rule evidence
     - ``review_ref`` with shared ``reviews`` records
   * - Legacy review ``reason``
     - ``rationale``, retaining the original text

The upgrade preserves mapping choices, factors, scores, classifications,
evidence and recipe contents. It never derives the reverse direction by
inverting another mapping. See :doc:`workflows/ecoinvent-to-uvek` and
:doc:`workflows/uvek-export` for directional scope and limitations.
