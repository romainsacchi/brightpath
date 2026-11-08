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

Canonical biosphere identity decisions
--------------------------------------

The UVEK bridge to the canonical ecoinvent 3.10 biosphere declares
``canonical_biosphere_identity_version: 1``. Its replacement rules include
``biosphere_identity_review.profiles`` entries with an exact source biosphere
profile, publisher ``source_flow_ids``, status and verified target identity.
Same-UUID decisions require unchanged units and complete receiving contexts.
The two reviewed publisher synonyms additionally require matching chemical
identity evidence. Targets include the publisher ``flow_id``.

The semantic resource validator checks these decisions in addition to the
shared JSON schema. Execution selects the declared source release before
matching any exchanges. A serialized default target cannot authorize another
release's decision. Unsupported entries carry explicit reasons in
``unresolved`` and never carry executable targets; used unsupported physical
flows cause full rollback even under permissive policy. This bridge permits
no quantity rescaling, deletion or inferred inverse. Native UVEK biosphere
correspondences retain their independent resource and approval scope.

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
* ``reference_flow_sign_review``: optional independent reference-coordinate
  review on a technosphere replacement. ``status`` is ``verified`` or
  ``unresolved``; ``reason`` and nonempty ``evidence`` record the basis.
  Verified reviews require integer ``source_sign`` and ``target_sign`` values
  of -1 or +1. The executable ``conversion_factor`` must have their product's
  sign for an unchanged reference role. Its magnitude retains any independently documented unit/quantity
  conversion. Unresolved reviews block the used correspondence under every
  policy; unused catalog rules do not prevent conversion.

  Optional ``profiles`` entries scope an independent decision to an exact
  ``source_profile`` (family, version, system model). Each entry has a ``review``
  with the same status/reason/evidence and reference-sign contract. Verified
  replacement entries require an explicit finite nonzero ``conversion_factor`` consistent
  with those signs; unresolved entries must not supply an executable factor.
  Profiles must be unique and declared by the resource. The containing review
  remains globally unresolved so that consumers without profile-selection support
  fail closed. There is no fallback between versions, patch versions, or models.

  A profile may instead explicitly declare ``operation: delete`` and
  ``approved_omission: true``, with a ``review_ref`` pointing to a shared review
  containing rationale and nonempty evidence. This requires a verified profile
  review and forbids a conversion factor. Execution removes that replacement
  from an operation-local resource and applies an approved warning-only deletion
  for the exact profile. Other profiles retain their independent decisions;
  neither a missing profile nor a reverse route inherits an omission. Cached
  resources and caller-owned inventory data remain unchanged.

  A separately approved waste-to-recovered-product credit may declare
  ``role_conversion: recovery_credit`` inside its verified review. It requires
  source sign -1, target sign +1, an explicit positive quantity factor and a
  ``review_ref`` with shared scientific rationale/evidence. It cannot be combined
  with an omission or recipe target. This role conversion preserves an incoming
  negative product-equivalent credit, so its factor sign is the negative of the
  ordinary reference-sign product. Both profile decisions and selected scalar
  reviews enforce this contract. An unmarked or pending role mismatch stays
  blocked, and a missing exact profile cannot inherit an approval.

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
Deletion rules contain ``source``. Ordinary deletion execution remains
policy-controlled; merely loading a deletion rule does not delete an exchange.

An explicitly reviewed technosphere deletion can declare
``approved_omission: true``. It requires an identifier and a shared review with
rationale and evidence, and cannot also replace/disaggregate the same source.
False markers, markers on other operations/axes and unaudited omissions are
rejected. A matched approved omission removes only the external exchange and
always reports a warning and a detailed loss, independently of generic deletion
and information-loss policies. The loss preserves the original exchange and
its path; the step counts ``technosphere_deletions``. Production, foreground
links and dataset identities are preserved. Unrelated execution errors still
roll back the operation. Omission does not create a reverse reconstruction rule.

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

An empty ``inputs`` list is accepted only with ``zero_burden: true`` and a
nonempty ``zero_burden_reason``, rationale and evidence. Every use requires an
exact verified ecoinvent cut-off source-profile decision with positive source
and target references and factor +1, plus shared scientific review evidence.
An unreferenced zero-burden recipe is rejected. A dependency on an approved
zero-burden stock requires ``zero_burden_supplier`` with its direct ``rule_id``,
exact ``source_profile`` and SHA-256 ``profile_review_sha256``. The digest covers
the sorted JSON profile entry. The direct approval must target that exact recipe.
Every containing helper, including indirect parents, requires an unresolved
default and verified entries matching the bound stock's exact source profile.
Unannotated, stale, cross-model and cross-version dependencies are rejected.
The annotation is removed when rendering exchanges; allocation warnings and
production-only helper metadata remain in the migration output.
All other recipes still require at least one input. These production-only
suppliers preserve observed allocation boundaries and material flows; unresolved
background links cannot become empty suppliers. Execution records a dedicated
``migration.zero_burden_supplier`` warning and retains the original exchange.

The nine reverse recipes are unchanged, preserving generated helper names,
codes, quantities, consumer uncertainty and provenance. The complete returned
foreground inventory, including generated helpers, must still be exported.
The registered reviewed UVEK exporter executes replacement/helper targets and
explicitly approved omissions; ordinary deletions and disaggregation remain
unsupported there. The heuristic forward route also executes individually reviewed
foreground recipes with verified exact source profiles. A verified profile entry
may contain ``target: {recipe_ref: ...}`` to select a unit-compatible recipe
variant. Pending decisions, deletions and recovery credits cannot override recipe
targets. Forward refrigerant helpers use the ecoinvent 3.10 biosphere independently
of the input inventory's biosphere version.

Forward recipe proxies declare ``technosphere_information_loss`` and shared
review evidence. Strict information-loss policy blocks their use. An accepted
proxy records a warning and a loss containing the full original consumer exchange,
including when the caller otherwise allows information loss. Target-native
technosphere inputs in these helpers are included in subsequent release migration
and final target-link validation.

Recipe biosphere inputs may declare ``biosphere_targets`` with unique exact
biosphere profiles, explicit name/category/unit targets and a reason. Quantities
and units are preserved, declared target flows must exist, and an authored
``information_loss`` warning is retained in loss reports. This is an explicit
recipe-local approval rather than runtime fuzzy matching. Only biosphere
inventory indicators may carry ``approved_omission_if_unavailable: true`` with
an ``omission_rationale``; unsupported indicators then warn and remain audited.
An unavailable catalog must not trigger optional omission.

Both directions share external-exchange protection,
warning and loss-audit behavior for approved omissions. Route bindings reject
unsupported recipe targets or operation types before transformation.

Coverage and validation
-----------------------

For a nonempty ``coverage`` object, the common fields are:

* ``source_identities``: distinct source identities in the declared coverage set;
* ``mapped_identities``: distinct source identities handled by the rules,
  including explicit deletion dispositions;
* ``unresolved_identities``: the remainder, whose absence from the rule lists
  never authorizes conversion.

The counts must agree. Optional ``target_identities`` describes the available
target catalog, ``mapping_methods`` counts selection procedures, and
``dispositions`` distinguishes correspondences, proxies, recipe mappings and
unresolved entries. ``mean_confidence`` averages only provided scores and is
absent when no scores were supplied.
Optional ``omitted_identities`` counts approved technosphere omission rules and
is validated against ``delete``. Mapping methods and confidence summaries concern
replacement rules; an omission does not advertise a target supplier.
Profile-specific omissions remain decisions inside their replacement rule and
do not increment ``omitted_identities`` or add a second source disposition.

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

A verified exact-profile decision may override a literal default supplier with
a service recipe whose unit matches the source unit. The profile must declare
its own conversion factor and verified signs. Other inspected profiles may
continue to inherit the literal supplier and its independently reviewed factor.
Selection happens before recipe materialization; an uninspected profile remains
unresolved and receives no default approval. This supports retaining an existing
native supplier for some versions while reconstructing a different source
operating boundary for others, without adding unused helpers.


Recipe-local parameters
~~~~~~~~~~~~~~~~~~~~~~~

An authored recipe may include a ``parameters`` list. Each entry requires a
unique nonempty ``name`` and finite numeric ``amount``; formulas, supported
uncertainty fields and additional parameter metadata are preserved. Invalid
parameter types, duplicate local names and reversed uncertainty bounds fail
resource validation. Negative parameter values remain valid.

Both directional helper materializers copy the complete parameter list into
the generated dataset. The helper's exchanges retain their formulas. Each
helper keeps its own parameter scope, including dependencies that use the same
parameter names. A parameter change changes the helper's deterministic identity.
Caller parameters remain caller-owned. Formula evaluation and translation into
a particular output syntax remain the responsibility of the registered format
adapters; helper materialization does not recompute inventory quantities.
