Background migration
====================

BrightPath packages attributed Premise migration resources and applies them to
canonical dataset, technosphere, substitution, and biosphere identities.
Migration keeps the current software format.

Supported routes
----------------

Packaged technosphere rules connect every ecoinvent cut-off version from 3.5
through 3.12. Routes work forward and backward, including multi-step paths.

.. code-block:: python

   from brightpath.migrations import available_ecoinvent_versions

   print(available_ecoinvent_versions())
   # ('3.5', '3.6', '3.7', '3.8', '3.9', '3.10', '3.11', '3.12')

Numeric patch versions are normalized before route resolution. For example,
``3.10.1`` uses the ``3.10`` rules.

Forward migration
-----------------

.. code-block:: python

   from brightpath import BackgroundProfile

   migrated = inventory.migrate_background(
       BackgroundProfile("ecoinvent", "3.12", "cutoff")
   )

``inventory`` may be either a ``BrightwayInventory`` or a
``SimaProInventory``. The returned object has the same facade type.

Target validation is enabled by default. Its issues are appended to the
migration report rather than raised immediately:

.. code-block:: python

   report = migrated.last_migration_report
   print(report.source_profile.label())
   print(report.target_profile.label())
   print(report.changed, report.has_errors)

   for step in report.steps:
       print(
           step.source_version,
           step.target_version,
           step.direction,
           step.technosphere_replacements,
           step.technosphere_disaggregations,
           step.biosphere_replacements,
           step.biosphere_deletions,
       )

   for issue in report.all_issues:
       print(issue.severity, issue.code, issue.path, issue.message)

Writers validate again by default and raise ``InventoryValidationError`` when
errors remain.

Reverse migration
-----------------

.. code-block:: python

   older = migrated.migrate_background(
       BackgroundProfile("ecoinvent", "3.6", "cutoff")
   )

Reverse routes can be lossy:

* a forward one-to-many disaggregation is reconstructed by aggregation;
* per-exchange metadata cannot always be reconstructed;
* forward biosphere deletions cannot be reversed;
* ambiguous packaged rules are resolved deterministically and reported;
* unit-changing rules do not contain amount-conversion factors.

Treat every warning as an audit item, especially
``migration_reverse_aggregation_lossy``,
``migration_biosphere_deletion_irreversible``, and
``migration_unit_changed_without_amount_conversion``.

Migration without target validation
-----------------------------------

For diagnostics or when a custom target catalog is not installed:

.. code-block:: python

   migrated = inventory.migrate_background(
       BackgroundProfile("ecoinvent", "3.12", "cutoff"),
       validate_target=False,
   )

This only skips catalog validation after the migration. It does not suppress
migration-rule issues and does not establish that the result is ready to use.

Same-profile migration
----------------------

Migrating to the normalized current profile is a supported no-op. It returns a
new facade with an empty, unchanged report, which is useful when target
profiles are selected dynamically.

Unsupported routes
------------------

The following raise ``MigrationUnavailableError``:

* ecoinvent cut-off to consequential or the reverse;
* migration between consequential versions;
* ecoinvent to UVEK or UVEK to ecoinvent;
* migration between UVEK versions;
* any ecoinvent version outside the packaged graph.

The ecoinvent-to-UVEK resource is deliberately an empty placeholder. It cannot
be mistaken for a successful conversion.
