Background migration
====================

Background migration is independent of file format. It has two explicit
services:

* ``plan_background_migration()`` resolves technosphere and biosphere resource
  routes without reading catalogs or changing inventory data;
* ``execute_background_migration()`` validates the source, applies a plan to a
  copy, validates the target, and commits or rolls back transactionally.

``InventoryPipeline.migrate()`` is the usual application entry point and
delegates to the executor with its injected catalog provider.

Plan without changing data
--------------------------

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       TechnosphereProfile,
   )
   from brightpath.background import plan_background_migration
   from brightpath.core import MigrationPolicy

   source = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.10"),
   )
   target = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11"),
   )

   plan = plan_background_migration(
       source,
       target,
       MigrationPolicy.strict(),
   )
   print(plan.executable)
   for step in plan.technosphere_steps:
       print(step.axis.value, step.source_version, step.target_version)
   for step in plan.biosphere_steps:
       print(step.axis.value, step.source_version, step.target_version)

Planning records exact source/target profiles, resource-series resolutions,
resource names, rule counts, direction, policy findings, and potential losses.
Unsupported routes are inspectable error reports; invalid Python argument types
still raise immediately.

Execute through the pipeline
----------------------------

.. code-block:: python

   migration = pipeline.migrate(
       document,
       target,
       policy=MigrationPolicy.strict(),
   )

   for stage in migration.report.stages:
       print(stage.stage.value, stage.has_errors, dict(stage.metrics))
   for issue in migration.report.issues:
       print(issue.severity.value, issue.code, issue.path, issue.message)
   for loss in migration.report.losses:
       print(loss.code, loss.path, loss.message)

   if not migration.succeeded:
       raise RuntimeError(migration.report.to_json(indent=2))

   assert migration.value.context.format == document.context.format
   pipeline.write(migration.value, "foreground-ei311.xlsx", sidecar=True)

Strict policy
-------------

``MigrationPolicy.strict()`` requires:

* exact source validation before rule application;
* a resource-backed route for every changed background component;
* no inferred reverse route, ambiguous rule, deletion, information loss, or
  unit change without a numeric factor;
* exact target validation after rule application;
* 100% resolved target-link coverage for technosphere and biosphere.

If any error-policy condition occurs, execution returns the original document.
The migration stage is marked as rolled back and its non-committed changes are
removed from the report.

Permissive review
-----------------

``MigrationPolicy.permissive()`` changes policy-controlled failures to
warnings and sets minimum target coverage to zero:

.. code-block:: python

   review = pipeline.migrate(
       document,
       older_target,
       policy=MigrationPolicy.permissive(),
   )
   for issue in review.report.issues:
       print(issue.severity.value, issue.code, issue.message)
   for loss in review.report.losses:
       print("loss", loss.code, loss.message)

This mode is intended for diagnosis and informed manual review. An operation
that continues under permissive policy is not automatically scientifically
valid.

Reverse routes
--------------

Only forward migration resources are packaged. A reverse route is inferred by
reversing those edges and is policy-controlled:

* forward one-to-many disaggregation may require lossy reverse aggregation;
* individual exchange metadata cannot always be reconstructed;
* deleted biosphere flows cannot be recovered;
* multiple source rules can map to the same target identity;
* a change of unit is unsafe unless the resource supplies a numeric factor.

Strict policy rejects inferred reverse steps. Permissive policy records both
the warning and explicit ``Loss`` objects. Unsafe unit-changing rules are
skipped rather than applying a new unit to an unchanged amount.

Exact patch versions
--------------------

Exact profile versions are preserved while planning uses a separately recorded
migration series:

.. code-block:: python

   patch_source = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.10.1"),
   )
   patch_target = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.11.2", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11.2"),
   )
   patch_plan = plan_background_migration(patch_source, patch_target)

   assert patch_plan.source_technosphere_resolution.exact_version == "3.10.1"
   assert patch_plan.source_technosphere_resolution.migration_series == "3.10"
   assert patch_plan.target_technosphere_resolution.migration_series == "3.11"

Execution also requires exact 3.10.1 and 3.11.2 catalogs from the injected
provider; packaged 3.10 and 3.11 catalogs are not treated as equivalents. Two
different exact versions in the same series, such as 3.10.1 and 3.10.2, are not
migrated because no resource establishes their equivalence.

Available resource edges
------------------------

Packaged ecoinvent cut-off technosphere edges connect 3.5→3.6 through
3.11→3.12. Packaged biosphere edges connect 3.5→3.6 through 3.10→3.11.
Capability discovery reports edges, not a promise that every multi-axis route
is executable under every policy.

In particular, no ecoinvent 3.11→3.12 biosphere migration resource is
packaged. A complete target that changes both technosphere and biosphere to
3.12 is therefore unavailable:

.. code-block:: python

   target_312 = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.12"),
   )
   unavailable = plan_background_migration(target, target_312)
   assert not unavailable.executable

The planner reports
``migration.biosphere_resource_missing_unavailable``. It does not fabricate an
identity mapping or silently leave the biosphere relabeled and unchanged.

Independent background components
---------------------------------

A target may intentionally preserve one component while changing the other:

.. code-block:: python

   technosphere_only_target = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.12", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11"),
   )

That plan uses the technosphere edge and no biosphere edge. The caller is
responsible for choosing a scientifically valid combination, and exact target
catalog validation still applies.

Unavailable families and models
-------------------------------

No resources are advertised for:

* consequential version migration;
* cross-system-model migration;
* ecoinvent↔UVEK migration;
* migration between UVEK versions;
* unsupported background families.

The ecoinvent-to-UVEK file is an explicit placeholder and is excluded from
capability discovery. UVEK can still be read, validated, and converted between
software formats without changing its background.
