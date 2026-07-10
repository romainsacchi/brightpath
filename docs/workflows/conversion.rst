Format conversion
=================

Format conversion changes only ``InventoryContext.format``. The complete
technosphere and biosphere context is preserved.

Build a pipeline and read
-------------------------

.. code-block:: python

   from brightpath import (
       BackgroundContext,
       BiosphereProfile,
       FormatProfile,
       InventoryContext,
       InventoryPipeline,
       TechnosphereProfile,
   )
   from brightpath.adapters import default_adapter_registry
   from brightpath.background import catalog_provider_from_environment

   pipeline = InventoryPipeline(
       default_adapter_registry(),
       catalog_provider_from_environment(),
   )
   source_context = InventoryContext(
       format=FormatProfile("brightway_excel"),
       background=BackgroundContext(
           technosphere=TechnosphereProfile("ecoinvent", "3.10", "cutoff"),
           biosphere=BiosphereProfile("ecoinvent", "3.10"),
       ),
   )
   read = pipeline.read("foreground.xlsx", hint=source_context.as_hint())
   if read.value is None or not read.succeeded:
       raise RuntimeError(read.report.to_json(indent=2))

Brightway to SimaPro
--------------------

.. code-block:: python

   from brightpath.core import ConversionPolicy

   policy = ConversionPolicy.strict()
   conversion = pipeline.convert(
       read.value,
       "simapro_csv",
       policy=policy,
   )
   if conversion.value is None or not conversion.succeeded:
       raise RuntimeError(conversion.report.to_json(indent=2))

   assert conversion.value.context.background == read.value.context.background

   written = pipeline.write(
       conversion.value,
       "foreground.csv",
       target_format="simapro_csv",
       policy=policy,
       sidecar=True,
   )
   if not written.succeeded:
       raise RuntimeError(written.report.to_json(indent=2))

SimaPro preflight reports representability issues before changing the format
context. The selected adapter owns this ``preflight_conversion`` hook; the
pipeline treats missing, failing, or malformed contracts as errors. Known
unused exchanges are explicit ``Loss`` values. Unsupported features,
information loss, ambiguous mappings, and invalid targets are errors under
strict policy.

Live conversion policy controls
-------------------------------

``ConversionPolicy`` applies independent decisions to unsupported features,
ordinary information loss, ambiguous mappings, and invalid target format.
SimaPro, for example, reports conflicting ``product`` and ``reference product``
aliases through ``on_ambiguous_mapping`` rather than folding them into generic
information loss.

With ``validate_target=True`` (the default), the pipeline changes only the
format context and then calls the target adapter's ``validate_format`` hook.
``on_invalid_target`` controls errors from this post-conversion stage. Set
``validate_target=False`` only when the caller will validate the target format
separately:

.. code-block:: python

   review_policy = ConversionPolicy(
       on_ambiguous_mapping="warn",
       on_information_loss="warn",
       validate_target=True,
   )
   review = pipeline.convert(
       read.value,
       "simapro_csv",
       policy=review_policy,
   )

Preflight always runs, regardless of ``validate_target``.

SimaPro to Brightway
--------------------

.. code-block:: python

   simapro_context = InventoryContext(
       format=FormatProfile("simapro_csv", encoding="latin-1"),
       background=source_context.background,
   )
   source = pipeline.read(
       "foreground.csv",
       hint=simapro_context.as_hint(),
   )
   converted = pipeline.convert(
       source.value,
       "brightway_excel",
       policy=ConversionPolicy.strict(),
   )
   pipeline.write(converted.value, "foreground.xlsx")

The SimaPro adapter declares ``requires_catalog_provider``. The pipeline
therefore injects its exact provider into the reader, which loads the declared
biosphere catalog before normalizing flow names.

Brightway delimited conversion
------------------------------

Brightway block CSV and TSV are registered read/write formats:

.. code-block:: python

   as_csv = pipeline.convert(read.value, "brightway_csv")
   pipeline.write(as_csv.value, "foreground.csv")

   as_tsv = pipeline.convert(as_csv.value, "brightway_tsv")
   pipeline.write(as_tsv.value, "foreground.tsv")

These operations preserve background context and use the same canonical
inventory. Content detection distinguishes Brightway CSV from SimaPro CSV; it
does not default by suffix.

Permissive review
-----------------

.. code-block:: python

   review = pipeline.convert(
       read.value,
       "simapro_csv",
       policy=ConversionPolicy.permissive(),
   )

   for issue in review.report.issues:
       print(issue.severity.value, issue.code, issue.message)
   for loss in review.report.losses:
       print("loss", loss.code, loss.path, loss.message)

Permissive policy changes unsafe conversion errors to warnings where possible.
It does not suppress findings or prove that a target file is semantically
equivalent.

Migrate, then convert
---------------------

When both background and software change, compose the independent operations:

.. code-block:: python

   from brightpath.core import MigrationPolicy

   target_background = BackgroundContext(
       technosphere=TechnosphereProfile("ecoinvent", "3.11", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.11"),
   )
   migration = pipeline.migrate(
       read.value,
       target_background,
       policy=MigrationPolicy.strict(),
   )
   if not migration.succeeded:
       raise RuntimeError(migration.report.to_json(indent=2))

   conversion = pipeline.convert(
       migration.value,
       "simapro_csv",
       policy=ConversionPolicy.strict(),
   )
   if conversion.value is None or not conversion.succeeded:
       raise RuntimeError(conversion.report.to_json(indent=2))

   pipeline.write(
       conversion.value,
       "foreground-ei311.csv",
       sidecar="foreground-ei311.audit.json",
   )

No intermediate artifact is needed. Migration does not choose SimaPro, and
conversion does not rename links for ecoinvent 3.10.

UVEK format conversion
----------------------

The same format-only operation works for UVEK:

.. code-block:: python

   uvek_background = BackgroundContext(
       technosphere=TechnosphereProfile("uvek", "2025", "cutoff"),
       biosphere=BiosphereProfile("ecoinvent", "3.10"),
   )
   uvek_source = InventoryContext(
       format=FormatProfile("brightway_excel"),
       background=uvek_background,
   )
   uvek = pipeline.read("foreground-uvek.xlsx", hint=uvek_source.as_hint())
   uvek_csv = pipeline.convert(uvek.value, "simapro_csv")

   assert uvek_csv.value.context.background == uvek_background

This is not an ecoinvent-to-UVEK mapping. Cross-family background migration
remains unavailable.

File round-trip limits
----------------------

An in-memory format-context change preserves canonical data. A written file is
limited by its target grammar. Brightway formats can retain namespaced unknown
fields through their canonical bridge. SimaPro has stricter sections, units,
categories, field sets, and Latin-1 encoding. Always inspect the conversion and
write reports; successful parsing alone is not proof of lossless file
round-trip behavior.

OpenLCA Excel and ecoSpold2 have no registered adapters. Conversion to those
reserved identifiers fails capability preflight rather than writing an
approximate file.

Qualified target descriptors
----------------------------

Pass a ``FormatDescriptor`` or qualified ``FormatProfile`` when a registry has
versioned or dialect-specific adapters. Registry lookup selects an exact
qualified adapter first, then a registered unqualified adapter for the same
format family as a conservative fallback. An unqualified request is rejected
as ambiguous when only multiple qualified adapters are registered.
