ILCD process packages
=====================

Scope
-----

``ilcd`` reads and writes ILCD 1.1 process datasets. ZIPs can contain folders
directly at their root or under ``ILCD/``. Python also accepts directories and
standalone process XML when a local reference directory supplies dependencies.
ZIP is the intended CLIC upload and download artifact.

Each process must have exactly one nonzero canonical production exchange.
Positive production is a product reference output. Negative production denotes
a waste-treatment reference input: the XML uses a waste flow with positive
input magnitude, and reimport restores the negative canonical coefficient.
Linked waste-treatment demands use waste outputs with the opposite signed
magnitude. This coordinate change also transforms formulas, uncertainty centres
and bounds; it never changes the caller's inventory or fabricates an output.
Unit process and aggregated-inventory types are preserved without aggregation.
Additional product outputs, non-reference waste inputs, allocations,
reminder flows, metadata-only records, EPDs and eILCD lifecycle models are
outside this adapter's conversion scope.

The reader resolves flows, flow properties and reference units locally by
type, UUID and dataset version. Relative URIs must agree with those identifiers.
Remote URLs are never fetched. ZIP traversal, DTDs, symlinks, duplicate members,
conflicting entities and excessive archive expansion are rejected.

Reading and inspecting
----------------------

.. code-block:: python

   from brightpath import (
       BackgroundContext, BiosphereProfile, FormatProfile,
       InventoryContext, TechnosphereProfile,
   )
   from brightpath.formats.ilcd import load_ilcd, load_ilcd_package

   context = InventoryContext(
       FormatProfile("ilcd", format_version="1.1"),
       BackgroundContext(
           TechnosphereProfile("ecoinvent", "3.10.1", "cutoff"),
           BiosphereProfile("ecoinvent", "3.10.1"),
       ),
   )
   document = load_ilcd("inventory.zip", context=context)

   # Syntax and local reference inspection without selecting a background:
   package = load_ilcd_package("inventory.zip")

The ``ilcd`` extension on each dataset retains metadata, original XML and its
reference closure. It is JSON serializable and survives snapshot storage.
Imported external product flows without verified canonical supplier identities
remain inspectable but carry ``ilcd.supplier_unresolved`` findings. Analysis
and format validation expose these findings; successful XML parsing alone
does not establish publication readiness.

Export metadata
---------------

Supply explicit temporal and process metadata if it was not imported:

.. code-block:: yaml

   schema_version: 1
   defaults:
     dataset_version: '01.00.000'
     process_type: 'Unit process, single operation'
     reference_year: 2020
     valid_until: 2025
   datasets:
     foreground-process-code:
       dataset_version: '01.01.000'

Overrides use exact canonical dataset codes. Explicit overrides take precedence
over preserved imported metadata, followed by supplied defaults. Validity years
are scientific metadata; publication dates and narrative comments do not supply
them implicitly. Authors, review claims and licenses are not invented.
Optional profile fields are ``license_type``, ``access_restrictions``,
``copyright`` (a boolean), and ``permanent_uri``. Unknown fields are rejected.
Imported contacts, sources and other administrative metadata remain in the
preserved XML templates. CLIC supplies its publication license explicitly.

.. code-block:: python

   from brightpath.formats.ilcd import write_ilcd
   from brightpath.formats.ilcd_metadata import ILCDExportProfile

   profile = ILCDExportProfile.from_file("ilcd-metadata.yaml")
   write_ilcd(
       document, "inventory-ilcd.zip",
       metadata_profile=profile, dialect="openlca",
   )

New foreground IDs are deterministic for persistent dataset code and exact
background context. Imported identifiers are preserved where applicable.
Reference production quantities are retained, including quantities other than
one. Directory exports require a new destination; ZIP publication is atomic.

Exchange comments longer than ILCD's 500-character limit are retained in full
in a labeled JSON block in the process general comment, whose free-text type
has no length limit. The exchange contains a short pointer with the complete
text's SHA-256, rather than a truncated qualification. Native openLCA preserves
the complete block in its process description. BrightPath restores the exact
exchange comment on reimport, including whitespace, and authenticates its
pointer even if the native exporter renumbers exchange IDs. The original
process comment is restored separately. No source comment is edited or lost.
Non-string exchange comments use the XML writer's existing text conversion
before length checking. This affects serialized metadata only: the source value,
amounts, uncertainty, formulas and inventory identities remain unchanged.

Supplier links and background mappings
--------------------------------------

Core ILCD flow references do not uniquely select suppliers. ``dialect="core"``
therefore rejects an export that would lose a selected supplier. Choose
``dialect="openlca"`` to preserve suppliers using the documented openLCA exchange
extensions. Other consuming tools may not interpret those extensions.

For the ``openlca`` dialect, BrightPath automatically resolves used external
exchanges from the exact background catalogs. Ecoinvent catalogs supply activity,
product-flow and elementary-flow UUIDs. Provider UUIDs follow openLCA 2.6.2's
activity/product ``KeyGen`` convention; product flows retain their ecoSpold UUIDs.
UVEK 2025 uses the separately verified packaged openLCA reference build. Missing
entries in its partial biosphere coverage remain errors.

Only referenced flows, flow properties and unit groups are generated. Background
process inventories are never embedded or synthesized. A recipient needs the
matching background database to resolve external providers. These conventions
target openLCA 2.6.2; arbitrary ILCD database builds may use different identifiers.
They do not establish EF reference-system equivalence.

Reference quantity definitions are pinned to the openLCA 2.6.2 units template.
Foreground mass-transport reference flows also use its ``Mass transport``
property and ``t*km`` reference unit. Canonical ``ton kilometer`` (including
``tkm`` and ``t*km`` aliases) retains its amount, formula and uncertainty
without rescaling. The template's ``kg*km`` unit has factor 0.001 relative
to ``t*km``; neither unit is ``kg*m``.
Foreground person-transport reference flows use the same pinned template's
``Person transport`` property, unit group and ``p*km`` reference unit at
factor 1. The ILCD reader returns canonical ``person kilometer`` for native
``p*km``; amounts, formulas, production identities and uncertainty retain
their values. This alias is local to ILCD and does not change global unit
normalization or create background provider identities.
The public unit template has no distinct standard-cubic-metre quantity. A
separate, integrity-checked resource now reproduces openLCA 2.6.2's native
ecoSpold unit fallback for ``Sm3``: source unit UUID
``225da0bd-ba54-48bc-9a0e-5fa27d722185``, native group
``c2dba802-b9a4-345b-b4b3-847754301054`` and native property
``c57e6ed2-5c28-3348-aaee-75d637b80c77``. Its reference factor is 1;
canonical ``standard cubic meter`` coefficients, formulas and uncertainty are
not rescaled. Source metadata defines gas volume at 15 degrees C and
101.325 kPa (ISO 13443); exported quantity comments retain these conditions.

Automatic Sm3 mapping is limited to metadata-confirmed ecoinvent profiles:
3.9.1 cutoff, 3.10.1 cutoff and consequential, and 3.12 cutoff. This targets
the openLCA 2.6.2 ecoSpold import using the public unit template; it does not
establish compatibility with arbitrary custom background builds. The installed
native importer was checked to reuse the same Sm3 unit/group/property and an
existing provider, with ordinary volume already present. Background process
and flow UUIDs still come from the exact catalog provider; no process inventory
is created. An explicit ordinary-volume quantity cannot redefine Sm3.
Normal cubic metres (Nm3), unsupported profiles and unrelated unknown units
remain guarded. Neither Sm3 nor Nm3 is relabeled as ordinary cubic metres.
Amounts, formulas and representable uncertainty are converted together to the
flow's reference unit (for example, kWh to MJ). BrightPath link extensions retain
the original canonical identity and explicit conversion for reimport. An unknown
unit requires an explicit mapping; units are never relabeled without conversion.

Explicit ``ILCDReferenceCatalog`` entries and same-context preserved ILCD evidence
take precedence over automatic lookup. They also cover alternative target builds
or references outside packaged coverage. ``core`` exports require explicit or
preserved mappings. The catalog includes supporting flow, property and unit XML,
not complete background process inventories. Its shape is:

.. code-block:: json

   {
     "schema_version": 1,
     "context": {
       "technosphere": {
         "family": "ecoinvent", "version": "3.10.1", "system_model": "cutoff"
       },
       "biosphere": {"family": "ecoinvent", "version": "3.10.1"}
     },
     "technosphere": [{
       "identity": {
         "name": "Verified supplier", "reference product": "Verified product",
         "location": "CH", "unit": "kilogram"
       },
       "process_id": "11111111-1111-4111-8111-111111111111",
       "flow_id": "22222222-2222-4222-8222-222222222222",
       "flow_version": "01.00.000"
     }],
     "biosphere": [],
     "resources": {}
   }

The identifiers above are illustrative. Populate ``resources`` with complete
referenced XML strings keyed by their relative package paths. Biosphere rows
use ``name``, ``categories`` and ``unit`` identities and need no process ID.
Load with ``ILCDReferenceCatalog.from_file`` and pass ``reference_catalog`` to
the reader or writer. The catalog's complete exact context must match.
An optional mapping row can specify ``amount_factor`` and ``reference_unit``
for an explicit conversion from its canonical identity unit to the XML flow's
reference unit. Factors must be finite and positive. Unsupported unit changes
remain errors. The writer's optional ``catalog_provider`` argument injects exact
background catalogs; the operation pipeline passes its provider automatically.

BrightPath preserves canonical link evidence in a namespaced extension for
CLIC reimports. Treat this evidence as submitted data: validate it against
the selected exact background catalog before publication or calculation.
No EF/ILCD-to-ecoinvent mappings are bundled in this release.

Parameters and uncertainty
--------------------------

Process parameters support bounded scalar arithmetic and acyclic dependencies.
The interpreter accepts names, numeric constants and arithmetic operators;
Python calls, attributes and indexing are rejected. Cached amounts must agree
with formulas. Shared database/project parameters must first be copied into
unambiguous process scope; CLIC performs that step for ILCD exports.

Deterministic, normal, uniform and positive log-normal exchanges are supported
when their parameters are representable. Normal dispersion uses twice the
absolute standard deviation; log-normal dispersion uses the squared geometric
standard deviation in the ``core`` dialect. openLCA 2.6 uses the same field
for a single standard deviation or geometric standard deviation instead;
the explicit ``openlca`` dialect follows that convention. Exported process
XML declares its dialect in a namespaced attribute. Unmarked native openLCA
exchange attributes provide inference evidence reported as
``ilcd.dialect_inferred``; use ``dialect=`` or ``--source-ilcd-dialect`` to
override interpretation of an unmarked input. Conflicting explicit dialects
and package declarations are errors.

Independent centres in the openLCA dialect
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Native openLCA 2.6.2 accepts a partial exchange extension containing only
``olca:amount`` alongside the standard ILCD uncertainty fields. Its importer
keeps the nominal exchange amount and expression from the core elements while
reading the uncertainty parameter from this attribute. BrightPath uses this
encoding for a normal distribution whose ``loc`` differs from its nominal amount.
The attribute is expressed in the flow's reference unit after quantity and
waste-sign conversion. It does not replace the nominal ``resultingAmount`` or
its parameter expression. Core ILCD retains its matching-centre guard.

The installed openLCA 2.6.2 log-normal sampler interprets its first parameter as
an arithmetic mean: it samples with underlying normal mean
``log(parameter1) - sigma**2 / 2``, despite documenting the parameter as a
geometric mean. BrightPath therefore writes partial
``olca:amount = sign * exp(loc + sigma_exported**2 / 2)`` for log-normal
distributions, including ones whose nominal amount equals their median.
``sigma_exported`` is derived from the actual, potentially rounded native
geometric standard deviation. This preserves canonical ``loc`` under native
sampling, including after permitted spread rounding and unit conversion.

The openLCA reader performs the inverse using actual XML values:
``loc = log(abs(native_amount)) - sigma_readback**2 / 2`` and restores the
``negative`` flag from the native mean's sign. It uses the partial
``olca:amount`` when present and otherwise the core amount, matching the native
importer. Thus foreign or older openLCA packages without the partial attribute
are interpreted according to their actual native sampling distribution.
Audit comments never override these parameters. The core dialect does not
apply this native sampler correction.

Do not complete the exchange extension with ``olca:unitId`` and
``olca:propertyId``: the full tuple activates a different importer branch that
replaces the nominal amount and skips uncertainty conversion. Partial amount
attributes cannot provide extra precision for the dispersion field.

An isolated native importer, expression interpreter and sampler check verified
centred and independent log-normal distributions, rounded spread, independent
and signed normal centres, and kWh-to-MJ conversion. A further native check
verified finite negative log-normal samples, their exact underlying normal
centre, nominal parameter expressions, a selected foreground supplier, and the
reversible waste-flow coordinate convention. Signed-native log-normal exports
require a nonzero nominal amount whose sign agrees with the canonical
``negative`` flag. Contradictory signs and zero nominal values remain guarded.
When the flag is absent, its effective value follows Brightway and is inferred
from whether the nominal amount is negative. The reader always returns the
actual native distribution sign explicitly, including ``negative=False`` when
needed to distinguish it from the nominal sign in a foreign native file.

Dispersion precision
~~~~~~~~~~~~~~~~~~~~

The schema's three-decimal field can prevent exact representation; by default
such exports fail explicitly in both dialects. openLCA 2.6.2 has no native extension for an
exact normal or log-normal dispersion. ``baseUncertainty`` is a separate field,
and complete amount/unit/property extensions bypass its uncertainty converter;
neither provides a lossless substitute for rounded dispersion.

An application can explicitly authorize bounded, export-only approximation for
the ``openlca`` dialect:

.. code-block:: python

   changes = []
   write_ilcd(
       document, "inventory-ilcd.zip", dialect="openlca",
       metadata_profile=profile,
       uncertainty_relative_tolerance=0.02,
       uncertainty_rounding_warnings=changes,
   )

``render_ilcd_package`` accepts the same two keywords. The tolerance defaults
to ``0.0`` (strict), must be finite, and cannot exceed ``0.02`` (2%). A smaller
explicit tolerance, including ``0.005`` (0.5%), remains supported. Rounding occurs
after native unit and waste-sign conversion. Error is measured in uncertainty
spread: log-standard-deviation ``scale`` for log-normal distributions, absolute
standard deviation for normal distributions. It is not measured in geometric
standard deviation or in the nominal exchange amount. Positive spread can never
become zero, even when floating-point ``isclose`` would otherwise mask the loss.
Core centre/sign guards, unsupported-distribution guards, and reference and
schema validation still apply. Independent openLCA centres use the native
encoding above.
Core exports do not accept a positive tolerance.

The optional list receives one record per rounded exchange only after successful
rendering or file publication. Failed operations leave the collector unchanged.
Rows contain ``dataset_code`` and the original canonical ``exchange_index``
before reference-production sorting; ``original_uncertainty`` retains every
present uncertainty type/loc/scale/shape/minimum/maximum/negative field.
``original_amount``, ``original_unit``, ``original_scale`` and ``exported_scale``
use canonical units; ``native_original_scale``, ``native_exported_scale``,
``native_original_dispersion`` and ``native_rounded_dispersion`` describe the
actual wire representation. ``native_amount``, ``native_unit``, ``relative_error``,
``tolerance`` and ``dialect`` complete the audit record. The relative error is
checked in canonical units after reversing native quantity scaling, including
the inclusive boundary without a second ratio affected by unit-conversion drift.

Full records are also preserved in a labeled JSON block in the native process
general comment. Reimport retains this block as informative provenance and
reports ``ilcd.uncertainty_rounded`` warnings. It reads uncertainty from the
actual standard XML field: for example, source log-scale ``0.2`` exported as
``1.221`` reimports as ``log(1.221)``, never as ``0.2``. The source inventory is
not modified. Amounts, formulas and centres receive no additional adjustments.
An unchanged reexport retains prior audit notes without rounding a second time.
ILCD adapter preflight reports the approximation as a structured warning without
appending to an export collector. Applications must present these warnings as an
approved approximation, not an exact conversion.

The ``openlca`` dialect additionally supports triangular uncertainty using
standard minimum/maximum amounts and native ``olca:mostLikelyValue`` for the
exact mode. The nominal exchange amount remains independent of that mode.
Only this mode attribute is emitted, preserving the native uncertainty importer.
Core triangular exports remain unsupported. The openLCA dialect supports
negative log-normal uncertainty using the signed partial amount encoding above.
Core negative log-normal uncertainty remains supported only through the
reversible waste-flow sign convention when its positive XML representation
satisfies the existing dispersion/centre guards. Dispersion precision and
positive-spread-to-zero guards apply to both signs.

CLI and validation
------------------

Use ``brightpath convert-format`` with ``--target-format ilcd`` and the existing
explicit source-context options. ILCD export options are ``--ilcd-metadata``,
``--ilcd-references``, ``--ilcd-dialect`` and ``--ilcd-package-mode``.
``--dry-run`` uses the same renderer and reference checks without writing.

``validate_ilcd_xml`` checks a supported XML entity against the pinned schema.
``validate_ilcd_package`` additionally checks local quantitative reference closure.
Missing auxiliary source/contact XML is retained with a warning; no inventory
quantity or flow reference is invented to compensate for missing data.
Canonical structure and exact technosphere/biosphere validation remain separate
pipeline stages. Format conversion never migrates a background profile.

Independent interoperability evidence
-------------------------------------

On 2026-10-23 an independently executed openLCA 2.6.2 ILCD importer accepted a
synthetic two-process package in a fresh in-memory Derby database. The check
verified a 100 kilogram reference output, a negative input of -2 kilograms,
and the exact selected foreground provider. It used openLCA's own Java
importer, independently of BrightPath's XML reader. A reference exchange with
amount 1 and normal standard deviation 0.1 also retained its distribution
parameters through native import/export. This establishes that
specific process-linking case, not universal background or LCIA compatibility.
The native openLCA exporter was then used to create an independent package;
BrightPath reimported and re-exported it with the same tested quantities and
selected foreground supplier. Native references to an omitted ILCD format
source document were retained with explicit auxiliary-reference warnings.

Repeat the synthetic check with an installed openLCA 2.6 runtime::

   python scripts/verify_ilcd_openlca.py \
       --java /path/to/java \
       --libraries /path/to/openlca/libs \
       --output /tmp/new-ilcd-pilot

The output directory must be new. It receives synthetic packages, native logs
and ``report.json``. The check uses an in-memory Derby database and does not
access a user's openLCA database. The runtime is an optional verification tool.
