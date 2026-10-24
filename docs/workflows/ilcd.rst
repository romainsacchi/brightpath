ILCD process packages
=====================

Scope
-----

``ilcd`` reads and writes ILCD 1.1 process datasets. ZIPs can contain folders
directly at their root or under ``ILCD/``. Python also accepts directories and
standalone process XML when a local reference directory supplies dependencies.
ZIP is the intended CLIC upload and download artifact.

Each process must have exactly one positive product reference output. Unit
process and aggregated-inventory types are preserved without aggregation.
Waste-treatment reference inputs, additional product outputs, allocations,
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

The schema's three-decimal field can prevent exact
representation; such exports fail explicitly. Triangular distributions,
negative log-normal distributions and incompatible uncertainty conventions
require a separately reviewed conversion.

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
