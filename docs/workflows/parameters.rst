Parameters and round trips
==========================

Inventory documents and format facades retain activity ``parameters`` together
with separate ``database_parameters`` and ``project_parameters`` lists. Upload
analysis exposes the shared lists on ``AnalysisResult`` as well as the activity
definitions in ``inventory_data``. Consumers extracting one activity must retain
its shared definitions too; the analyzer does not duplicate them into each dataset.

Format preservation
-------------------

Brightway Excel and block CSV/TSV retain the native parameter sections. Inventory
formula columns contain expression text, not executable spreadsheet cells.

SimaPro CSV writes input and calculated parameters at all three scopes and writes
exchange expressions in amount columns. A versioned ``BrightPath parameter v1``
marker in parameter comments preserves additional metadata and supplied calculated
values. Native values, expressions and uncertainty take precedence when a receiving
tool edits them. The marker is round-trip metadata, not authenticated provenance.
Waste sign reversal and water volume-to-mass conversion transform expressions along
with amounts and distributions, without changing the caller's inventory. Unsupported
uncertainty types and unknown water units still fail explicitly.

openLCA JSON-LD writes process and global parameters. Calculated definitions have
``isInputParameter: false`` and retain their formula; input definitions carry native
uncertainty. The existing lognormal conversion between Brightway log-space and openLCA
geometric parameters remains in effect. BrightPath extension metadata preserves
database/project scope, units and other supported parameter fields across reimport.

These checks establish serialization and reimport through BrightPath, not acceptance
of every expression by a native desktop application. Format-specific validation and
conversion policies still apply. Reimport without BrightPath-specific metadata may
lose information that the receiving format cannot represent independently.

Identity and migration safeguards
----------------------------------------

Biosphere mapping resources use ``formula`` for chemical metadata, such as ``CO2``.
Identity migration must not copy that field over an exchange's amount expression.
Existing expressions, and the absence of an expression, are preserved in both
directions. The resource itself is unchanged. The migration fingerprint changes so
clients using it for export caches can invalidate results from the old implementation.
