Ecoinvent SimaPro representation evidence
=========================================

Scope
-----

SimaPro format rules apply independently of the ecoinvent release. Exact
background catalogs and migrations still use the declared version and system
model. A native reference is evidence of format representation, not a substitute
for validating a target background or its LCIA methods.

The licensed reference reviewed here was exported by SimaPro 9.5.0.2 using CSV
format 9.0.0 on 2026-08-06. It identifies the ecoinvent 3.9.1 cut-off library and
contains 24,043 processes, including 122 waste scenarios and ancillary processes.
Its SHA-256 is
``df2919ec3507ee625fb25dc8580d2bae4bc0a96c1721ecaac5e6a60c8e6cd0d0``.
The inventory remains local; it is not distributed with Brightpath.

Biosphere representation
-------------------------

Actual nonzero exchange rows, rather than comments or substance lists, demonstrate
native representations for oxygen, radon-220/222, carbon-14, xenon-133/135,
unspecified radioactive noble gases, tritium, road occupation, turbine water,
reservoir occupation, and primary-forest biomass energy. These flow names are
no longer excluded from ecoinvent exports. Their existing compartments and
SimaPro units are retained; incompatible units produce a structured error instead
of silent conversion or omission. For example, radionuclides use kBq, reservoir
occupation uses m3y, and road occupation uses m2a.

Generic turbine water retains its generic label. An explicitly regionalized
label retains its region. The exporter does not invent a geographic suffix from
the producer's location or treat regional and generic flows as interchangeable.
UVEK exclusion behavior is unchanged because this reference supplies no evidence
for that background family. Gangue remains an unresolved exclusion reported by
the existing loss machinery.

The reference also contains ``Waste mass, total, placed in landfill`` and
``Organic carbon, placed in landfill`` under ``Final waste flows`` in kg. Canonical
``inventory indicator/waste`` exchanges for these names are rendered there.
Other inventory indicators require an explicit reviewed representation; they
cannot disappear silently. Native final-waste rows are preserved on import,
including their subcompartment, through ``simapro section`` and
``simapro subcompartment`` metadata. This also preserves unfamiliar native rows
without claiming that they match a target ecoinvent catalog.

The earlier Premise 3.12 / REMIND 2050 export excluded 10,232 occurrences.
Synthetic replay of those flow identities with the corrected writer represents
8,055 occurrences and explicitly rejects 2,177 occurrences of hazardous and
non-hazardous waste-disposal indicators that remain unresolved. This is a
representation probe, not a complete inventory re-export or LCIA comparison.

Category inference
------------------

``infer_classifications`` resolves waste status and non-waste category type
separately. Explicit production categories and native ``Category type`` metadata
remain authoritative. Otherwise product identity, unit and CPC support energy,
transport, processing, use, or physical-material classification. The folder path
is independent. Unresolved product roles require an explicit category instead of
silently becoming ``material``.

Positive mass-based markets for recyclable commodities in CPC groups 392/393
are material suppliers; negative waste-sector references remain waste treatment.
Unspecified waste and treatment-service references still require review. These
are product-role rules, not lists of ecoinvent activity identities or release
numbers. They address the paperboard-market discrepancy seen in the reference
and confirmed in local ecoinvent 3.12 source classifications and quantities.

The native audit found 1,347 agreements and three category-type disagreements
among 1,350 matched fallback identities. It also found 7,322 non-waste category
types collapsed to material among 18,701 matched process identities and units.
These are baseline observations, not a guarantee that all cases are resolved by
conservative inference. The 21 generic Premise defaults had no exact native
reference matches and remain a separate mapping review.

Reproduction and limits
-----------------------

Run the audit against a locally licensed export::

    python scripts/audit_simapro_flow_representation.py reference.csv \
        --excluded-report prior.export-report.json --output audit.json

The optional replay uses synthetic processes and amounts, retaining only the
reported flow identities and occurrence counts. Unit tests exercise ecoinvent
3.8, 3.9.1, 3.10 and 3.12 contexts and both supported cut-off and consequential
naming profiles. This tests format behavior; it does not certify every inventory
version or system model in SimaPro.

Premise must pass inventory indicators through to Brightpath and update its
Brightpath dependency before these changes can affect a full scenario export.
Its pre-export filtering and explicit category overrides cannot be corrected
by a downstream writer. Native SimaPro import and LCIA comparison remain
separate validation steps.
