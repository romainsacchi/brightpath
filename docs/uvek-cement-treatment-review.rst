Cement treatment dependencies: cut-off 3.9
========================================

The frozen ``uvek_cement_treatment_39_review.json`` records two inspected
correspondences used while preparing cement inventories for UVEK 2026 with
ecoinvent's 3.10 biosphere. The maintainer replay step modifies mapping resources;
it introduces no runtime conversion branch or Premise dependency.

Polyethylene municipal incineration
----------------------------------

The actual Swiss 3.9 source and native 2026 endpoint have matching 42 wet
composition coordinates and two heating values. The source reference is -1 kg;
the complete native dedicated 0.4%-water incinerator supplies +1 kg of treatment.
Factor -1 preserves the treatment burden for a negative waste input. No moisture
correction, energy credit or wrapper freight is added. The separately reviewed
3.6 target, factor and reference-review entry remain unchanged. Only an exact
3.9 cut-off profile is appended. Version-neutral limitation wording reflects the
two separately inspected profiles; this changes the closed parent fingerprint.
Independent consumers must verify the old projection before refreshing approval.

Concrete inert landfill
----------------------

The old European correspondence selected an empty disposal marker. The actual
source requires a nonempty landfill operation and a share of its facility.
The new helper retains 1 kg of the complete native generic operation and
1.48e-9 native facility units per wet kg, with both original independent
lognormal distributions and pedigree metadata. Its two source market wrappers
have single deterministic +1 suppliers and no biosphere exchanges. Flattening
only these wrappers introduces no extra weighting or uncertainty approximation.

Native Swiss operation and facility functions have prior independent review;
they are screening proxies for the source RoW component supplies. Differences
in heat, materials, freight, geography, vintage, land classes, use and aftercare
remain explicit limitations. No dry-mass, density, capacity, lifetime or moisture
scaling, demolition, crushing, recycling, collection freight or recovery credit
is inferred. Only the actual 3.9 cut-off source is admitted. Other profiles,
reverse routes and conversions between system models remain unsupported.

Evidence and verification
-------------------------

The packaged evidence contains exact identities, source/native references,
complete inventory and metadata fingerprints, authority hashes and derived
target coefficients. Original inventories remain private. Tests cover caller
signs and non-mutation, complete treatment inputs and uncertainty, uninspected
profile refusal, preservation of the previous polyethylene profile, idempotent
replay and rejection of changed parents, helpers or evidence before mutation.
The public review does not claim equal LCIA scores or certification of the
Premise cement sector; that requires its own shared-workflow qualification.
