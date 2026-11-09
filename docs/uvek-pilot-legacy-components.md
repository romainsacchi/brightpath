Three legacy component correspondences
=====================================

Only the inspected ecoinvent 3.6 cut-off cases are covered. The historic 160 kWe
CHP heat-pump component uses the complete native heat exchanger rather than the
whole CHP assembly. The flight-hour service uses the native single-engine
without-LTO inventory; the separately listed LTO service uses the native cycle.
All caller quantities, signs and supported uncertainty retain factor +1.

The native 2026 hourly helicopter record has a positive one-hour reference. Its
legacy 2025 catalog descriptor incorrectly says year. An on-demand copy of the
complete native hourly inventory avoids that intermediate catalog inconsistency:
two native technosphere inputs and 22 biosphere exchanges, with no time conversion.
The original native process is unchanged. Input coordinates and marginal
uncertainty metadata are preserved; joint stochastic correlation equivalence is
not claimed. Native compiled matrices introduce their own coefficient precision.

The packaged review records substantial fleet, fuel consumption, geography,
water-emission, steel-processing and vintage differences. These are screening
proxies with unquantified score effects, not LCIA equivalence claims. Other
releases cannot inherit these three approvals.

Validation: 29 focused migration/authoring cases; full resource generation
replays byte identically. Separate Premise checks cover cold/warm harmonization,
portable limitations and destination supplier resolution.
