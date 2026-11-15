# Hydrogen-train direct supplies: source 3.12 cut-off

The packaged review `data/export/uvek_hydrogen_train_direct_312_review.json`
documents thirteen direct supply correspondences actually required by the six
CLIC hydrogen freight-train foregrounds. Independent source 3.12, native 2025
and native 2026 checks establish positive one references in matching units.
Product, grade, delivery, technology, allocation and incomplete native inventory
limitations remain explicit. These are screening proxies, not LCIA equivalence.
Licensed primary inventories remain private; the review packages bounded
identities, facts and record hashes.

Only PPS and European medium-voltage electricity need new exact-profile sign
entries in the resource. Existing source-profile decisions are unchanged.
The maintainer replay hook verifies frozen parents and evidence, rejects drift
before mutation and is idempotent. No runtime Python or sector logic changes.
The lithium-carbonate suggestion for barium carbonate remains unapproved.

Verification on 2026-11-15: the selected reference-sign, resource-storage,
manifest and new review tests produced **717 passes and 29 failures**. The
failures concern older used-Li-ion-market expectations (28 parameter cases) and
a stale direct-destination count (expected 4,024, actual 4,027). Representative
old-release and source-3.12 battery cases plus the destination-count test also
fail when loading the unchanged preceding published resource, commit
`a945340170a4314aabfee9497dee4c1cb0193ecd`. Only the two unrelated supplier
rules changed in this review. The new replay/idempotence/non-mutation tests and
manifest checks passed in that initial run.

A separate test-maintenance change now checks the actually reviewed used-battery
market: older releases explicitly refuse an unreviewed sign decision; source
3.12 preserves its half-pyrometallurgical/half-hydrometallurgical treatment mix,
four delivery entries and signed uncertainty. No production mapping was changed
to satisfy the older test. The destination count is aligned with the already
published resource. The same complete selected group now passes **746 tests**.
Clean maintainer regeneration also reproduces all three active forward
resources byte for byte; redundant private generated copies were removed after
recording their hashes.
