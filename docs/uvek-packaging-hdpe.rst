HDPE supply for packaging and fuel dependencies
==============================================

Two independently inspected cut-off profiles extend the existing global HDPE
market correspondence: ecoinvent 3.8 and 3.11. Each receives its own derived
native-delivery helper. This completes an HDPE dependency, not the still-pending
fertiliser/pesticide packaging waste closure or a prospective fuel export.

Stock and delivery
------------------

Both profiles retain their original virgin producer shares separately:
0.190635451504994 kg European and 0.809364548494888 kg rest-of-world production
per kg source stock. Complete native European HDPE granulate represents each
producer as a same-product/unit screening proxy. No density, purity, energy,
recycled-content or avoided-production factor is inferred. Native cumulative
stock, grade, production boundary, allocation, geography, vintage and internal
uncertainty differ, with unquantified score effects and limited prospective
relinking where native embodied burdens are aggregated.

All three source freight entries retain their quantities, order, formulas and
uncertainty. Native generic rail, heavy-road fleet and container-ship services
provide tonne-km proxies, without new payload/distance or infrastructure factors.
The renamed 3.11 transport references are independently inspected in their own
source project rather than inferred from the older labels.

Version-specific omissions
--------------------------

The 3.8 market contains four positive recycled-to-generic supplies, each
2.93285310007683e-14 kg/kg. Their inventories are nonempty. Under the standing
same-kind auto-approval, exactly those four entries are omitted with original
source positions, signed uncertainty, inventory hashes and portable loss audits.
They are not represented by empty recycling handovers or virgin stock. The
unquantified score effect is not asserted zero. Remaining stock and delivery
are not renormalized; no general trace threshold is introduced.

The inspected 3.11 market has only the two virgin producers and three freight
entries. Nothing is omitted. The rule retains a historical global description
of the separately approved 3.9 omission; that description concerns 3.9 only.
Current-profile recipe audits and approval metadata distinguish 3.8's four
losses from 3.11's intact five-row inventory.

Preserved earlier behavior
--------------------------

The selected 3.9 and 3.10 rules, targets, factors, warnings, shared evidence and
two old helper closures remain identical. Only two exact target profiles are
appended to the fingerprinted full rule. Premise refreshes the two previous
full-rule fingerprints after proving those invariants, then adds two new exact
scopes. No other source version or system model is inferred.

``data/export/uvek_packaging_hdpe_review.json`` freezes the evidence and derived
recipes. Replay rejects changed parents, references, helpers or omission scope
before mutation. Tests cover strict links, signed quantities, normal/lognormal
uncertainty, formulas, separate delivery entries, cached CLIC conversion and
refusal when omission authorization is removed. Historical authoring tests
restore their earlier stage only after proving current selected behavior and
old helper closures unchanged. Current complete replay and new-profile tests
cover the extension directly.

The combined regression run also checks isolation of replay tests: intentional
mutations operate on deep copies rather than the shared cached mapping. No
runtime Python module changes. Exact licensed preparation, weighted native LCIs,
package/replay and test results are recorded in Premise's companion receipt.
