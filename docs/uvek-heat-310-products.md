# Exact 3.10 heat product and processing proxies

The review `brightpath/data/export/uvek_heat_310_products_review.json` qualifies ten
independently inspected ecoinvent 3.10 cut-off dependencies for native UVEK 2026,
through the unchanged 2025 edge. Source market/producer and native inventory hashes
pin the evidence; raw licensed inventories are excluded from Git.

Complete native inventories replace the source product or processing service once.
The reviews cover steel/aluminium metal working, injection-moulded glass-filled
polyamide, petrochemical AE7, film-on-glass PTFE, SAN, EVA, generic sulfonylurea,
thermoforming with calendering and anhydrous liquid ammonia. Signed caller amounts,
formulas and supported uncertainty remain unchanged in the same reference units.

Metal-working services exclude the main material and degreasing, but include their
own processing-loss material. Thermoforming excludes the main polymer and already
has the same 0.977 kg sheet-output statement in both databases. EVA yield is already
embedded in the resin inventory. No repeated yield factor, main feedstock, machine,
factory, polymerisation, dilution factor or recovery credit is introduced.

Native polyamide product prose mentions melamine substitution, but its actual input
is glass-filled Nylon 66; independent upstream checks in both releases confirm that
inventory. The inconsistency and unverified precise fibre fraction remain warnings.
AE7 excludes native water consumption. EVA has incomplete emission estimates and
omits solid wastes. SAN retains known eco-profile exclusions. Sulfonylurea is a
broad chemical-family proxy, with differing internal aqueous reagent bases, not a
specific formulated herbicide. Ammonia uses native 85/15 reforming/partial oxidation
rather than the source producer portfolio. Film heat and processing-scrap boundaries
also differ. These are explicit complete-process screening proxies with unquantified
score effects; native biosphere exclusions remain visible.

No underlying rule, recipe, shared review or native inventory changes. Exact-profile
Premise approvals retain all prior fingerprints. Other releases and system models
receive no new permission. Focused tests cover quantity/sign/uncertainty invariants,
source ownership, other-version refusals and cold/warm warning retention. Installed
wheels and finite native LCIs are checked separately. These checks establish neither
LCIA equivalence nor impact completeness, and do not enable public heat updates.
