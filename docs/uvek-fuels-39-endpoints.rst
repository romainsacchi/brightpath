Reviewed native endpoints for ecoinvent 3.9 fuel dependencies
==========================================================

Three corrections use complete native UVEK services instead of the previous
mismatched targets. They apply only to the individually inspected ecoinvent 3.9
cut-off identities. Other source releases and consequential routes are not
approved by these decisions.

* The European low-voltage electricity market uses UVEK's complete European RER
  low-voltage grid, replacing its erroneous Swiss-only target. The native RER
  geography and historical country mix differ from Europe excluding Switzerland.
  Generation, network infrastructure and losses remain in the native model once.
* Solid sodium silicate uses the complete furnace-process solid product instead
  of 80% spray powder. Primary source and native producer inventories agree on
  the sand, soda and steam quantities per one-kg solid reference. Their common
  erroneous phosphate prose is not used to identify the chemistry. Generic CAS
  metadata does not justify a metasilicate, solution or hydrate correction.
  No 1/0.8 multiplier, extra drying or water addition is introduced. Source-market
  delivery and geographical differences remain screening limitations.
* Polyurethane waste uses the dedicated municipal incinerator directly, replacing
  the building-waste wrapper that adds collection transport. Source and native
  waste descriptions agree on composition, 0.2% water and lower heating value.
  The source -1 kg waste reference and native +1 kg service require factor -1.
  Separate caller freight remains. No extra collection, energy credit or repeated
  treatment is added; energy allocation, emissions and residue differences remain.

The frozen evidence is ``uvek_fuels_39_endpoint_review.json``. It contains primary
metadata/inventory hashes and derived reference checks, not original proprietary
source inventories. The maintainer replay refuses changed evidence or parent
rules before mutation. There is no change to Brightpath runtime Python modules.
The existing generator gains one data-review stage; the shared migration engine
continues to apply the mappings.

The new endpoint, prior fuel-dependency and resource-storage group passes 67
checks, including both caller signs, normal and lognormal uncertainty, formulas,
source preservation, refusal of uninspected profiles, and replay/drift guards.
All 572 earlier Premise approval fingerprints remain valid without modification.
This is preparation work: it does not qualify a Premise fuel-sector update,
future IAM market relinking, validation or export.
