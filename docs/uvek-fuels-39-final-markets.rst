Chemical references and allocated silica markets
================================================

Four exact ecoinvent 3.9 cut-off market correspondences preserve all 35 original
supplier and freight rows. No source row is omitted or renormalized. Four
reviewed helpers use complete native processes; no runtime Python changes or
alternative Premise transformations are introduced.

Silica sand
-----------

The source market contains two dried-silica producers and one chemical
co-product. Both primary producers use complete native dried silica sand,
including drying, rather than ordinary undried mine sand. All twelve
nonproduction flows of the chemical co-product equal 0.0312 times those of its
paired cryolite product. Its allocated burden is represented by 0.0312 kg of
complete native cryolite inventory per kg co-product sand. This is an
allocated-burden proxy, not physical cryolite consumption or ordinary quarry
production. Native hydrogen-fluoride chemistry replaces source fluosilicic-acid
chemistry, with explicit feedstock, process, allocation, recovery and emission
limitations. The three market shares and five delivery rows remain separate.
No recovery credit or additional quarry burden is invented.

Active chemical references
--------------------------

The `ecoinvent version-2 usage guide, section 5.1.11
<https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/code%20of%20practice_ecoinvent%20version%202.pdf?hsLang=en>`_
reports dissolved chemicals and solvents per kg active substance. Concentration
in a name alone therefore does not justify dividing the native quantity by that
concentration. Original archive hashes and reference witnesses are retained in
the frozen review; licensed inventories remain private.

* Potassium hydroxide retains both source producer shares and four market
  freight entries. Complete native KOH is a screening proxy per active kg,
  following that documented reference convention. Source production explicitly
  references pure KOH and describes rescaling its older production basis.
  No 1/0.856 multiplier or salt-input-derived concentration is introduced.
  Precursor units, chlorine/hydrogen allocation, energy, concentration/drying,
  upstream transport, geography and vintage differ. The native K2O-labelled
  precursor is not treated as ordinary KCl mass.
* Ethanol retains eleven supplier rows and their source quantities. Native
  CN/US/RER original documentation explicitly uses dry active mass; CH source
  and native shares/utilities provide an additional independent basis check.
  Native CH, US and BR suppliers serve those countries, RER proxies European
  and otherwise unlocated dewatering, and BR proxies all sugarcane branches,
  including Colombia and rest-of-world. Complete native processes replace the
  source modern/traditional and annexed/autonomous production and allocation;
  the helpers do not reconstruct these technologies. No 1/0.997 multiplier,
  extra water or additional sugar/electricity credit is added.
* Diethyl ether retains both production shares and eight freight rows. Complete
  native ethylene hydration proxies both source hydration and ethanol-based
  ether production. That second branch changes chemistry and allocation and is
  explicitly warned. Both source and target use active kg; no 1/0.9995
  multiplier, source utility inventory or ethanol credit is added.

All proxies retain native internal inventories, uncertainty and upstream
transport. Original source market delivery is retained separately. Source and
native production boundaries differ, so transport-dominated results require
boundary review rather than a claim of identical shipping. Feedstock, grade,
water, technology, co-product allocation, energy, emissions, geography and
vintage differences have unquantified score effects.

Verification scope
------------------

``data/export/uvek_fuels_39_final_markets_review.json`` freezes the exact source
references, allocation comparison, native witnesses, freight bindings and
compiled rules. Replay rejects changed evidence, parents or bindings before
mutation. Tests cover signed caller quantities, formulas, uncertainty, complete
supplier/delivery rows, exact source-profile scope, strict target linking and
cached CLIC conversion. Independent licensed checks compare all 35 source rows
and solve the weighted native inventories of the four market helpers.

Preparation, package and test results are recorded in Premise's companion
receipt. Acceptance of these mappings does not establish that fuel transformation
or export, further preparation cohorts or other sectors have been qualified.
