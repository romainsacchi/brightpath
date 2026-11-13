# Municipal-waste treatment: public calculation evidence

The mining inventory-preparation refusal `bp-168324086066bce9` concerns a European
municipal-waste disposal portfolio. Its uncontrolled disposal routes cannot be
represented by the complete Swiss incineration process. This evidence prepares
those missing routes without changing Premise's sector algorithms.

`brightpath/data/export/uvek_msw_public_treatment_calculation.json` contains
independently calculated direct biosphere exchanges for nine exact ecoinvent 3.8 cut-off
cases: open burning, and four infiltration classes each for open dumps and
unsanitary landfills. It is calculation evidence, **not a conversion approval or
a complete treatment inventory**. No mapping rule or supported-sector gate is
changed by this file.

## Sources and reproduction

Use Gabor Doka's original [2017 calculation tools](https://www.doka.ch/WasteTools2017.zip)
and [calculation manual](https://www.doka.ch/05_CalculationManualDoka2017.pdf).
Workbook SHA256 values are recorded in the resource. The reports and workbooks
are not redistributed. The numerical model is recalculated from public inputs;
licensed inventories are used only for a private comparison afterward.

The calculation uses LibreOffice 26.8.3.1, an isolated profile, disabled macros
and full formula recalculation. No runtime dependency on LibreOffice is added.
The Excel interface failed to open these files reliably and is not the engine
used for the recorded results.

1. In the Central Repository, choose `DS info!B12 = 238`: public legacy average
   MSW, waste index 97, with 0.22882637862718752 kg water and 0.3342299889 kg carbon
   per wet kg. The incineration-only mixture at waste index 3 has a different
   composition and was rejected for these cases.
2. Override `DS info!E12` with the appropriate public treatment label. Override
   `D12` with the corresponding public infiltration-class site. The four net
   infiltration rates are 100, 300, 500 and 1000 mm/year; temperature is 8 °C.
   Dump height/operation are 7 m/15 years; unsanitary landfill 15 m/25 years.
   Keep the public calculated dump methane-correction factor; no override is
   introduced. Gas is not captured and leachate is not treated.
3. For open burning, use the legacy global site with `site!B56 = hi`, `B59 = ind`,
   `B64 = 0.95`, `B66 = 0`, and `B68 = 0.95`. These select urban air close to
   ground, industrial soil and the documented iron/aluminium/copper recovery
   rates. They preserve the inspected source receiving compartments and metal
   recovery rather than assuming the workbook defaults.
4. Recalculate the central workbook first. Refresh the model workbook's cached
   external coordinates from that recalculated central file, then recalculate
   the model. The receipt records 716 refreshed coordinates for burning and
   6,683 for each landfill case. Check the changed waste composition in the
   model itself to reject stale cached results.
5. Read numerical means and basic lognormal variances from `Synopsis Exchanges`.
   Export means use four significant digits, in column AI for burning or BL for
   landfill. Round basic variance to five significant digits before adding the
   retained pedigree variance: 0.002 for burning and 0.008 for landfill/dumping.
   The ratings and receiving compartments are recorded explicitly. VBA-dependent
   metadata strings are not used to identify flows. Resolve names and complete
   receiving compartments against the public master-data table instead.

## Verification and limits

All 1,008 nonzero direct-biosphere coordinates match the privately inspected
source means, basic/total variances and lognormal locations. Each of the eight
landfill/dump cases has 115 coordinates; burning has 88. This includes resource exchanges as well as emissions; the resource quantities
are retained too. Source datasets were not modified. Separate fossil/non-fossil flows and near-/long-term groundwater
compartments remain distinct. This is a fixed-case calibration; it does not
establish other versions, system models, waste mixtures or LCIA equivalence.

The actual installed ecoinvent 3.10 biosphere accepts every resulting flow
identity, compartment and unit. Two changed publisher UUIDs reuse existing,
verified same-CAS nickel/potassium groundwater synonyms. Other label changes
retain their publisher UUID. No ionic-mass factor, new synonym or compartment
proxy is introduced.

The public model also reports eight operating inputs across the four unsanitary
cases, and two recovered-metal outputs for burning. They are retained as pending
conversion requirements: their presence does not approve omission, zero burden,
avoided virgin-production credits or their eventual supplier choice.

Before the mining workflow can pass, the complete disposal mapping still needs
reviewed native engineered-treatment proxies, the operating-service suppliers,
cut-off scrap handovers, original route weights and collection freight, followed
by migration, signed-LCI and real mining workflow checks. The positive treatment
helper must reverse the negative waste-input convention exactly once at its
caller. These calculations alone do not satisfy that remaining work.
