Delivered-gas reference bases
============================

Two exact cut-off identities restore three CLIC inputs using complete native
high-pressure gas supplies. The source cubic-metre caller reference is retained
by a product helper; its native input uses fuel MJ. Caller amounts, formulas and
supported uncertainty retain factor +1. No equal-volume m3/Nm3 assertion,
extra distribution network, end-use burner or stack-emission correction is added.

For Swiss ecoinvent 3.9 gas, the source explicitly specifies 36 MJ net energy per
reference Sm3 at 15 degrees C and 1013 mbar. The helper purchases 36 MJ of the
complete native Swiss high-pressure consumer supply.

For the European ecoinvent 3.6 market group, a source-specific NCV is unavailable.
Its declared dry/wet mass is 0.842655842955148 kg per reference m3. Apply the
published generic legacy natural-gas NCV of 45.4 MJ/kg as an explicit fuel-quality
screening assumption, yielding 38.256575270163715 MJ of complete native RER
high-pressure supply. This is an estimate, not a verified source NCV; the gross
energy property is not used as net energy and the Swiss 36 MJ value is not copied.

The generic property comes from ecoinvent's 2007 Overview and Methodology,
Table 4.6, printed page 16:
https://support.ecoinvent.org/hubfs/Knowledge%20Base/Database/Releases/overview_methodology_ecoinvent%20version%202.pdf

Native supplies include production/trade, transport, compression, infrastructure,
own fuel demand and leakage. Fuel quality, density/carbon content, origin mix,
geographic boundary, allocation and vintage differ. The legacy estimate's
uncertainty and total score effects remain unquantified; no numerical distribution
or LCIA-equivalence claim is invented. Other releases and reverse/system-model
routes do not inherit either calibration.

Validation: 21 migration/authoring tests, 10 Premise cold/warm/profile tests,
byte-identical resource replay, actual helper LCIs and full encrypted scenario /
certification / Brightway export. Readback verifies the two MJ coefficients,
complete linking, all restored inputs and unchanged original background signatures.
Actual harmonized source gas quantities are checked exactly before normal IAM
updates; subsequent generator quantity changes are recorded independently.
