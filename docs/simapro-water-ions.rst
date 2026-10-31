Reviewed aqueous-ion labels in SimaPro CSV
----------------------------------------

The CSV codec retains exact publisher flow UUIDs, declared
ecoinvent biosphere profiles, water contexts and kilograms. It uses charged stock names for Aluminium, Barium, Cadmium, Cobalt, Lead,
Lithium, Manganese, Mercury, Nickel, Potassium, Silver and Zinc ions, plus
qualified legacy Calcium and Vanadium labels. Sodium retains its separate naming rules.

The names below were checked against the August 2025 substance list for
SimaPro Professional 10.2. Import nomenclature applies to substances
independently of the selected product libraries. The CSV syntax version alone
does not establish that naming convention. Compatibility of the complete file with
Professional 9.5, 9.6 or 10.4 requires a separate check against the definitions
for that version.
See the vendor's `import nomenclature guidance
<https://support.simapro.com/en/articles/504727-how-can-i-choose-an-import-option-in-simapro-cloud-edition>`_.

The vendor's `Professional 10.3/10.4 release changelog
<https://pre-support.sharefile.com/d-se9f78a69160a4ceca89be5be0971d161>`_
was also reviewed. It records substance additions, renames and replacements,
but does not provide a complete stock with defined compartments, substance
units and chemical definitions. Its publisher identifiers do not match the
FlowUUIDs in the checked 10.2 list; these identifiers must not be equated.
The changelog therefore does not establish complete 10.4 compatibility or
justify additional aliases. The profile-specific qualifications below remain.

The three additional correspondences are ``Caesium I -> Cesium (I)``,
``Molybdenum VI -> Molybdenum (VI)`` and ``Strontium II -> Strontium (II)``.
Actual ecoinvent3.12 elementary-flow metadata explicitly defines the source
ions as Cs+, Mo+6 and Sr+2. The pinned August 2025 Professional 10.2 list provides
the corresponding water-emission/kg labels. Molybdenum's explicit Mo+6
formula agrees. NIST's cesium-ion entry identifies stock CAS18459-37-5 as
Cs+; ChEBI/Rhea identify stock CAS22537-39-9 as strontium2+ and record
charge separately from the elemental formula Sr. An elemental formula
display without printed charge does not alone identify a neutral species.

Source CAS metadata disagree with these charged definitions. The discrepancy
is retained explicitly, with no source CAS edit or general formula-over-CAS
precedence rule. This is a reviewed specific chemical-definition correspondence,
not a claim that old and new stock UUIDs are identical. No native9.5 binding,
native desktop acceptance or LCIA equivalence is claimed for the additional
three labels. No publisher stock-history continuity requirement is imposed.

Each correspondence uses its original unique canonical UUID and full water
context. Both quantities are kilograms of the same monatomic flow; the
factor is one. There is no molecular/stoichiometric mass conversion, density
conversion, charge rescaling or substitution of a different generic UUID.
Amounts, signs, formulas and uncertainty parameters remain untouched.

EI3.9/3.9.1 use the exact catalog display names Caesium and Strontium for
the same Cs/Sr UUIDs. The codec uses those profile-specific identities on
readback, without applying a general generic-to-ionic name substitution.
Older3.6–3.8 wire behavior remains unchanged. Inverse parsing requires an
explicit UUID-bearing provider, honors supplied flowsets and rejects wrong,
missing, ambiguous or colliding identities before modification. Generic
Sodium A remains refused by its existing separate guard.

The full19 scope contains94 verified UUID/context pairs. Lithium groundwater-
long-term is outside this verified naming scope. These naming rules preserve
quantities and do not add formula or uncertainty serialization capabilities.

Qualified modern Chromium display names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Chromium III and Chromium VI use ``Chromium (III)`` and ``Chromium (VI)``
for the exact selected water-context/kilogram UUIDs in biospheres3.9–3.12.
The two valences retain their separate canonical UUIDs. The pinned Professional 10.2
stock metadata match the source CAS, formal charged formula, emission role
and kg unit. Exact legacy factor-one mappings cover all ten contexts.

The bare Roman labels are explicitly valid in the public9.4 list; they are
not declared generally invalid. Their old stock UUIDs are absent from the
pinned 10.2 list, so stock-ID continuity and LCIA equivalence are not claimed.
The native9.5 lexical record directly witnesses eight of the ten contexts;
CrIII groundwater-long-term and CrVI ocean were not observed. That limited
absence does not invalidate the modern qualified correspondence.

Biospheres3.6–3.8 keep their unchanged wire output, including ``Chromium``
for the older ``Chromium, ion`` name and the existing ``Chromium VI`` label.
These are baseline behavior controls, not a new generic/species equivalence
claim. No quantity, unit, formula, uncertainty or molecular conversion occurs.

Primary chemical references used by the pinned peer review:

* https://webbook.nist.gov/cgi/cbook.cgi?ID=18459-37-5
* https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:35104
* https://www.rhea-db.org/rhea/78679
