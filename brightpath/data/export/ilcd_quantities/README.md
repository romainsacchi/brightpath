# ILCD native quantity metadata

`openlca_2_6_2.json` is the existing public unit-template extraction, protected
by `RESOURCE_MANIFEST.json`. It is unchanged by the Sm3 support.

`ecospold_sm3_openlca_2_6_2.json` is a separate metadata resource, protected by
`SM3_RESOURCE_MANIFEST.json`. It contains the ecoinvent unit UUID and definition
of Sm3 plus the unit-group and flow-property identifiers produced by native
openLCA 2.6.2's ecoSpold fallback. It is not a public-template entry or an alias
for ordinary m3 or Nm3. Reference factor 1 preserves canonical coefficients.

Only the exact metadata-confirmed profiles listed in the resource are enabled:
ecoinvent 3.9.1 cutoff, 3.10.1 cutoff/consequential, 3.11 cutoff, and 3.12 cutoff.
The recorded SHA-256 values identify the local licensed `MasterData/Units.xml`
metadata used for verification. The resource contains no background process
inventories or exchange vectors. Original ecoinvent databases remain subject
to their original licence; attribution is to the ecoinvent Association.

The native identifier derivation is documented in GreenDelta's
[`RefDataImport.syncUnit`](https://github.com/GreenDelta/olca-modules/blob/v2.6.2/olca-io/src/main/java/org/openlca/io/ecospold2/input/RefDataImport.java),
distributed under MPL-2.0. No native source code is copied into this resource.
Native `KeyGen` derives a separate unit group and flow property from the exact
source unit UUID when Sm3 is absent from the public template. A fresh in-memory
native check verified creation, repeat lookup, ordinary-volume separation,
actual ILCD import reuse, and binding to an existing synthetic provider.

The generated quantity comments preserve the source conditions: gas at
15 degrees C and 101.325 kPa (ISO 13443). Unknown target builds require explicit
verified mappings; an ordinary-volume target reference cannot substitute for Sm3.

The 3.11 cutoff entry is witnessed by that exact release's
`MasterData/Units.xml`, SHA-256
`e5a424fc15ad1f509743f166f9b4cb156f2d759896d11b9e4342e96189d4dc9b`
(22,080 bytes; archive member CRC32 verified). Its source unit
`225da0bd-ba54-48bc-9a0e-5fa27d722185` is named `Sm3` and defined as a
standard cubic metre of gas at 15 degrees C and 101.325 kPa (ISO 13443).
Only the necessary unit metadata and its checksum are packaged; the native
archive and authentication/retrieval code are not distributed. This adds an
exact verified profile to the existing format mapping, with factor 1 and
unchanged native identifiers. It does not convert Sm3 into Nm3 or ordinary m3,
and does not enable unverified system models or versions.
