# BrightPath release readiness

## 1.0.0a5 preparation (2026-10-08)

Prepared from `main` at `8feafa6`, including the European primary-aluminium
reverse-migration fix. `pyproject.toml`, the public package version, the version
assertion, and the dated changelog target `1.0.0a5`. Sphinx reads the package
version; Conda receives the same version through the release workflow's `VERSION`
environment variable. No separate Conda version bump is needed.

The fix adds an explicitly lossy reverse proxy, with unchanged quantities and
uncertainty and exact-target validation. It preserves both forward mappings,
retains strict handling of unrelated ambiguities, and refreshes the migration
resource manifest and conversion fingerprint. It requires no database migration
or rewrite of published inventories.

The 251-inventory CLIC reproduction completes migration and Brightway round trips
for ecoinvent 3.10.1, 3.11, and 3.12. The full SimaPro basket remains blocked by
existing category folder names longer than 60 characters. That separate format
limitation is retained in the changelog; this alpha must not be described as
fixing the complete SimaPro basket workflow.

### Local alpha validation

Validated a clean export of tracked files at `8feafa6` plus the five release
preparation files. Untracked local inventories and reports were excluded.

- Source checkout and freshly extracted source distribution: 1,311 tests pass
  in each, with 106 upstream/optional-dependency warnings.
- Separately installed wheel outside the checkout: 39 version, migration,
  manifest and facade tests pass; the openLCA package smoke test passes.
- Black, isort, Ruff, Bandit and strict Sphinx HTML: pass; Bandit reports no issues.
- Source/wheel builds and strict Twine metadata checks: pass.
- All 114 packaged runtime files in each artifact match the prepared source
  byte-for-byte. No local inventory directories, IDE files or checkpoints occur.
- The installed wheel includes the aluminium proxy and validates its integrity
  manifest. Its package and distribution versions are both `1.0.0a5`.
- Conda recipe rendering with `VERSION=1.0.0a5` preserves the matching package
  version and required `olca-schema` dependency. This is not a fresh Conda build.

The prepared artifacts are `dist/brightpath-1.0.0a5-py3-none-any.whl` and
`dist/brightpath-1.0.0a5.tar.gz`. Detailed logs, checksums and the artifact
inspection report are under `/private/tmp/brightpath-alpha5-prep/`.
Checks ran on macOS with Python 3.12 and existing runtime dependencies; a clean
dependency installation, Conda solve/build and Linux/Windows CI remain unverified
for this candidate. No CLIC dependency pin or deployment is changed.

Preparation does not publish packages or create a release tag. After committing
and reviewing this preparation, run the final cross-platform CI checks and use
`v1.0.0a5` for the alpha tag. The existing release workflow accepts alpha tags
and publishes to PyPI and Anaconda; pushing that tag is a publication action.

## Historical preparation

Initial review: 2026-09-24, on `main` after `6f94021`. Commit preparation and
validation refreshed on 2026-09-25 against `5c9a0d8` plus the release-preparation
changes. Those reviews used `1.0.0a1`. Preparation on 2026-10-06 targeted the
`1.0.0a2` prerelease. These older validation results are retained as history;
the current alpha target is documented above.

## Corrections made

- Updated Conda requirements and the Python build variant for Python 3.12,
  setuptools 77+, and the required olca-schema dependency.
- Updated Read the Docs from Python 3.11 to 3.12 and corrected the architecture
  documentation's Python support statement.
- Added strict documentation checks to the release workflow and passed manual
  version input through environment variables instead of inserting it into shell code.
- Fixed import ordering, an explicit zip strictness check, and RST heading lengths.
- Included the changelog, maintainer scripts, and the two tracked review fixtures
  needed by the source archive's tests. Local inventories remain excluded.
- Updated the documentation landing page to list openLCA JSON-LD support.
- Replaced credential-like test literals with dummy values.
- Consolidated the changelog under the upcoming 1.0.0 release.

## Initial validation (2026-09-24)

- Checkout tests: 953 passed.
- Final extracted source-distribution tests: 953 passed, 53 warnings.
- Black, isort, Ruff, and Bandit: passed; Bandit reported no issues.
- Sphinx HTML build with warnings treated as errors: passed.
- Source archive and wheel builds, and Twine metadata checks: passed.
- Wheel import and capability discovery outside the checkout: passed.
- Packaged data checked byte-for-byte against the checkout: passed.
- Distribution inspection found no local inventories, IDE folders, or notebook
  checkpoints. The wheel contains runtime resources; source archives also contain
  the explicitly included test fixtures.

Artifacts and logs are under `/private/tmp/brightpath-release-*` and are temporary.
Checks used Python 3.12 on macOS with the existing dependency environment; a clean
Linux/Windows dependency installation and the Conda build were not run locally.
The environment emitted upstream deprecation, requests dependency, and optional
bw2io importer warnings; these did not fail the checks.

## Commit-preparation validation (2026-09-25)

Validated an isolated copy of the tracked working tree at `5c9a0d8` with the
selected release changes. Untracked inventories and generated UVEK export reports
were excluded from this copy and from the commits.

- Full checkout suite: 1,086 passed, 75 warnings.
- The first source-archive run found seven missing-fixture errors because
  `tests/conftest.py` was absent. Added shared pytest fixtures to `MANIFEST.in`;
  the rebuilt, freshly extracted archive passes all 1,086 tests (75 warnings).
- Black, isort, Ruff, Bandit and strict Sphinx HTML: passed.
- Source/wheel builds without build isolation and Twine metadata checks: passed.
- Required source members, including maintainer scripts, shared pytest fixtures,
  the changelog and two review JSON fixtures: verified in the rebuilt archive.
- All 62 wheel resource files match the checkout byte-for-byte. No local inventory
  directories, generated UVEK export reports, IDE files or notebook checkpoints
  are included in the checked distributions.
- Manual/tag release-version selection, Conda recipe rendering and Python 3.12
  requirement alignment: passed.

Logs, archives and check summaries are in
`/private/tmp/brightpath-targeted-commits-20260925/`. Tests used the existing
Python 3.12 macOS environment. A fresh dependency installation, Conda solve/build,
Linux/Windows CI, publishing configuration and channel availability were not
revalidated. The warning types match the upstream/optional dependency warnings
noted in the initial review.

## Outstanding before a stable release

1. Resolve the existing catalog redistribution review recorded in
   `docs/adr/0003-catalog-and-resource-governance.rst` and the reference-catalog
   manifests (`legal_review_required`). Integrity checks do not establish
   redistribution permission. Record the review outcome or use the documented
   separately licensed/local-provider approach.
2. Publish the required `olca-schema` Conda package alongside BrightPath. The
   release workflow now builds `conda/olca-schema/` from the checksum-pinned
   PyPI 2.6.2 source, then builds BrightPath with the local channel and uploads
   the dependency first. Both pip and Conda keep `olca-schema` mandatory. The
   BrightPath recipe tests an openLCA JSON-LD round trip with the installed
   packages. On 2026-10-06, both Conda packages built and passed their package
   tests locally on macOS with Python 3.12; the 50 openLCA adapter tests also
   passed. The resulting BrightPath package retains the explicit
   `olca-schema >=2.6.2,<3` runtime dependency. Nothing was uploaded; the Linux
   release workflow still needs to run before tagging.
3. Check whether the removed credential-like test literals were real. If so,
   rotate them; replacing working-tree literals does not remove Git history.
4. When ready, synchronize `pyproject.toml`, `brightpath/__init__.py`, and the
   version assertion in `tests/test_brightway_inventory.py` to `1.0.0`; update
   the README alpha notice and date the changelog. They intentionally still
   identify the package as `1.0.0a5` during preparation.
5. Run CI on the final commit, including Linux, macOS, and Windows tests, then
   confirm PyPI trusted publishing and Anaconda credentials/environments are
   configured before creating the release tag. These external settings were
   not checked in this local review.
