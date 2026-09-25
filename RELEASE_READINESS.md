# BrightPath 1.0.0 release readiness

Initial review: 2026-09-24, on `main` after `6f94021`. Commit preparation and
validation refreshed on 2026-09-25 against `5c9a0d8` plus the release-preparation
changes. The package remains `1.0.0a1`; the changelog describes the upcoming
1.0.0 release. No release was tagged or published.

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
2. Make `olca-schema >=2.6.2,<3` available on a configured Conda channel and verify
   the complete Conda build. The Anaconda package search returned no matching
   package during this review; the conda-forge package endpoint returned 404.
   The corrected recipe declares the dependency, but its availability remains
   a Conda publishing blocker in the initial review. Channel availability was
   not rechecked during commit preparation.
3. Check whether the removed credential-like test literals were real. If so,
   rotate them; replacing working-tree literals does not remove Git history.
4. When ready, synchronize `pyproject.toml`, `brightpath/__init__.py`, and the
   version assertion in `tests/test_brightway_inventory.py` to `1.0.0`; update
   the README alpha notice and date the changelog. They intentionally still
   identify the package as `1.0.0a1` during preparation.
5. Run CI on the final commit, including Linux, macOS, and Windows tests, then
   confirm PyPI trusted publishing and Anaconda credentials/environments are
   configured before creating the release tag. These external settings were
   not checked in this local review.
