# Repository Guidelines

## Project Structure & Module Organization

`brightpath/` contains the Python package. Core converters live in `brightpath/bwconverter.py` and `brightpath/simaproconverter.py`; shared helpers are in `brightpath/utils.py`. Packaged conversion data and mappings are under `brightpath/data/export/` and are included by `MANIFEST.in`. `tests/` is the pytest target directory. `examples/` is available for runnable examples, while `dev/` contains exploratory notebooks and source inventory files used during database conversion work. Keep generated notebook checkpoints and ad hoc exports out of normal commits unless they are intentionally part of a data update.

## Build, Test, and Development Commands

- `python -m pip install -r requirements.txt` installs runtime dependencies (`bw2io`, `prettytable`).
- `python -m pip install -e .` installs BrightPath in editable mode for local development.
- `python -m pytest` runs the configured test suite from `tests/`.
- `python -m build` builds source and wheel distributions when the `build` package is installed.

Use Python 3.9 to 3.11, matching `pyproject.toml`.

## Coding Style & Naming Conventions

Format Python code with Black, as required by `CONTRIBUTING.md`. Use 4-space indentation, descriptive snake_case names for functions and variables, PascalCase for classes, and lowercase module filenames. Prefer Sphinx-style docstrings for public functions and classes. Keep structured data in text formats such as CSV, JSON, or YAML when practical.

## Testing Guidelines

Pytest is configured in `pytest.ini` with `testpaths = tests` and `python_files = tests/*.py`. Add tests directly under `tests/` using names such as `test_simaproconverter.py`. Focus coverage on converter behavior, data mapping edge cases, and round-trip import/export assumptions. When adding package data, include a test that verifies the resource can be loaded from an editable install.

## Commit & Pull Request Guidelines

The git history uses short imperative subjects such as `Add BAFU mapping`, `Fix CSV save`, and `Update notebooks...`. Follow that style: one focused change per commit, with a concise verb-led subject. For major changes, open an issue or discuss the approach with maintainers first, per `CONTRIBUTING.md`.

Pull requests should describe the change, note any data files touched, list tests run, and link relevant issues. Include screenshots only for notebook or documentation output where visual changes matter. Follow the repository code of conduct in all project interactions.
