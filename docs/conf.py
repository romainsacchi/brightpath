"""Sphinx configuration for the BrightPath documentation."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY_ROOT))
PACKAGE_VERSION = importlib.import_module("brightpath").__version__

project = "BrightPath"
author = "Romain Sacchi and contributors"
copyright = "2020-2026, Potsdam Institute for Climate Impact Research and Paul Scherrer Institut"
release = ".".join(str(part) for part in PACKAGE_VERSION)
version = ".".join(str(part) for part in PACKAGE_VERSION[:2])

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

autodoc_member_order = "bysource"
autodoc_preserve_defaults = True
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented_params"
autosectionlabel_prefix_document = True
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "format-profile-refactor.md"]
nitpicky = True
nitpick_ignore = {
    ("py:class", "pathlib.Path"),
}
templates_path = ["_templates"]

html_theme = "alabaster"
html_title = f"BrightPath {release}"
html_short_title = "BrightPath"
html_static_path = ["_static"]
html_theme_options = {
    "description": "Validate, migrate, and exchange foreground LCI data",
    "fixed_sidebar": True,
    "github_button": True,
    "github_repo": "brightpath",
    "github_type": "star",
    "github_user": "romainsacchi",
    "page_width": "1180px",
    "sidebar_width": "260px",
}

pygments_style = "friendly"
pygments_dark_style = "monokai"
