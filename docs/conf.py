# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import importlib.metadata
from pathlib import Path

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pymseed"
copyright = "2026, EarthScope Data Services"
author = "EarthScope Data Services"

# Read from the installed package rather than hardcoding -- autodoc already
# requires an installed pymseed, so this stays in sync for free.
release = importlib.metadata.version("pymseed")
version = release

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.doctest",
    "myst_parser",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

root_doc = "index"

# -- Options for MyST ---------------------------------------------------------

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "attrs_inline",
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
html_static_path = ["_static"]
html_favicon = "_static/favicon.ico"
html_baseurl = "https://earthscope.github.io/pymseed/"

html_title = "pymseed"

html_theme_options = {
    "source_repository": "https://github.com/EarthScope/pymseed",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_logo": "earthscope-logo-light.svg",
    "dark_logo": "earthscope-logo-dark.svg",
    "top_of_page_buttons": ["view", "edit"],
    "light_css_variables": {
        "color-brand-primary": "#37358C",
        "color-brand-content": "#37358C",
        "color-brand-visited": "#CC2929",
    },
    "dark_css_variables": {
        "color-brand-primary": "#8f8cdb",
        "color-brand-content": "#8f8cdb",
        "color-brand-visited": "#e86a6a",
    },
}

# -- Options for autodoc -----------------------------------------------------

# Automatically extract typehints
autodoc_typehints = "description"

autodoc_default_options = {
    "members": True,
    "show-inheritance": True,
    "member-order": "bysource",
}

# Document both class docstring and __init__ docstring
autoclass_content = "both"

# -- Options for autosummary --------------------------------------------------

autosummary_generate = True

# Local TOC entries are already scoped to the page's class; the "Class."
# prefix only forces the sidebar to wrap.
toc_object_entries_show_parents = "hide"

# -- Options for Napoleon (Google-style docstrings) --------------------------

napoleon_google_docstring = True
napoleon_numpy_docstring = False
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_attr_annotations = True

# -- Options for intersphinx -------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "numpy": ("https://numpy.org/doc/stable/", None),
}

# -- Options for nitpicky mode -------------------------------------------------

# Catch broken cross-references instead of silently rendering plain text.
nitpicky = True

nitpick_ignore = [
    # CFFI's own opaque handle type; there is nothing for Sphinx to link to.
    ("py:class", "CData"),
    # Private record-source classes referenced only as type hints on
    # MS3RecordValidator.__init__; not part of the public API, so undocumented.
    ("py:class", "_BufferSource"),
    ("py:class", "_FileSource"),
    ("py:class", "_FileLikeSource"),
    # Return type of the optional jsonschema-rs integration; that package
    # ships no Sphinx inventory to link against.
    ("py:class", "JsonSchemaValidationError"),
]

# -- Options for the doctest builder ------------------------------------------

# Tutorial doctests reference "examples/example_data.mseed" as a repo-root-
# relative path, matching the README and the in-source docstring examples.
# Change into the repo root so `sphinx-build -b doctest` works regardless of
# the directory it is invoked from.
_repo_root = Path(__file__).resolve().parent.parent

doctest_global_setup = f"""
import os
os.chdir({str(_repo_root)!r})
"""
