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

html_title = f"pymseed {release}"

html_theme_options = {
    "source_repository": "https://github.com/EarthScope/pymseed",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_logo": "earthscope-logo-light.svg",
    "dark_logo": "earthscope-logo-dark.svg",
    "top_of_page_buttons": ["view", "edit"],
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/EarthScope/pymseed",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 24 24">
                    <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/>
                </svg>
            """,
            "class": "",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/pymseed/",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 24 24">
                    <path d="M23.922 13.58v3.912L20.55 18.72l-.078.055.052.037 3.45-1.256.026-.036v-3.997l-.053-.036-.025.092z M23.621 5.618l-3.04 1.107v3.912l3.339-1.215V5.509zM23.92 13.457V9.544l-3.336 1.215v3.913zM20.47 14.71V10.8L17.17 12v3.913zM17.034 19.996v-3.912l-3.313 1.206v3.912zM17.17 16.057v3.868l3.314-1.206V14.85l-3.314 1.206zm2.093 1.882c-.367.134-.663-.074-.663-.463s.296-.814.663-.947c.365-.133.662.075.662.464s-.297.814-.662.946z M13.225 9.315l.365-.132-3.285-1.197-3.323 1.21.102.037 3.184 1.16zM20.507 10.664V6.751L17.17 7.965v3.913zM17.058 11.918V8.005l-3.302 1.202v3.912zM13.643 9.246l-3.336 1.215v3.913l3.336-1.215zM6.907 13.165l3.322 1.209v-3.913L6.907 9.252z M10.34 7.873l3.281 1.193V5.198l-3.28-1.193zM20.507 2.715L17.19 3.922v3.913l3.317-1.207zM16.95 3.903L13.724 2.73l-3.269 1.19 3.225 1.174zM15.365 4.606l-1.624.592v3.868l3.317-1.207V3.991l-1.693.615zm-.391 2.778c-.367.134-.662-.074-.662-.464s.295-.813.662-.946c.366-.133.663.074.663.464s-.297.813-.663.946z M10.229 18.41v-3.914l-3.322-1.209V17.2zM13.678 17.182v-3.913l-3.371 1.227v3.913z M13.756 17.154l3.3-1.2V12.04l-3.3 1.2zM13.678 21.217l-3.371 1.227v-3.912h-.078v3.912l-3.322-1.209v-3.913l-.053-.058-.025-.06-3.336-1.21v-3.948l.034.013 3.287 1.196.015-.078-3.261-1.187 3.26-1.187v-.109L3.876 9.62l-.307-.112 3.26-1.188v.877l.079-.055V6.769l3.257 1.185.058-.061L7.084 6.75l-.102-.037 3.24-1.179v-.083L6.854 6.677v.018l-.025.018v1.523L3.44 9.47v.02l-.025.017v4.007l-3.39 1.233v.019L0 14.784v3.995l.025.037 3.4 1.237.008-.006.007.01 3.4 1.238.008-.006.006.01 3.4 1.237.014-.009.012.01 3.45-1.256.026-.037-.078-.027zM3.493 9.563l3.257 1.185-3.257 1.187V9.562zM3.4 19.96L.078 18.752v-3.913l2.361.86.96.349v3.913zm.015-3.99L.335 14.85l-.182-.066 3.262-1.187v2.374zm3.399 5.231l-3.321-1.209v-3.912l3.321 1.209v3.912zM23.791 5.434l-3.21-1.17v2.338zM20.387 2.643l-3.24-1.18-3.27 1.19 3.247 1.182z"/>
                </svg>
            """,
            "class": "",
        },
        {
            "name": "conda-forge",
            "url": "https://anaconda.org/conda-forge/pymseed",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 24 24">
                    <path d="M8.206 5.866l.005.396H6.754l.006.655v.005l-6.758.002v.211L0 7.973l.02.041c.212.467.663.901 1.257 1.313.594.411 1.335.796 2.145 1.13 1.62.664 3.502 1.12 5.006 1.1.746-.01 1.265.228 1.62.672.341.426.51 1.092.524 1.92L7.94 16.239l.008 1.896H20.29l-.004-1.76-2.63-2.22c.055-2.013.708-3.443 1.777-4.405 1.087-.979 2.61-1.49 4.37-1.616l.195-.015L24 5.872zm.425.422l14.946.006-.004 1.457c-1.737.155-3.29.666-4.424 1.685-.912.822-1.433 2.062-1.691 3.534l-1.617.004.002.422 1.535-.004c-.027.226-.113.4-.123.64l-.893-.003-.002.422.995.004 2.138 1.802-2.941.002c-.724-.675-1.552-1.116-2.416-1.158-.817-.04-1.638.324-2.387 1.04l-2.978-.024 2.248-1.781v-.102c.002-.943-.2-1.72-.64-2.269-.396-.496-1.007-.749-1.741-.79l-.008-4.49h.008zm-1.45.396h1.026l.008 4.404c-1.387-.02-3.125-.404-4.631-1.023-.787-.324-1.507-.698-2.066-1.086C.968 8.6.587 8.203.424 7.86v-.514l6.336-.002v2.16h.422v-2.16h.004l-.004-.435v-.226zm6.935 8.839c.75.037 1.503.436 2.18 1.078l-.002 1.112h-4.345l-.006-1.2c.706-.717 1.443-1.026 2.173-.99zM8.36 16.537l3.16.023.006 1.153h-3.16zm11.5.142l.002 1.034h-3.148V16.68z"/>
                </svg>
            """,
            "class": "",
        },
    ],
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
