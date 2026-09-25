Commands needed for setting up a development environment and running tests

# Contributing

Pull requests should be submitted against the `develop` branch.

Before submitting, run the same checks used by CI and ensure they pass
during development:

pytest
ruff check src/ tests/
mypy src/pymseed

After the PR is created, ensure that the test checks pass in the PR.

# Development environment

## Create the development environment, only needed once
python3 -m venv venv

## Activate the development environment
source ./venv/bin/activate

## Install the module locally using the source in-place
python3 -m pip install --editable '.[dev,numpy]'


# Testing

The package must be installed to be tested because the C library
must be compiled and this is done during packaging.

python3 -m pip install pytest

## Run tests (without coverage analysis - default)
pytest

## Run tests on code blocks in README.md (not included by default)
pytest README.md

## Run tests with coverage analysis (when needed)
pytest --cov=pymseed --cov-report=term-missing --cov-report=html --cov-report=xml

## Run tests with coverage and open HTML report
pytest --cov=pymseed --cov-report=html && open htmlcov/index.html

## Install linting and type checking tools
python3 -m pip install ruff mypy

## Run the linter
ruff check src/ tests/

## Run the type checker
mypy src/pymseed


# Update release version
The single-source package version is specified in:
src/pymseed/__version__.py

The versioning follows the semantic versioning definition.

# Release dependencies
For maintainers publishing releases, install the release dependencies:
pip install pymseed[release]

This provides build and twine for package building and PyPI publishing.

# Building documentation

## Install the docs dependencies
python3 -m pip install --editable '.[docs]'

## Build the HTML site
sphinx-build -W -b html docs docs/_build/html

then open docs/_build/html/index.html

## Run the tutorial and docstring doctests
sphinx-build -b doctest docs docs/_build/doctest

Published at https://earthscope.github.io/pymseed/ by the Documentation
GitHub Action on every push to main.

# Building distribution packages

Distributions are built automatically by a GitHub Action when a
release is created.

The Action has a workflow_dispatch trigger that allows manually
triggering of the build process from here:
https://github.com/EarthScope/pymseed/actions/workflows/release.yml

This is convenient for testing the build process prior to creating
a new release.  These wheels will not be published to PyPI, that only
happens for releases created on github.com

## Triggering GitHub Actions using GH CLI
gh workflow run BuildRelease --ref <branch>


# Building distribution packages manually for testing

## Ensure required build modules are installed
python3 -m pip install build twine

## Build sdist and wheel
python3 -m build

## Test and upload to PyPI

Official releases should be built by the GitHub Action, this
is only needed in special cases, or more likely nevermore.

python3 -m twine check --strict dist/*
python3 -m twine upload dist/*
