#!/usr/bin/env python3

import importlib.util
import sys
from pathlib import Path
from setuptools import setup
from cffi import FFI

# --- Configuration ---
# Get the absolute path to the directory containing setup.py
here = Path(__file__).parent.resolve()
cffi_module_path = here / "cffi_module"
libmseed_path = cffi_module_path / "libmseed"

# --- CFFI Setup ---
# Import the CFFI definitions from the separate definitions file
spec = importlib.util.spec_from_file_location(
    "cffi_defs", cffi_module_path / "cffi_defs.py"
)
cffi_defs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cffi_defs)

# Create FFI instance and configure it with the C definitions
ffi = FFI()
ffi.cdef(cffi_defs.LIBRARY_CDEF)

# Find all C source files.
#
# IMPORTANT: Paths must be explicitly relative to the 'setup.py' directory
# for setuptools to work correctly, especially with 'python -m build'.
# We use pathlib to find the absolute paths and then make them relative to 'here'.
c_sources = [str(p.relative_to(here)) for p in libmseed_path.glob("*.c")]

# --- Platform-specific compiler options ---
if sys.platform.startswith("win"):
    # Windows-specific options
    extra_compile_args = ["/O2"]
    extra_link_args = []
    define_macros = [("_CRT_SECURE_NO_WARNINGS", None)]
else:
    # Unix-like systems (Linux, macOS)
    extra_compile_args = ["-O2"]
    extra_link_args = []
    define_macros = []

# --- CFFI Source Configuration ---
# Configure the CFFI extension module
ffi.set_source(
    "_libmseed_cffi",
    # This is the C code that will be compiled. It includes the header
    # which makes all the C functions available to the CFFI module.
    '#include "libmseed.h"',
    # Provide the list of all C source files to be compiled together.
    sources=c_sources,
    # Provide the include directory, also as a relative path.
    include_dirs=[str(libmseed_path.relative_to(here))],
    # Pass platform-specific compiler and linker arguments.
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    define_macros=define_macros,
)

# --- Setuptools Configuration ---
setup(
    # Tell setuptools that CFFI is a build-time requirement.
    setup_requires=["cffi>=1.0.0"],
    # And also an install-time requirement for the end-user.
    install_requires=["cffi>=1.0.0"],
    # This is where the magic happens: ffi.distutils_extension() creates
    # the Extension object that setuptools will build.
    ext_modules=[ffi.distutils_extension()],
    # The compiled extension will be placed inside the 'pymseed' package.
    ext_package="pymseed",
    # Wheels with compiled extensions are not zip-safe.
    zip_safe=False,
)
