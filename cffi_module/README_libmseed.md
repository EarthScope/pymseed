# libmseed source management in pymseed

This directory contains the CFFI module for libmseed used by pymseed.
This module is private and used through Python API of pymseed.

## Updating libmseed

The libmseed source code is in the `libmseed` subdirectory and is simply the
sources from a release available from:
https://github.com/EarthScope/libmseed/releases

The CFFI package requires definitions of the C functions, structures, constants,
etc. to be used from the library.  This is in essence the public API expressed
in `libmseed.h`.  The rub is that the CFFI definition parser cannot understand
complex C headers and so cannot use the library headers directly.   The solution
is to maintain a simplified set of definitions from `libmseed.h` specifically
for CFFI in the `cffi_defs.py` file.

To update to a new release:
1. copy the new sources to the `libmseed` directory
2. add any new files
3. review and update `cffi_defs.py` definitions to make sure it matches any changes in `libmseed.h`
4. commit all changes

p.s. A CFFI-usable header definitions is manually maintained due to lack of
robust solutions to parse C headers into the simplified form.

## Dynamic versioning

During the CFFI module build process (when building wheels) the version of
libmseed is extracted and stored in the pymseed package to ensure they are in
sync.
