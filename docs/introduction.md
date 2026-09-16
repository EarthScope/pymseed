# Introduction

pymseed reads and writes [miniSEED](https://docs.fdsn.org/projects/miniseed3)
formatted data, the standard format for seismological and other geophysical
time series. Both
[miniSEED version 2](https://fdsn.org/pdf/SEEDManual_V2.4.pdf) (defined in the
SEED standard) and [miniSEED version 3](https://docs.fdsn.org/projects/miniseed3)
are supported.

The package uses the C-language [libmseed](https://earthscope.github.io/libmseed)
library for the data format and manipulation work; pymseed is a Python layer
over it.

## Installation

Install from PyPI:

```bash
pip install pymseed
```

Or from conda-forge:

```bash
conda install -c conda-forge pymseed
```

## A simple reading example

Read a file and print details from each record:

```python
from pymseed import MS3Record

input_file = "examples/example_data.mseed"

for msr in MS3Record.from_file(input_file):
    print(f"   SourceID: {msr.sourceid}, record length {msr.reclen}")
    print(f" Start Time: {msr.starttime_str()}")
    print(f"    Samples: {msr.samplecnt}")

    # Alternatively, use the library print function
    msr.print()
```

Or read a file into a trace list, organized by Source ID and continuous
time series segments:

```python
from pymseed import MS3TraceList

traces = MS3TraceList.from_file("examples/example_data.mseed")

for trace in traces:
    print(traceid)

    for segment in traceid:
        print('  ', segment)
```

The {doc}`tutorial` walks through both of these reading styles, writing
miniSEED, working with source identifiers, and thread safety. Working
programs for a variety of use cases can be found in the
[examples](https://github.com/EarthScope/pymseed/tree/main/examples)
directory of the repository, and are listed with explanation on the
{doc}`examples/index` page.

## Package design rationale

The package functionality and exposed API are designed to support the most
common use cases of reading and writing miniSEED data using `libmseed`.
The naming of functions, classes, arguments, etc. often follows the naming
used in the library in order to reference their fundamentals at the C level
if needed; even though this leaves some names distinctly non-Pythonic.
