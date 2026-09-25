# pymseed Documentation

**pymseed** is a Python package for reading and writing miniSEED formatted data.

Both [miniSEED version 2](https://fdsn.org/pdf/SEEDManual_V2.4.pdf) (defined in
the SEED standard) and [miniSEED version 3](https://docs.fdsn.org/projects/miniseed3)
are supported.

The package uses the C-language [libmseed](https://earthscope.github.io/libmseed)
library for most of the data format and manipulation work.

## Key Features

* Read and write miniSEED version 2 and version 3 files
* Iterate over individual records with {class}`~pymseed.MS3Record`
* Organize data into trace lists with {class}`~pymseed.MS3TraceList`
* Convert between time formats and source identifiers
* Thread-safe operation with configurable logging
* Optional NumPy integration for data arrays

## Installation

Install from [PyPI](https://pypi.org/project/pymseed/):

```bash
pip install pymseed
```

Or from [conda-forge](https://anaconda.org/conda-forge/pymseed):

```bash
conda install -c conda-forge pymseed
```

## Quick Example

Read a miniSEED file and print trace information:

```python
from pymseed import MS3TraceList

traces = MS3TraceList.from_file("data.mseed")

print(traces)
```

See the {doc}`introduction` for more on installing and using pymseed.

## Documentation

```{toctree}
:maxdepth: 2

introduction
tutorial
examples/index
api/index
changelog
```

## License

pymseed is licensed under the Apache License, Version 2.0.

Copyright (C) 2026 EarthScope Data Services

## Indices and tables

* {ref}`genindex`
* {ref}`modindex`
* {ref}`search`
