# pymseed - a Python package to read and write miniSEED formatted data

The pymseed package supports reading and writing of miniSEED formatted data.
Both [miniSEED version 2](https://fdsn.org/pdf/SEEDManual_V2.4.pdf)
(defined in the SEED standard) and [miniSEED version 3](https://docs.fdsn.org/projects/miniseed3)
are supported.

The package uses the C-language [libmseed](https://earthscope.github.io/libmseed)
for most of the data format and manipulation work.

## Installation

The [releases](https://pypi.org/project/pymseed/) should be installed
directly from PyPI with, for example, `pip install pymseed`.

If using numpy features use optional dependency "numpy" or install it independently
e.g. `pip install pymseed[numpy]`.

For package development use optional dependency "dev" for needed dependencies
e.g. `pip install pymseed[dev]`.

## Example usage

Working programs for a variety of use cases can be found in the
[examples](https://github.com/EarthScope/pymseed/tree/main/examples) directory of the repository.

Read a file and print details from each record:
```python
from pymseed import MS3Record, TimeFormat

input_file = "examples/example_data.mseed"

for msr in MS3Record.from_file(input_file):
    # Print values directly
    print(f'   SourceID: {msr.sourceid}, record length {msr.reclen}')
    print(f' Start Time: {msr.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_SPACE_Z)}')
    print(f'    Samples: {msr.samplecnt}')

    # Alternatively, use the library print function
    msr.print()
```

Read a file into a trace list and print the list:
```python
from pymseed import MS3TraceList

traces = MS3TraceList.from_file("examples/example_data.mseed")

# Print the trace list using the library print function
traces.print(details=1, gaps=True)

# Alternatively, traverse the data structures and print each trace ID and segment
for traceid in traces:
    print(traceid)

    for segment in traceid:
        print('  ', segment)
```

Simple example of writing multiple channels of data:
```python
import math
from pymseed import MS3TraceList, timestr2nstime

# Generate sinusoid data, starting at 0, 45, and 90 degrees
data0 = list(map(lambda x: int(math.sin(math.radians(x)) * 500), range(0, 500)))
data1 = list(map(lambda x: int(math.sin(math.radians(x)) * 500), range(45, 500 + 45)))
data2 = list(map(lambda x: int(math.sin(math.radians(x)) * 500), range(90, 500 + 90)))

traces = MS3TraceList()

output_file = "output.mseed"
sample_rate = 40.0
start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
format_version = 2
record_length = 512

# Add generated data to the trace list
traces.add_data(sourceid="FDSN:XX_TEST__B_S_1",
                data_samples=data0, sample_type='i',
                sample_rate=sample_rate, start_time=start_time)

traces.add_data(sourceid="FDSN:XX_TEST__B_S_2",
                data_samples=data1, sample_type='i',
                sample_rate=sample_rate, start_time=start_time)

traces.add_data(sourceid="FDSN:XX_TEST__B_S_3",
                data_samples=data2, sample_type='i',
                sample_rate=sample_rate, start_time=start_time)

traces.to_file(output_file,
               format_version=format_version,
               max_reclen = record_length)
```

### Converting between Source IDs and NSLC codes

miniSEED 3 and FDSN [Source Identifiers](https://docs.fdsn.org/projects/source-identifiers)
use a single string (for example `FDSN:IU_COLA_00_B_H_Z`). Classic SEED-style
names split the same information into network, station, location, and channel
(NSLC) codes.  SourceIDs are a superset of SEED v2 codes, all SEED codes can
be represented as SourceIDs, but not all SourceIDs will fit into SEED codes.

The utility methods `nslc2sourceid()` and `sourceid2nslc()` allow mapping
between these identifier systems:

```python
from pymseed import nslc2sourceid, sourceid2nslc

# NSLC (four strings) → FDSN source ID
sid = nslc2sourceid("IU", "COLA", "00", "BHZ")
# 'FDSN:IU_COLA_00_B_H_Z'

# Blank location codes are represented as an empty strings
sid2 = nslc2sourceid("XX", "TEST", "", "BHZ")
# 'FDSN:XX_TEST__B_H_Z'

# Source ID → (network, station, location, channel)
net, sta, loc, chan = sourceid2nslc("FDSN:IU_COLA_00_B_H_Z")
assert (net, sta, loc, chan) == ("IU", "COLA", "00", "BHZ")

# To accommodate practical identifier conversion `sourceid2nslc()` does not
# strictly require field lengths for SEED v2 conformance, instead converting
# fields to their most SEED-like form.  For example single character
# Source ID fields of band, source and subsource are collapsed to a SEED
# channel (B_H_Z -> BHZ); but if the codes cannot form a SEED channel
# they are left in the "extended channel" form of Source IDs.  Furthermore,
# larger-than-SEED network, station, and location codes are not truncated
# to fit SEED v2 fields.  For example:

nslc = sourceid2nslc("FDSN:NETWORK_STATION_LOCATION_G_SR_1")
assert nslc == ('NETWORK', 'STATION', 'LOCATION', 'G_SR_1')
```

Invalid source IDs raise `ValueError` from `sourceid2nslc()`; invalid NSLC combinations
raise `ValueError` from `nslc2sourceid()`.

## Threaded usage

The pymseed package is safe to use with threads as long as the threads
are not sharing data structures, e.g. a `MS3TraceList`.

The underlying libmseed library uses thread-local storage for logging,
allowing each thread to have its own logging configuration.

When using threads, call `configure_logging()` in each thread to initialize
the logging registry for that thread. This can be done either explicitly at
the start of the thread function or as an initializer for thread pools:

```python
from concurrent.futures import ThreadPoolExecutor
from pymseed import configure_logging, MS3TraceList

def process_file(filename):
    traces = MS3TraceList.from_file(filename)
    # ... process traces ...
    return len(traces)

# A list of files to process (silly example)
file_list = ["examples/example_data.mseed",
             "examples/example_data.mseed"]

# Using initializer to configure logging for each worker thread
with ThreadPoolExecutor(max_workers=4, initializer=configure_logging) as executor:
    results = executor.map(process_file, file_list)
```

## Package design rationale

The package functionality and exposed API are designed to support the most
common use cases of reading and writing miniSEED data using `libmseed`.
Extensions of data handling beyond the functionality of the library are
out-of-scope for this package.  Furthermore, the naming of functions, classes,
arguments, etc. often follows the naming used in the library in order to
reference their fundamentals at the C level if needed; even though this leaves
some names distinctly non-Pythonic.

In a nutshell, the goal of this package is to provide just enough of a Python
layer to `libmseed` to handle the most common cases of miniSEED data without
needing to know any of the C-level details.

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Copyright (C) 2026 EarthScope Data Services
