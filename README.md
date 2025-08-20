# pymseed - a Python package to read and write miniSEED formatted data

The pymseed package supports reading and writing of miniSEED formatted data.
Both [miniSEED version 2](https://fdsn.org/pdf/SEEDManual_V2.4.pdf)
(defined in the SEED standard) and [miniSEED version 3](https://docs.fdsn.org/projects/miniseed3)
are supported.

The package is uses the C-language [libmseed](https://earthscope.github.io/libmseed)
for most of the data format and manipulation work.

## Installation

The [releases](https://pypi.org/project/pymseed/) should be installed
directly from PyPI with, for example, `pip install pymseed`.

If using numpy features use optional dependency "numpy" or install it independently
e.g. `pip install pymseed[numpy]`.

For package develop use optional dependency "dev" for needed dependencies
e.g. `pip install pymseed[dev]`.

## Example usage

Working programs for a variety of use cases can be found in the
[examples](https://github.com/EarthScope/pymseed/tree/main/examples) directory of the repository.

Read a file and print details from each record:
```python
from pymseed import MS3Record, TimeFormat

input_file = "examples/example_data.mseed"

for record in MS3Record.from_file(input_file):
    # Print values directly
    print(f'   SourceID: {record.sourceid}, record length {record.reclen}')
    print(f' Start Time: {record.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_SPACE_Z)}')
    print(f'    Samples: {record.samplecnt}')

    # Alternatively, use the library print function
    record.print()
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

Writing miniSEED requires specifying a "record handler" function that is
a callback to consume, and do whatever you want, with generated records.

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

Copyright (C) 2025 EarthScope Data Services
