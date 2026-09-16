# Tutorial

This tutorial walks through the core workflows in pymseed: reading and writing
miniSEED files, working with time and source identifiers, validating data, and
handling errors in threaded code.

## Prerequisites

Install pymseed before you begin:

```bash
pip install pymseed
```

Or from conda-forge:

```bash
conda install -c conda-forge pymseed
```

## Reading miniSEED Data

pymseed offers two reading patterns depending on what is needed:

* {class}`~pymseed.MS3Record` methods iterate over records one at a time -- minimal
  memory use, full access to every record header field.
* {class}`~pymseed.MS3TraceList` reads all records and organizes them into
  continuous time series segments grouped by source ID -- convenient for analysis.

### Record-by-record with MS3Record

Use {meth}`~pymseed.MS3Record.from_file` to iterate over miniSEED records
in a file without loading them all into memory at once:

```{testcode}
from pymseed import MS3Record

for msr in MS3Record.from_file("examples/example_data.mseed"):
    print(f"Source ID  : {msr.sourceid}")
    print(f"Start time : {msr.starttime_str()}")
    print(f"Samples    : {msr.samplecnt}")
    print(f"Sample rate: {msr.samprate} sps")
    print(f"Encoding   : {msr.encoding}")
    print()
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

You can also read from a bytes buffer in memory with
{meth}`~pymseed.MS3Record.from_buffer`, or from an open file-like object with
{meth}`~pymseed.MS3Record.from_filelike`:

```{testcode}
from pymseed import MS3Record

with open("examples/example_data.mseed", "rb") as fh:
    buf = fh.read()

for msr in MS3Record.from_buffer(buf):
    print(msr.sourceid, msr.starttime_str())
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

### Trace list with MS3TraceList

{class}`~pymseed.MS3TraceList` assembles records into trace IDs (one per source ID)
and segments of continuous time series.

```{testcode}
from pymseed import MS3TraceList

traces = MS3TraceList.from_file("examples/example_data.mseed")

# Print a summary of the trace listing with gap information to the console
traces.print(details=1, gaps=True)

# Iterate over trace IDs and their segments
for trace in traces:
    print(f"Channel: {trace.sourceid}")
    for segment in trace:
        print(f"  {segment.starttime_str()} - {segment.endtime_str()}")
        print(f"  {segment.samplecnt} samples @ {segment.samprate} sps")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

TraceIDs and segments also support indexing and slicing:

```{doctest} tutorial-tracelist-indexing
>>> from pymseed import MS3TraceList
>>> traces = MS3TraceList.from_file("examples/example_data.mseed")
>>> len(traces)
3
>>> traces[0].sourceid
'FDSN:IU_COLA_00_L_H_1'
>>> len(traces[0])
1
>>> traces[0][0].starttime_str()
'2010-02-27T06:50:00.069539Z'
```

### Selecting data with criteria

Reading methods such as ``from_file``, ``from_buffer``, and ``from_filelike``
on both {class}`~pymseed.MS3Record` and {class}`~pymseed.MS3TraceList` can
limit which records are read:

- ``sourceid`` — a glob pattern matched against each record's Source ID
  (for example ``"FDSN:IU_COLA_*"``).  Omit it, or pass ``None``, to match all.
- ``starttime`` / ``endtime`` — date-time strings that bound a time range.
  Records that *overlap* the window are kept, so the retained data can extend
  slightly beyond the requested bounds.  Omit either bound for an open-ended
  range.

For example, read only one channel over a half-hour window into a trace list:

```{doctest} tutorial-selection
>>> from pymseed import MS3TraceList
>>> traces = MS3TraceList.from_file(
...     "examples/example_data.mseed",
...     sourceid="FDSN:IU_COLA_00_L_H_Z",
...     starttime="2010-02-27T07:00:00Z",
...     endtime="2010-02-27T07:30:00Z",
... )
>>> len(traces)
1
>>> traces[0].sourceid
'FDSN:IU_COLA_00_L_H_Z'
>>> traces[0][0].starttime_str()
'2010-02-27T06:59:01.069539Z'
>>> traces[0][0].endtime_str()
'2010-02-27T07:31:43.069538Z'
```

## Accessing Data Samples

A couple of options exist for accessing data samples from
{class}`~pymseed.MS3Record` and {class}`~pymseed.MS3TraceList` objects.
In all cases they are zero-copy methods providing either access to
data samples as a `memoryview` or a {mod}`numpy` array ({class}`numpy.ndarray`).

:::{important}
By default data samples are _not_ decoded.  Set `unpack_data=True` during
reading for access to data samples.
:::

:::{admonition} Lifetime caveat
:class: warning
The memory containing the data samples belongs to the object.  Any
reference to the samples remaining after the object is out of scope
will become invalid.  Make a copy if needed.

An exception is the use of {meth}`~pymseed.mstracelist.MS3TraceSeg.take_np_datasamples`,
which explicitly takes ownership of the data sample memory.
See {ref}`taking-ownership-of-data-samples`.
:::

### From an MS3Record

Data samples are available as a `memoryview` using {meth}`~pymseed.MS3Record.datasamples`:

```{testcode}
from pymseed import MS3Record

for msr in MS3Record.from_file("examples/example_data.mseed", unpack_data=True):

    # memoryview over integers/floats/characters, depending on encoding
    samples = msr.datasamples
    if samples:
        print(f"First sample: {samples[0]}")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

With NumPy installed you can access a zero-copy view of the record data as
a NumPy array using {meth}`~pymseed.MS3Record.np_datasamples`:

```{testcode}
import numpy as np
from pymseed import MS3Record

for msr in MS3Record.from_file("examples/example_data.mseed", unpack_data=True):
    arr = msr.np_datasamples  # numpy.ndarray, same memory as the record
    if arr.size:
        print(f"min={arr.min()}, max={arr.max()}, mean={arr.mean():.2f}")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

### From an MS3TraceList's segments

Data samples in a {class}`~pymseed.MS3TraceList` are available from the contained
{class}`~pymseed.mstracelist.MS3TraceSeg` objects.

Similar to {class}`~pymseed.MS3Record`, data samples are available via
{meth}`~pymseed.mstracelist.MS3TraceSeg.datasamples` and
{meth}`~pymseed.mstracelist.MS3TraceSeg.np_datasamples` methods of
{class}`~pymseed.mstracelist.MS3TraceSeg`.

```{testcode}
from pymseed import MS3TraceList

traces = MS3TraceList()
traces.add_file("examples/example_data.mseed", unpack_data=True)

for trace in traces:
    for segment in trace:
        data = segment.datasamples
        print(f"{trace.sourceid}: {len(data)} samples, mean={np.mean(data):.2f}")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

(taking-ownership-of-data-samples)=
#### Taking ownership of data samples

The {meth}`~pymseed.mstracelist.MS3TraceSeg.take_np_datasamples` method allows
taking ownership of the datasamples as a NumPy array.  The samples returned
are owned by the calling code, independent of pymseed, and remain valid after
the trace list is released.

```{testcode}
import numpy as np
from pymseed import MS3TraceList

traces = MS3TraceList.from_file("examples/example_data.mseed", unpack_data=True)

# Collect values for each continuous segment of data
data_segments = []

for trace in traces:
    for segment in trace:
        data_segments.append((trace.sourceid,
                              np.datetime64(segment.starttime, 'ns'),
                              np.datetime64(segment.endtime, 'ns'),
                              segment.samprate,
                              segment.take_np_datasamples()))

# Release the trace list (illustrating memory reclamation through GC)
del traces

for sourceid, starttime, endtime, samprate, data_samples in data_segments:
    print(f"{sourceid}, {starttime} - {endtime}, {samprate} sps")
    print(f"  {len(data_samples)} samples: [{data_samples[0]} .. {data_samples[-1]}]")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

## Writing miniSEED Data

### Creating miniSEED with MS3TraceList

Use {meth}`~pymseed.MS3TraceList.add_data` to add samples to a trace list, then
write with {meth}`~pymseed.MS3TraceList.to_file`:

```{testcode}
import math
import tempfile
from pathlib import Path
from pymseed import MS3TraceList, timestr2nstime

sample_rate = 40.0
start = timestr2nstime("2024-01-01T00:00:00Z")

# Generate a 500-sample sine wave
data = [int(math.sin(math.radians(x)) * 500) for x in range(500)]

traces = MS3TraceList()
traces.add_data(
    sourceid="FDSN:XX_TEST__B_H_Z",
    data_samples=data,
    sample_type="i",       # 'i' = 32-bit integer, 'f' = float, 'd' = double
    sample_rate=sample_rate,
    starttime=start,
)

output_file = Path(tempfile.gettempdir()) / "output.mseed"
traces.to_file(output_file, format_version=3, max_record_length=4096, overwrite=True)
```

### Streaming records without a file

{meth}`~pymseed.MS3TraceList.generate` yields packed miniSEED records as
{class}`bytes` objects -- useful for writing to network sockets, HTTP responses,
or any stream:

```{testcode}
import math
import tempfile
from pathlib import Path
from pymseed import MS3TraceList, timestr2nstime, sample_time

traces = MS3TraceList()
sample_rate = 100.0
start = timestr2nstime("2024-06-01T12:00:00Z")
batch_size = 200

output_file = Path(tempfile.gettempdir()) / "streamed.mseed"

with open(output_file, "wb") as fh:
    for i in range(10):
        data = [int(math.sin(math.radians(x)) * 1000) for x in range(batch_size)]
        traces.add_data(
            sourceid="FDSN:XX_TEST__H_H_Z",
            data_samples=data,
            sample_type="i",
            sample_rate=sample_rate,
            starttime=start,
        )
        start = sample_time(start, batch_size, sample_rate)

        # Flush only full records; keep partial records in the buffer
        for record in traces.generate(flush_data=False, remove_packed=True):
            fh.write(record)

    # Flush everything remaining in the buffer
    for record in traces.generate(flush_data=True):
        fh.write(record)
```

## Working with Time

All timestamps in pymseed are **nanoseconds since the Unix epoch** (an integer).
The utility functions {func}`~pymseed.timestr2nstime` and
{func}`~pymseed.nstime2timestr` convert between this representation and ISO 8601
strings.

```{doctest} tutorial-time
>>> from pymseed import timestr2nstime, nstime2timestr, TimeFormat, SubSecond

>>> # Parse an ISO 8601 string to nanoseconds
>>> t = timestr2nstime("2024-03-15T10:30:00.123456789Z")

>>> # Format back to a string; choose the precision and style you need
>>> nstime2timestr(t, timeformat=TimeFormat.ISOMONTHDAY_Z)
'2024-03-15T10:30:00.123456789Z'

>>> nstime2timestr(t, timeformat=TimeFormat.ISOMONTHDAY_Z, subsecond=SubSecond.MICRO)
'2024-03-15T10:30:00.123456Z'

>>> nstime2timestr(t, timeformat=TimeFormat.ISOMONTHDAY_SPACE_Z, subsecond=SubSecond.NONE)
'2024-03-15 10:30:00Z'
```

Use {func}`~pymseed.sample_time` to advance a timestamp by a given number of
samples at a known sample rate -- this avoids floating-point drift:

```{doctest} tutorial-time
>>> from pymseed import sample_time
>>> start = timestr2nstime("2024-01-01T00:00:00Z")
>>> next_start = sample_time(start, 512, 40.0)
```

And {func}`~pymseed.system_time` returns the current wall-clock time as a
nanosecond integer:

```{testcode}
from pymseed import system_time, nstime2timestr

now = system_time()
print(nstime2timestr(now))
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

## Working with Source IDs

miniSEED v3 uses FDSN Source Identifiers of the form
`FDSN:NET_STA_LOC_B_S_SS`. pymseed provides helpers for converting between
these identifiers and the classic SEED NSLC (Network, Station, Location, Channel)
notation.

```{doctest} tutorial-sourceid
>>> from pymseed import nslc2sourceid, sourceid2nslc

>>> # Build a source ID from network, station, location, channel components
>>> nslc2sourceid("IU", "COLA", "00", "BHZ")
'FDSN:IU_COLA_00_B_H_Z'

>>> # Parse a source ID back to NSLC
>>> sourceid2nslc("FDSN:IU_COLA_00_B_H_Z")
('IU', 'COLA', '00', 'BHZ')
```

:::{note}
Components that are empty in the source ID are returned as empty strings by
{func}`~pymseed.sourceid2nslc`. A malformed source ID raises `ValueError`.
:::

## Validating miniSEED Files

{class}`~pymseed.MS3RecordValidator` scans a file or buffer and reports every
structural problem it finds, without raising an exception for individual bad
records:

```{doctest} tutorial-validate
>>> from pymseed import MS3RecordValidator
>>> validator = MS3RecordValidator.from_file("examples/example_data.mseed")
>>> errors, tracelist = validator.validate()
>>> len(errors)
0
```

{meth}`~pymseed.MS3RecordValidator.validate` returns a tuple of:

* A list of {class}`~pymseed.ValidationError` objects (empty if the file is
  clean).
* An {class}`~pymseed.MS3TraceList` built from the valid records, or `None`
  if `return_trace_list=False` was passed to the validator.

```{testcode}
from pymseed import MS3RecordValidator

validator = MS3RecordValidator.from_file("examples/example_data.mseed")
errors, tracelist = validator.validate()

if not errors:
    print("File is valid")
else:
    for err in errors:
        print(f"Offset {err.offset}: {err.message}")
        if err.sourceid:
            print(f"  Source: {err.sourceid}, Start: {err.starttime}")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

The validator also accepts a bytes buffer via
{meth}`~pymseed.MS3RecordValidator.from_buffer`.

## Error Handling

All errors from the underlying libmseed library raise
{class}`~pymseed.MiniSEEDError`, a subclass of {class}`~pymseed.PymseedError`:

```{testcode}
from pymseed import MS3TraceList, MiniSEEDError

try:
    traces = MS3TraceList.from_file("missing.mseed")
except MiniSEEDError as e:
    print(f"libmseed error (status {e.status_code}): {e}")
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

libmseed also emits warnings and non-fatal messages to an internal log
registry. Retrieve them with {func}`~pymseed.get_error_messages`:

```{testcode}
from pymseed import MS3TraceList, get_error_messages, clear_error_messages

traces = MS3TraceList.from_file("examples/example_data.mseed")

# Check for any warnings logged during the read
messages = get_error_messages()
for msg in messages:
    print(f"Warning: {msg}")

# Clear the log before the next operation
clear_error_messages()
```

## Thread Safety

pymseed is safe to use from multiple threads, with one rule: threads must not
share mutable objects like {class}`~pymseed.MS3TraceList`.

Each thread needs its own logging registry. The easiest way to set this up is
by passing {func}`~pymseed.configure_logging` as the
{class}`~concurrent.futures.ThreadPoolExecutor` initializer:

```{testcode}
from concurrent.futures import ThreadPoolExecutor
from pymseed import MS3TraceList, configure_logging

def read_file(path):
    traces = MS3TraceList.from_file(path)
    return len(traces)

files = ["examples/example_data.mseed"] * 3

with ThreadPoolExecutor(max_workers=4, initializer=configure_logging) as pool:
    counts = list(pool.map(read_file, files))

print(counts)
```

```{testoutput}
:hide:
:options: +ELLIPSIS

...
```

If you create threads manually, call {func}`~pymseed.configure_logging` at the
start of each thread's target function before any pymseed calls.

## Next Steps

* {doc}`examples/index` -- complete runnable example scripts
* {doc}`api/index` -- full API reference
* [libmseed documentation](https://earthscope.github.io/libmseed) -- the
  underlying C library that pymseed wraps
