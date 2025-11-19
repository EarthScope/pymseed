#!/usr/bin/env python3
"""
This example illustrates how to write miniSEED using a rolling
buffer of potentially multi-channel data.

This pattern of usage is particularly useful for applications that need to:
a) generate miniSEED in a continuous stream for an unknown (long) duration,
   i.e. real-time streams.
b) generate miniSEED from a large volume of data while avoiding the need to
   have it all in memory.

In this example, a sine wave generator is used to create synthetic data for 3
channels by producing 100 samples at a time.

The general pattern is (writing bytes to a file for example):

```python
  traces = MS3TraceList() # Create an empty MS3TraceList object

  Loop on input data:
    traces.add_data()    # Add data to the MS3TraceList object

    # Generate filled records during regular data flow
    for record in traces.generate(flush_data=False,
                                  flush_idle_seconds=60,
                                  removed_packed=True):
        # Write the record (bytes) to the output file
        output_file.write(record)

  # Flush any data remaining in the buffers
  for record in traces.generate(flush_data=True,
                                removed_packed=True):
    output_file.write(record)
```

This file is part of the the Python pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import math
from pymseed import MS3TraceList, timestr2nstime, sample_time

output_file = "output.mseed"


def sine_generator(start_degree=0, yield_count=100, total=1000):
    """A generator returning a continuing sequence for a sine values."""
    generated = 0
    while generated < total:
        bite_size = min(yield_count, total - generated)

        # Yield a list of continuing sine values
        yield [
            int(math.sin(math.radians(x)) * 500)
            for x in range(start_degree, start_degree + bite_size)
        ]

        start_degree += bite_size
        generated += bite_size


# Define 3 generators with offset starting degrees
generate_yield_count = 100
sine0 = sine_generator(start_degree=0, yield_count=generate_yield_count)
sine1 = sine_generator(start_degree=45, yield_count=generate_yield_count)
sine2 = sine_generator(start_degree=90, yield_count=generate_yield_count)

output_file = open(output_file, "wb")

traces = MS3TraceList()

total_records = 0
sample_rate = 40.0
start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
format_version = 2
record_length = 512

# A loop that iteratively adds data to traces in the list.
#
# This could be any data collection operation that continually
# adds samples to the trace list.
for i in range(10):
    # Add new synthetic data to each trace using generators
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_0",
        data_samples=next(sine0),
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )

    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_1",
        data_samples=next(sine1),
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )

    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_2",
        data_samples=next(sine2),
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )

    # Update the start time for the next iteration of synthetic data
    start_time = sample_time(start_time, generate_yield_count, sample_rate)

    # Generate full records and do not flush the data buffers
    for record in traces.generate(
        format_version=format_version,
        record_length=record_length,
        flush_data=False,
        flush_idle_seconds=60,
        removed_packed=True,
    ):
        output_file.write(record)
        total_records += 1

# Flush the data buffers and write any data to records
for record in traces.generate(
    format_version=format_version,
    record_length=record_length,
    flush_data=True,
):
    output_file.write(record)
    total_records += 1

output_file.close()

print(f"Packed {total_records} records")
