#!/usr/bin/env python3
"""
Read miniSEED files and assemble independent traces using NumPy arrays.

This example demonstrates how to:
- Read miniSEED files using pymseed and create a trace list
- Extract data samples as NumPy arrays without copying data
- Access basic trace metadata

The result is a collection of trace data with no dependency
on any pymseed data structures.

Usage:
> python read_numpy.py [file1.mseed] [file2.mseed] ...

This file is part of the pymseed package.
Copyright (c) 2026, EarthScope Data Services
"""

import argparse
import sys

import numpy as np

from pymseed import MS3TraceList, sourceid2nslc


def read_traces(input_files):
    """Read miniSEED files and return list of trace data with NumPy arrays."""
    trace_data = []
    traces = MS3TraceList()

    # Read all files
    for filename in input_files:
        print(f"Reading: {filename}")
        try:
            traces.add_file(filename, unpack_data=True)
        except Exception as e:
            print(f"Warning: Could not read {filename}: {e}")
            continue

    # Extract data for each trace segment
    for trace_id in traces:
        for segment in trace_id:
            try:
                # Take ownership of the data sample array
                data_array = segment.take_np_datasamples()

                # Organize trace information
                trace_entry = {
                    "source_id": trace_id.sourceid,
                    "network_station_location_channel": sourceid2nslc(trace_id.sourceid),
                    "start_time": segment.starttime_str(),
                    "end_time": segment.endtime_str(),
                    "sample_rate_hz": segment.samprate,
                    "num_samples": len(data_array),
                    "data_samples": data_array,
                }

                trace_data.append(trace_entry)

            except Exception as e:
                print(f"Warning: Could not process segment for {trace_id.sourceid}: {e}")
                continue

    return trace_data


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Read miniSEED files and assemble independent traces"
    )
    parser.add_argument("files", nargs="*", help="miniSEED files to read")
    args = parser.parse_args()

    # Check if files were provided
    if not args.files:
        parser.print_help()
        sys.exit(1)

    input_files = args.files

    trace_data = read_traces(input_files)

    if not trace_data:
        sys.exit("No trace data found")

    # Display trace information
    print(f"\nFound {len(trace_data)} trace segments:")
    print("-" * 80)

    for trace in trace_data:
        nslc = trace["network_station_location_channel"]
        data = trace["data_samples"]

        print(f"Trace {trace['source_id']}, NSLC: {nslc[0]}.{nslc[1]}.{nslc[2]}.{nslc[3]}")
        print(f"  Time: {trace['start_time']} to {trace['end_time']}")
        print(f"  Sample rate: {trace['sample_rate_hz']} Hz")
        print(f"  Samples: {trace['num_samples']:,}")

        # Basic NumPy statistics
        print(f"  Data range: {np.min(data):.2f} to {np.max(data):.2f}")
        print(f"  Mean: {np.mean(data):.2f}, Std: {np.std(data):.2f}")
        print()
