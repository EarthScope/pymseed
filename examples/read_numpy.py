#!/usr/bin/env python3
"""
Read miniSEED files and convert data samples to NumPy arrays.

This example demonstrates how to:
- Read miniSEED files using pymseed
- Extract data samples as NumPy arrays
- Access basic trace metadata

Usage:
> python read_numpy.py [file1.mseed] [file2.mseed] ...

This file is part of the pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import argparse
import sys

import numpy as np
from pymseed import MS3TraceList, sourceid2nslc


def read_traces_to_numpy(input_files):
    """Read miniSEED files and return list of trace data with NumPy arrays."""
    trace_data = []
    traces = MS3TraceList()

    # Read all files, explicitly not unpacking data samples and creating a record list
    for filename in input_files:
        print(f"Reading: {filename}")
        try:
            traces.add_file(filename, unpack_data=False, record_list=True)
        except Exception as e:
            print(f"Warning: Could not read {filename}: {e}")
            continue

    # Extract data for each trace segment, creating a NumPy array from the record list
    for trace_id in traces:
        for segment in trace_id:
            try:
                # Create and populate a NumPy array from the record list
                data_array = segment.create_numpy_array_from_recordlist()

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
    # Simple argparse setup
    parser = argparse.ArgumentParser(description="Read miniSEED files and convert to NumPy arrays")
    parser.add_argument('files', nargs='*', help='miniSEED files to read')
    args = parser.parse_args()

    # Check if files were provided
    if not args.files:
        parser.print_help()
        sys.exit(1)

    input_files = args.files

    # Read traces and convert to NumPy arrays
    trace_data = read_traces_to_numpy(input_files)

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
