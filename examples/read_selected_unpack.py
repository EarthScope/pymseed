#!/usr/bin/env python3
"""
Read miniSEED files and selectively unpack data samples.

This example demonstrates how to selectively unpack and processes
data samples.

When processing a large volume of data this strategy allows you
to limit the amount of data read into memory at any one time.
The strategy requires reading the data twice, with the first read
summarizing the data and the second read unpacking the data into
NumPy arrays when desired.

Usage:
> python read_selected_unpack.py [file1.mseed] [file2.mseed] ...

This file is part of the pymseed package.
Copyright (c) 2026, EarthScope Data Services
"""

import argparse
import sys

import numpy as np

from pymseed import MS3TraceList


def process_data(trace, segment, data_array):
    """Calculate min, max, and mean of the data array."""

    if len(data_array) == 0:
        raise ValueError(f"Data array is empty for segment {segment.sourceid}")

    min_value = np.min(data_array)
    max_value = np.max(data_array)
    mean_value = np.mean(data_array)

    return min_value, max_value, mean_value


if __name__ == "__main__":
    # Simple argparse setup
    parser = argparse.ArgumentParser(description="Read miniSEED files and convert to NumPy arrays")
    parser.add_argument("files", nargs="*", help="miniSEED files to read")
    args = parser.parse_args()

    # Check if files were provided
    if not args.files:
        parser.print_help()
        sys.exit(1)

    input_files = args.files

    # Read all files, explicitly not unpacking data samples and creating a record list
    traces = MS3TraceList()

    for filename in input_files:
        print(f"Reading: {filename}")
        try:
            traces.add_file(filename, unpack_data=False, record_list=True)
        except Exception as e:
            print(f"Warning: Could not read {filename}: {e}")
            continue

    # Unpack and process each trace segment independently
    for trace in traces:
        for segment in trace:
            # Skip segments with less than 100 samples
            if segment.samplecnt < 100:
                print(f"Skipping segment {trace.sourceid} with {segment.samplecnt} samples")
                continue

            # Unpack the data samples into a NumPy array
            data_array = segment.create_numpy_array_from_recordlist()

            # Process the data
            try:
                min_value, max_value, mean_value = process_data(trace, segment, data_array)
            except Exception as e:
                print(f"Warning: Could not process segment for {trace.sourceid}: {e}")
                continue

            # Report the processed results
            print(f"Trace {trace.sourceid}")
            print(f"  Time: {segment.starttime_str()} to {segment.endtime_str()}")
            print(f"  Sample rate: {segment.samprate} Hz")
            print(f"  Samples: {len(data_array)}")
            print(f"  Data range: {min_value:.2f} to {max_value:.2f}")
            print(f"  Mean: {mean_value:.2f}")
            print()
