#!/usr/bin/env python3
"""
Read miniSEED files and display trace information.

Displays channel ID, start/end times, and sample rate for each trace segment.

Example:
  > python read_traces.py example_data.mseed

This file is part of the pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import argparse
import sys
from pathlib import Path

from pymseed import MS3TraceList, SubSecond


def read_traces(filename: str) -> None:
    """Read and display trace information from a miniSEED file."""
    try:
        traces = MS3TraceList.from_file(filename)
    except Exception as e:
        print(f"Error reading {filename}: {e}", file=sys.stderr)
        return

    # Print header
    print(f"\nFile: {filename}")
    print(f"{'Channel ID':<26} {'Start Time':<30} {'End Time':<30} {'Sample Rate'}")
    print("-" * 95)

    # Print trace information
    for trace in traces:
        for segment in trace:
            start_time = segment.starttime_str(subsecond=SubSecond.NANO_MICRO)
            end_time = segment.endtime_str(subsecond=SubSecond.NANO_MICRO)
            print(f"{trace.sourceid:<26} {start_time:<30} {end_time:<30} {segment.samprate}")


def main():
    """Main function to parse arguments and process files."""
    parser = argparse.ArgumentParser(
        description="Read miniSEED files and display trace information"
    )
    parser.add_argument(
        "input_files",
        nargs="+",
        help="One or more miniSEED files to read"
    )

    args = parser.parse_args()

    # Validate files exist
    for filename in args.input_files:
        if not Path(filename).exists():
            print(f"Error: File '{filename}' not found", file=sys.stderr)
            sys.exit(1)

    # Process each file
    for filename in args.input_files:
        read_traces(filename)


if __name__ == "__main__":
    main()
