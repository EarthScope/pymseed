#!/usr/bin/env python3
#
# Read miniseed files and print channels, start and end times and sample rate.
#

import argparse

from pymseed import MS3TraceList, SubSecond


def print_traces(input_file: str) -> None:

    mstl = MS3TraceList(file_name=input_file)

    for traceid in mstl.traceids():
        for segment in traceid.segments():
            start_time = segment.starttime_str(subsecond=SubSecond.NANO_MICRO)
            end_time = segment.endtime_str(subsecond=SubSecond.NANO_MICRO)
            print(f"  {traceid.sourceid:<26} {start_time:<30} {end_time:<30} {segment.samprate}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some input files.")
    parser.add_argument('input_files', nargs='+', help='List of input files')
    args = parser.parse_args()

    for input_file in args.input_files:
        print_traces(input_file)
