#!/usr/bin/env python3
"""
Read miniSEED file(s) from a stream, accumulate stats, and write to a stream.

For this illustration input is stdin, output is stdout, and stats are printed
to stderr on completion.

Example usage:
  cat example_data.mseed | stream_stats.py > output.mseed

This file is part of the pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import pprint
import sys

from pymseed import MS3Record, nstime2timestr


def create_initial_stats():
    """Create a new stats dictionary with default values."""
    return {
        "record_count": 0,
        "sample_count": 0,
        "bytes": 0,
        "pubversions": [],
        "formatversions": [],
        "earliest": None,
        "latest": None,
    }


def update_stats(stats, record):
    """Update statistics with data from a miniSEED record."""
    # Update counters
    stats["record_count"] += 1
    stats["sample_count"] += record.samplecnt
    stats["bytes"] += record.reclen

    # Track unique publication versions
    if record.pubversion not in stats["pubversions"]:
        stats["pubversions"].append(record.pubversion)

    # Track unique format versions
    if record.formatversion not in stats["formatversions"]:
        stats["formatversions"].append(record.formatversion)

    # Track earliest sample time (fix: should use < for earliest)
    if stats["earliest"] is None or record.starttime < stats["earliest"]:
        stats["earliest"] = record.starttime

    # Track latest sample time (fix: should use > for latest)
    if stats["latest"] is None or record.endtime > stats["latest"]:
        stats["latest"] = record.endtime


def main():
    """Main processing function."""
    trace_stats = {}

    print("Reading miniSEED from stdin, writing to stdout", file=sys.stderr)

    # Read miniSEED from stdin and process each record
    with MS3Record.from_file(sys.stdin.fileno()) as reader:
        for record in reader:
            # Get or create stats for this source ID
            if record.sourceid not in trace_stats:
                trace_stats[record.sourceid] = create_initial_stats()

            # Update statistics for this trace
            update_stats(trace_stats[record.sourceid], record)

            # Write raw miniSEED record to stdout
            sys.stdout.buffer.write(record.record)

    # Add human-readable time strings
    for stats in trace_stats.values():
        if stats["earliest"] is not None:
            stats["earliest_str"] = nstime2timestr(stats["earliest"])
        if stats["latest"] is not None:
            stats["latest_str"] = nstime2timestr(stats["latest"])

    # Print statistics to stderr
    printer = pprint.PrettyPrinter(stream=sys.stderr, indent=4, sort_dicts=False)
    printer.pprint(trace_stats)


if __name__ == "__main__":
    main()
