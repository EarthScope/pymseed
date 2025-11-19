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


class StreamStats:
    """Accumulate statistics from a stream of miniSEED records."""
    def __init__(self):
        self.record_count = 0
        self.sample_count = 0
        self.bytes = 0
        self.sourceids = {}  # Per-sourceid statistics

    def __str__(self):
        """Return a string representation of the statistics."""
        printer = pprint.PrettyPrinter(indent=4, sort_dicts=False)
        return printer.pformat(self.to_dict())

    def to_dict(self):
        """Return a dict representation of the statistics."""
        # Create copy to avoid modifying original
        sourceids_copy = {}
        for sid, stats in self.sourceids.items():
            sid_copy = stats.copy()
            if sid_copy["earliest"]:
                sid_copy["earliest_str"] = nstime2timestr(sid_copy["earliest"])
            if sid_copy["latest"]:
                sid_copy["latest_str"] = nstime2timestr(sid_copy["latest"])
            sourceids_copy[sid] = sid_copy

        return {
            "record_count": self.record_count,
            "sample_count": self.sample_count,
            "bytes": self.bytes,
            "sourceids": sourceids_copy,
        }

    def update(self, record):
        """Update statistics with data from a miniSEED record."""
        # Update global statistics
        self.record_count += 1
        self.sample_count += record.samplecnt
        self.bytes += record.reclen

        # Track per-sourceid statistics
        sid = record.sourceid
        if sid not in self.sourceids:
            self.sourceids[sid] = {
                "record_count": 0,
                "sample_count": 0,
                "bytes": 0,
                "earliest": None,
                "latest": None,
            }

        sid_stats = self.sourceids[sid]
        sid_stats["record_count"] += 1
        sid_stats["sample_count"] += record.samplecnt
        sid_stats["bytes"] += record.reclen

        if sid_stats["earliest"] is None or record.starttime < sid_stats["earliest"]:
            sid_stats["earliest"] = record.starttime

        if sid_stats["latest"] is None or record.endtime > sid_stats["latest"]:
            sid_stats["latest"] = record.endtime


def main():
    """Main processing function."""
    print("Reading miniSEED from stdin, writing to stdout", file=sys.stderr)

    # Read miniSEED from stdin and accumulate stats for each record
    stats = StreamStats()
    with MS3Record.from_file(sys.stdin.fileno()) as reader:
        for record in reader:
            # Update statistics with data from the record
            stats.update(record)

            # Write raw miniSEED record to stdout
            sys.stdout.buffer.write(record.record)

    print(stats, file=sys.stderr)

if __name__ == "__main__":
    main()
