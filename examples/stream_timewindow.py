#!/usr/bin/env python3
"""
Read miniSEED file(s) from a stream (stdin), select those that
fall within the selected earliest and latest times, and write out
to a stream (stdout). Records that contain the selected times are
trimmed to the selected times.

Example usage:
 > stream_timewindow.py --earliest 2010-02-27T07:00:00 --latest 2010-02-27T07:10:00 < example_data.mseed > windowed.mseed

This file is part of the pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import argparse
import sys

from pymseed import MS3Record, timestr2nstime, NSTMODULUS

def process_stream(args):
    """Process miniSEED records from stdin, applying time window selection."""
    records_written = 0
    bytes_written = 0

    print("Reading miniSEED from stdin, writing to stdout", file=sys.stderr)

    # Read miniSEED from stdin
    with MS3Record.from_file(sys.stdin.fileno()) as msreader:
        for record in msreader:
            # Skip records completely outside the time window
            if _record_outside_timewindow(record, args.earliest, args.latest):
                continue

            # Check if record needs trimming
            output_record = record.record
            if _record_needs_trimming(record, args.earliest, args.latest):
                trimmed_record = trim_record(record, args.earliest, args.latest)
                if trimmed_record:
                    output_record = trimmed_record

            # Write record to stdout
            sys.stdout.buffer.write(output_record)
            records_written += 1
            bytes_written += record.reclen

    print(f"Wrote {records_written} records, {bytes_written} bytes", file=sys.stderr)


def _record_outside_timewindow(record, earliest, latest):
    """Check if a record is completely outside the specified time window."""
    # Record ends before earliest time
    if earliest and record.endtime < earliest:
        return True

    # Record starts after latest time
    if latest and record.starttime > latest:
        return True

    return False


def _record_needs_trimming(record, earliest, latest):
    """Check if a record overlaps with time window boundaries and needs trimming."""
    needs_early_trim = earliest and record.starttime < earliest <= record.endtime
    needs_late_trim = latest and record.starttime <= latest < record.endtime
    return needs_early_trim or needs_late_trim


def trim_record(record, earliest, latest):
    """Trim a miniSEED record to the specified start and end times."""
    # Cannot trim time coverage of a record with no coverage
    if record.samplecnt == 0 and record.samprate == 0.0:
        return None

    # Re-parse the record and decode the data samples
    buffer = bytearray(record.record)  # Mutable/writable buffer required
    with MS3Record.from_buffer(buffer, unpack_data=True) as msreader:
        record = msreader.read()

        trimmed_data = _trim_data_samples(record, earliest, latest)
        if not trimmed_data:
            return None

        # Pack the trimmed record
        return _pack_trimmed_record(record, trimmed_data)


def _trim_data_samples(record, earliest, latest):
    """Extract and trim data samples based on time window."""
    data_samples = record.datasamples[:]
    start_time = record.starttime
    end_time = record.endtime
    sample_period_ns = int(NSTMODULUS / record.samprate)

    # Trim early samples to the earliest time
    if earliest and start_time < earliest <= end_time:
        count = 0
        while start_time < earliest:
            start_time += sample_period_ns
            count += 1
        data_samples = data_samples[count:]

    # Trim late samples to the latest time
    if latest and start_time <= latest < end_time:
        count = 0
        while end_time > latest:
            end_time -= sample_period_ns
            count += 1
        data_samples = data_samples[:-count] if count > 0 else data_samples

    return {
        'data': data_samples,
        'start_time': start_time
    }


def _pack_trimmed_record(record, trimmed_data):
    """Pack a record with trimmed data samples."""
    # Use a simple approach to capture the packed record
    packed_record = None

    def record_handler(record_bytes, handler_data):
        nonlocal packed_record
        packed_record = bytes(record_bytes)

    # Update the record's start time and pack with trimmed data
    record.starttime = trimmed_data['start_time']
    record.pack(record_handler, data_samples=trimmed_data['data'], sample_type=record.sampletype)

    return packed_record


def parse_timestr(timestr):
    """
    Helper for argparse to convert a time string to a nanosecond time value.
    """
    try:
        return timestr2nstime(timestr)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid time string: {timestr}") from None


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Stream miniSEED records with time window selection",
        epilog="Reads from stdin and writes to stdout. Records overlapping the "
               "time window boundaries are trimmed to fit within the window."
    )
    parser.add_argument(
        "--earliest", "-e",
        type=parse_timestr,
        help="Earliest time to include (ISO format: YYYY-MM-DDTHH:MM:SS)"
    )
    parser.add_argument(
        "--latest", "-l",
        type=parse_timestr,
        help="Latest time to include (ISO format: YYYY-MM-DDTHH:MM:SS)"
    )

    args = parser.parse_args()

    # Validate time arguments
    if args.earliest and args.latest and args.earliest > args.latest:
        parser.error("Earliest time cannot be after latest time")

    if not args.earliest and not args.latest:
        parser.error("At least one of --earliest or --latest must be specified")

    try:
        process_stream(args)
    except BrokenPipeError:
        # Handle broken pipe gracefully (e.g., when piping to head)
        pass
    except KeyboardInterrupt:
        print("\nInterrupted by user", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
