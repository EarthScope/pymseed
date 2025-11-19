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
        for msr in msreader:
            # Skip records completely outside the time window
            if (args.earliest and msr.endtime < args.earliest) or (
                args.latest and msr.starttime > args.latest
            ):
                continue

            # Trim if record overlaps with time window boundaries
            output_record = msr.record
            if (args.earliest and msr.starttime < args.earliest <= msr.endtime) or (
                args.latest and msr.starttime <= args.latest < msr.endtime
            ):
                trimmed_record = trim_record(msr, args.earliest, args.latest)
                if trimmed_record:
                    output_record = trimmed_record

            # Write record to stdout
            sys.stdout.buffer.write(output_record)
            records_written += 1
            bytes_written += msr.reclen

    print(f"Wrote {records_written} records, {bytes_written} bytes", file=sys.stderr)


def trim_record(msr, earliest, latest):
    """Trim a miniSEED record to the specified start and end times."""
    # Cannot trim time coverage of a record with no coverage
    if msr.samplecnt == 0 and msr.samprate == 0.0:
        return None

    # Re-parse the record and decode the data samples
    buffer = bytearray(msr.record)  # Mutable/writable buffer required
    with MS3Record.from_buffer(buffer, unpack_data=True) as msreader:

        # Read/parse the single record
        record = msreader.read()

        data_samples = record.datasamples[:]
        start_time = record.starttime
        end_time = record.endtime
        sample_period_ns = int(NSTMODULUS / record.samprate)

        # Trim early samples to the earliest time
        if earliest and start_time < earliest <= end_time:
            # Use ceiling division to ensure we skip enough samples
            samples_to_skip = -((start_time - earliest) // sample_period_ns)
            start_time += samples_to_skip * sample_period_ns
            data_samples = data_samples[samples_to_skip:]

        # Trim late samples to the latest time
        if latest and start_time <= latest < end_time:
            # Use ceiling division to ensure we remove enough samples
            samples_to_remove = -((latest - end_time) // sample_period_ns)
            data_samples = (
                data_samples[:-samples_to_remove]
                if samples_to_remove > 0
                else data_samples
            )

        if not data_samples:
            return None

        # Pack the trimmed record
        record.starttime = start_time
        record_buffer = b""
        for packed_record in record.generate(
            data_samples=data_samples, sample_type=record.sampletype
        ):
            record_buffer += packed_record

        return record_buffer


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
        "time window boundaries are trimmed to fit within the window.",
    )
    parser.add_argument(
        "--earliest",
        "-e",
        type=parse_timestr,
        help="Earliest time to include (ISO format: YYYY-MM-DDTHH:MM:SS)",
    )
    parser.add_argument(
        "--latest",
        "-l",
        type=parse_timestr,
        help="Latest time to include (ISO format: YYYY-MM-DDTHH:MM:SS)",
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
