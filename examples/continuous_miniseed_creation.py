#!/usr/bin/env python3
"""
Generate miniSEED output from a continuous stream of data.

This program illustrates the use of an MS3TraceList to function as a temporary,
or transient, data buffer for a continuous stream of data, and using
MS3TraceList.pack() to continuously generate miniSEED output.

This pattern is useful for generating miniSEED output from a continuous stream
of data from any arbitrary source in a manner that creates full miniSEED records
as much as possible.  Data are either packed into records when a full record is
possible, when the data are idle for a specified number of seconds, or when the
program shuts down.

Usage: python continuous_miniseed_creation.py <output_file>

The output file will be continuously updated with new data.

For this example, a simple sine wave with is generated and used as the data
source.

Example usage:
  > python continuous_miniseed_creation.py output.mseed

This file is part of the pymseed package.
Copyright (c) 2025, EarthScope Data Services
"""

import argparse
import math
import signal
import threading
from collections.abc import Generator
from typing import Any

from pymseed import DataEncoding, MS3TraceList, NSTMODULUS, system_time

# Global flag and event for handling shutdown signals
shutdown_requested = False
shutdown_event = threading.Event()


def signal_handler(signum: int, frame: Any) -> None:
    """Handle shutdown signals (SIGTERM, SIGINT) by setting shutdown flag and event."""
    global shutdown_requested
    print(f"\nReceived signal {signum}, initiating shutdown...")
    shutdown_requested = True
    shutdown_event.set()  # Wake up any waiting threads immediately


def data_source(
    start_time: int = None,
    degree_offset: float = 0.0,
    sample_rate: float = 100.0,
    amplitude: float = 1.0,
    integer_samples: bool = False,
) -> Generator[tuple[list[float], int]]:
    """
    Generate a 1 Hertz sinusoidal signal for a time series starting at the
    specified start time and degree offset with the specified sample rate. The
    generator yields a list of new samples and the time of the first
    sample in the list (as a nanosecond timestamp).

    The number of samples returned is the number needed to fill the series since
    the last invocation.

    The start_time value is the time in nanosecond since the Unix epoch
    (nstime_t in libmseed), such as returned by pymseed.system_time() or
    pymseed.timestr2nstime(). If it is None, the current time is used.

    The degree_offset value is the phase offset in degrees, allowing for
    distinct sinusoids to be generated.

    The amplitude value is the maximum amplitude of the sinusoid. The sinusoid
    will range from -amplitude to +amplitude. Default is 1.0.

    The integer_samples value is a boolean indicating whether the samples should
    be integers or floats. Default is False (float samples).
    """
    if start_time is None:
        start_time = system_time()

    # Calculate the sample interval in nanoseconds
    sample_interval_ns = int(NSTMODULUS / sample_rate)

    # Convert degree offset to radians
    phase_offset = math.radians(degree_offset)

    # Keep track of the next sample time
    next_sample_time = start_time

    while True:
        current_time = system_time()

        # Generate samples until we catch up to current time
        samples = []
        first_sample_time = next_sample_time

        while next_sample_time <= current_time:
            # Calculate time for this sample in seconds
            sample_time_sec = (next_sample_time - start_time) / NSTMODULUS

            # Generate sinusoid sample
            sample_value = amplitude * math.sin(2 * math.pi * sample_time_sec + phase_offset)
            samples.append(int(sample_value) if integer_samples else sample_value)

            # Advance to next sample time
            next_sample_time += sample_interval_ns

        yield (samples, first_sample_time)


def create_continuous_miniseed(
    output_file: str, flush_idle_seconds: int, record_length: int, encoding: DataEncoding, verbose: int = 0
) -> None:
    """
    Create a miniSEED file from a continuous stream of data.

    Generates sinusoid data once per second for two source IDs until terminated
    by SIGTERM or Control-C.

    The data is written to the specified output file.
    """
    global shutdown_requested

    def file_handler(record_bytes: bytes, file_handle: Any) -> None:
        """Handler function to write packed miniSEED records to file."""
        file_handle.write(record_bytes)

    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Parameters for the data series
    sourceid_1 = "FDSN:XX_STA__H_X_1"
    sourceid_2 = "FDSN:XX_STA__B_X_2"
    sample_rate1 = 100.0
    sample_rate2 = 40.0

    # Determine sample type from encoding
    sample_type = "f" if encoding in [DataEncoding.FLOAT32, DataEncoding.FLOAT64] else "i"

    # Create data source generators for sinusoids with a 45 degree phase offset,
    # +/-100 amplitude range, and integer or float samples depending on the encoding
    sinusoid1 = data_source(
        degree_offset=0, sample_rate=sample_rate1, amplitude=100, integer_samples=sample_type == "i"
    )
    sinusoid2 = data_source(
        degree_offset=45, sample_rate=sample_rate2, amplitude=100, integer_samples=sample_type == "i"
    )

    # Create a trace buffer to function as a temporary data buffer
    # holding data until full-length records can be created or flushed
    # when idle.
    trace_buffer = MS3TraceList()

    # Print some information about the data series
    print(f"Starting continuous miniSEED creation to {output_file}")
    print(f"Flush idle time: {flush_idle_seconds} seconds")
    print(f"Record length: {record_length} bytes")
    print(f"Encoding: {encoding.name}")
    print(f"Verbosity level: {verbose}")
    print("Press Control-C to stop...\n")

    # Open the output file for writing
    with open(output_file, "wb") as output_handle:
        loop_count = 0
        while not shutdown_requested:
            try:
                # Get new data from generators, could be an arbitrary source
                new_data1, start_time1 = next(sinusoid1)

                # Only generate data for sourceid_2 every 5th loop (every 5 seconds)
                if loop_count % 5 == 0:
                    new_data2, start_time2 = next(sinusoid2)
                else:
                    new_data2 = []

                # Add any new data to trace buffer for sourceid_1
                if new_data1:
                    trace_buffer.add_data(
                        sourceid=sourceid_1,
                        data_samples=new_data1,
                        sample_type=sample_type,
                        sample_rate=sample_rate1,
                        start_time=start_time1,
                    )
                    if verbose > 1:
                        print(f"Added {len(new_data1)} samples to {sourceid_1}")

                # Add any new data to trace buffer for sourceid_2
                if new_data2:
                    trace_buffer.add_data(
                        sourceid=sourceid_2,
                        data_samples=new_data2,
                        sample_type=sample_type,
                        sample_rate=sample_rate2,
                        start_time=start_time2,
                    )
                    if verbose > 1:
                        print(f"Added {len(new_data2)} samples to {sourceid_2}")
                elif verbose > 2 and loop_count % 5 != 0:
                    print(f"Skipped data generation for {sourceid_2} (not 5th loop)")

                # Pack traces with idle data flushing, but don't flush all data yet
                packed_samples, packed_records = trace_buffer.pack(
                    handler=file_handler,
                    handlerdata=output_handle,
                    flush_data=False,
                    flush_idle_seconds=flush_idle_seconds,
                    record_length=record_length,
                    encoding=encoding,
                )

                if packed_records > 0 and verbose > 0:
                    print(f"Packed {packed_records} records with {packed_samples} samples")

                loop_count += 1

                # Wait for 1 second or until shutdown is requested (interruptible)
                # This allows the program to be terminated by SIGTERM/Control-C
                shutdown_event.wait(1)

            except KeyboardInterrupt:
                # Handle Control-C gracefully
                shutdown_requested = True
                shutdown_event.set()
                break

        # Perform final packing with flush_data=True to ensure all data is written
        print("\nPerforming final data creation to flush remaining data...")
        final_packed_samples, final_packed_records = trace_buffer.pack(
            handler=file_handler,
            handlerdata=output_handle,
            flush_data=True,
            record_length=record_length,
            encoding=encoding,
        )

        print(
            f"Final flush: packed {final_packed_records} records with {final_packed_samples} samples"
        )

        print(f"miniSEED creation complete. Output written to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate miniSEED output from a continuous stream of data until the program is terminated"
    )
    parser.add_argument("output_file", help="The file to write the miniSEED output to")
    parser.add_argument(
        "-f", "--flush_idle_seconds",
        type=int,
        default=10,
        help="The number of seconds of idle time before flushing the data",
    )
    parser.add_argument(
        "-r", "--record_length",
        type=int,
        default=512,
        help="Record length in bytes for miniSEED output (default: 512)",
    )
    parser.add_argument(
        "-e", "--encoding",
        type=str,
        choices=["STEIM1", "STEIM2", "FLOAT32", "FLOAT64", "INT16", "INT32"],
        default="STEIM1",
        help="Data encoding format for miniSEED output (default: STEIM1)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Increase verbosity level (can be used multiple times: -v, -vv, -vvv)",
    )
    args = parser.parse_args()

    # Convert encoding string to DataEncoding enum
    encoding_map = {
        "STEIM1": DataEncoding.STEIM1,
        "STEIM2": DataEncoding.STEIM2,
        "FLOAT32": DataEncoding.FLOAT32,
        "FLOAT64": DataEncoding.FLOAT64,
        "INT16": DataEncoding.INT16,
        "INT32": DataEncoding.INT32,
    }
    encoding = encoding_map[args.encoding.upper()]

    # Create continuous miniSEED output until the program is terminated
    create_continuous_miniseed(
        args.output_file, args.flush_idle_seconds, args.record_length, encoding, args.verbose
    )


if __name__ == "__main__":
    main()
