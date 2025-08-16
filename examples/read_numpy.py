#!/usr/bin/env python3
#
# Read miniSEED file(s) using the pymseed package and decode
# data samples directly into NumPy arrays.
#
# The data are organized into a list of dictionaries, one for
# each contiguous trace of data.  The dictionaries contain very
# basic metadata and a NumPy array of data samples.  This data
# structure is for illustration only, and would like need
# adaptation for a real application.
#
# This file is part of the pymseed package.
# Copyright (c) 2025, EarthScope Data Services

import os
import sys
import pprint
from pymseed import MS3TraceList, sourceid2nslc

input_files = []

# Verify that input files are readable
for arg in sys.argv[1:]:
    if os.access(arg, os.R_OK):
        input_files.append(arg)
    else:
        sys.exit("Cannot read file: %s" % arg)

if not input_files:
    sys.exit("No input files specified")

# List of dictionaries for each trace
trace_info = []

traces = MS3TraceList()

# Read all input files, creating record lists and _not_ unpacking data samples
for file in input_files:
    print("Reading file: %s" % file)
    traces.read_file(file, unpack_data=False, record_list=True)

for traceid in traces:
    for segment in traceid:

        # Create a numpy array containing the data samples of this segment
        data_samples = segment.create_numpy_array_from_recordlist()

       # Create a dictionary for the trace with basic metadata
        info = {
            "sourceid": traceid.sourceid,
            "NSLC": sourceid2nslc(traceid.sourceid),
            "publication_version": traceid.pubversion,
            "start_time": segment.starttime_str(),
            "end_time": segment.endtime_str(),
            "sample_rate": segment.samprate,
            "data_samples": data_samples,
        }

        trace_info.append(info)

# Pretty print the trace list
pp = pprint.PrettyPrinter(indent=4, sort_dicts=False)
pp.pprint(trace_info)
