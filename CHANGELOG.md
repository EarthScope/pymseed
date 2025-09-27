# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.5] - 2025-09-27
### Fixed
- MS3Record.samprate consistently returns the rate in samples/second,
  and MS3Record.samprate_raw returns the record's raw value.
- Properly return None when MS3Record.sampletype is not set.

### Added
- MS2Record.unpack_data() to decode sample payload after parsing.
- MS3Record.samprate_period_ns to return sample period in nanoseconds.
- MS3Record.samprate_period_seconds to return the period in seconds.

## [0.0.4] - 2025-9-24
### Fixed
- Track update time of segments in an MS3TraceList consistently

### Changed
- Update MS3TraceList.pack() to use new libmseed function for performance
  when packing with a specified `flush_idle_seconds` set.

## [0.0.3] - 2025-9-14
### Added
- MS3TraceList.from_buffer() to create a trace list from miniSEED in a buffer
- MS3TraceList.add_buffer() to add miniSEED data from a buffer
- `buffer` parameter of MS3TracesList initialization
- system_time() to return the current system time in nanoseconds
- `flush_idle_seconds` parameter of MS3TraceList.pack() to control flushing
  of data buffers that have not been updated in a specified number of seconds
- libmseed updated to v3.1.8

### Changed
- MS3RecordReader parameter `source` renamed to `buffer` for consistency

## [0.0.2] - 2025-8-20
### Added
- List-like access to trace IDs in MS3TraceList (indexing, slicing, iteration)
- List-like access to segments in MS3TraceID (indexing, slicing, iteration)
- MS3Record.with_datasamples() is a context manager for setting sample buffer, type, counts
  allowing an MS3Record to be used for record packing with zero-copy of data
- MS3Record.from_file() and MS3Record.from_buffer() for convenience
- MS3TraceList.from_file() for consistency and future flexibility
- MS3TraceList.to_file() for writing miniSEED
- MS3TraceSeg.has_same_data() for comparison
- docstring documentation including examples
- Comprehensive repr() and summary str() methods

### Changed
- Rename MS3TraceList.read_file() to MS3TraceList.add_file() for clarity

### Removed
- MS3TraceList.numtraces in favor of supporting len() directly
- MS3TraceList.traceids() in favor of supporting iteration directly
- MS3TraceID.numsegments in favor of supporting len() directly
- MS3TraceID.segments() in favor of supporting iteration directly
- MS3TraceList.read_files() as unnecessary

## [0.0.1] - 2025-8-5
### Added
- Initial release
- MS3TraceList class for reading miniSEED files
- MS3Record class for individual records
- CFFI-based bindings to libmseed

[Unreleased]: https://github.com/EarthScope/pymseed/compare/v0.0.5...HEAD
[0.0.5]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.5
[0.0.4]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.4
[0.0.3]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.3
[0.0.2]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.2
[0.0.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.1
