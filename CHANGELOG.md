# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.0] - 2026-04-04

### Added
- Support and tests for writing header-only records, i.e. no data payload.
- `MS3Record.from_filelike()` to read miniSEED from python file-like streams.
- `MS3Record.iter_records()` convenience method to read any supported source.

### Changed
- libmseed updated to v3.4.0

## [0.7.0] - 2026-03-30

### Changed
- **Breaking:** Minimum supported Python is now 3.10.
- Optional extra-header JSON Schema validation uses `jsonschema-rs` instead of
  `jsonschema` (the `pymseed[jsonschema]` extra name is unchanged).
  `MS3Record.validate_extra_headers()` now returns `jsonschema_rs.ValidationError`
  instances instead of `jsonschema.exceptions.ValidationError`.
- Use `orjson` module on all implementations except PyPy for optimization.
- Significant optimization of `MS3RecordValidator.validate()`.

### Removed
- CPython 3.14 free threading wheels.  ABI not stable and dependencies missing.

## [0.6.1]

### Added
- `MS2Record.from_file()` now includes `start_byte_offset` and `end_byte_offset`
  supporting reads of specific byte ranges.

### Changed
- libmseed updated to v3.3.0

## [0.6.0] - 2026-03-12

### Added
- `MS3Record.parse_into()` instance method for reuse-based parsing in high-throughput
  loops to eliminating per-record allocation/deallocation overhead.

### Changed
- `MS3Record.sourceid` property now skips an unnecessary NULL check.
- `cdata_to_string()` no longer wraps `.decode()` in a redundant `str()` call.

## [0.5.0] - 2026-03-11

### Added
- `MS3Record.parse()` for parsing a single record from a bytes-like buffer.

### Changed
- **Breaking:** `MS3Record.from_buffer()` is now a generator. The context
  manager protocol (`with from_buffer(...) as reader`) and `.read()` method
  are no longer available. Plain `for` iteration is unchanged.
  - Replace `with from_buffer(buf) as r: for msr in r:` with
    `for msr in from_buffer(buf):`
  - Replace `with from_buffer(buf) as r: msr = r.read()` with
    `msr = MS3Record.parse(buf)`
- Optimize `MS3Record.from_file()` iteration by inlining next, eliminating a
  method dispatch per record

### Removed
- `MS3RecordBufferReader` class removed. Use `MS3Record.from_buffer()`
  (generator) or `MS3Record.parse()` for single records.

## [0.4.0] - 2026-03-08

### Changed
- `MS3RecordValidator.from_file()` significantly optimized

## [0.3.0] - 2026-03-08

### Added
- `MS3Record.validate_extra_headers()` to return detailed validation errors
- `MS3RecordValidator` class for validating records in buffers and files

### Changed
- Capture libmseed error/warning console output and include in MiniSEEDError exceptions
- Numerous edge-case fixes and usability improvements
- Build wheels for Python 3.14
- libmseed updated to v3.2.4

## [0.2.0] - 2026-01-02

### Added
- `MS3Record.get_extra_header()` to get a specified extra header
- `MS3Record.set_extra_header()` to set a specified extra header
- `MS3Record.merge_extra_headers()` to apply a JSON Merge Patch to extra headers
- `MS3Record.valid_extra_headers()` to validate extra headers

### Changed
- libmseed updated to v3.2.3

## [0.1.0] - 2025-11-19
### Added
- `MS3Record.generate()`, a generator to produce miniSEED records
- `MS3TraceList.generate()`, a generator to produce miniSEED records

### Deprecated
- `MS3Record.pack()` functionality, use `MS3Record.generate()` instead.
- `MS3TraceList.pack()` functionality, use `MS3TraceList.generate()` instead.

### Changed
- Updated examples and inline docs to reflect current recommended patterns

### Changed
- libmseed updated to v3.2.0

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

### Changed
- MS3RecordReader parameter `source` renamed to `buffer` for consistency
- libmseed updated to v3.1.8

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
