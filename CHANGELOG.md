# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `__repr__()` and `__str__()` for `MS3RecordReader` and `MS3RecordValidator`,
  the two public classes that still printed as a bare object address.

### Changed
- `MS3Record.to_file()` accepts `os.PathLike` filenames and encodes them with
  `os.fsencode()`, matching the rest of the package.
- `MS3RecordValidator.from_file()` validates the filename type when called,
  raising `TypeError` as the other path-accepting entry points do, instead of
  accepting `bytes` and deferring to `open()` in `validate()`.
- `MS3Record.from_filelike()` and `MS3TraceList.add_filelike()` raise
  `TypeError` when the stream has no callable `.read`, matching
  `MS3RecordValidator.from_filelike()`.
- The `MS3Record.flags`, `pubversion`, and `reclen` setters, and the `encoding`
  argument of `MS3TraceList.pack()`, `generate()`, and `to_file()`, raise
  `ValueError` for out-of-range values as `MS3Record.encoding` does, instead of
  the `OverflowError` raised where the value reaches the C struct.
- Arguments encoded as C strings raise `TypeError` when they are not a `str`,
  instead of an `AttributeError` from `.encode()`: the `MS3Record.sourceid`
  setter, `set_starttime_str()`, the extra header JSON Pointers,
  `MS3TraceList.get_traceid()`, the `sourceid` filtering argument,
  `configure_logging()` prefixes, `timestr2nstime()`, `sourceid2nslc()`, and
  `nslc2sourceid()`.
- `MS3TraceList.add_data()` names the deprecated `start_time` aliases it counts
  in the error for an ambiguous start time.
- Deprecation notices share one wording; `MS3RecordReader`'s `input` alias and
  the `pack()` methods no longer differ from the rest.

### Fixed
- `MS3Record.from_filelike()` and `MS3TraceList.add_filelike()` reject a
  `chunk_size` outside 1 byte to 1 GiB; a `chunk_size` of 0 read no records
  and reported success.

## [0.9.1] - 2026-07-25

### Added
- `MS3RecordValidator` now checks that each record does not contain future data
  based on the system clock.  The new `future_data_tolerance` option sets the
  allowed excess in seconds (default 5, `None` disables the check).
- `sourceid`, `starttime`, and `endtime` filtering kwargs on `MS3Record.from_file()`
  (i.e. `MS3RecordReader`), `MS3Record.from_buffer()`, `MS3Record.from_filelike()`,
  and therefore `MS3Record.iter_records()`.
- `MS3TraceSeg.update_time` and `update_time_seconds` report when a segment was last
  updated, the value `MS3TraceList.generate(flush_idle_seconds=N)` measures against.
  `None` for a segment carrying no update time.

### Changed
- libmseed updated to v3.5.1
- For `MS3TraceList.add_data()` deprecate the `start_time` kwargs in favor of
  `starttime` for uniformity with the rest of the package.
- `MS3TraceList.generate()`: document that abandoning the generator with
  `remove_packed=True` keeps the samples of the records already yielded, which a
  later call creates records for again.
- `MS3TraceList.generate()`: document that `flush_idle_seconds` is what drains a
  source that stops delivering data, and that the default of 0 holds its samples
  and trace ID for the life of the trace list.
- `util.sample_size()` raises `ValueError` for an unrecognized sample type instead
  of returning a size of 0.
- `MS3Record.from_buffer()` and `with_datasamples()`: document that only CPython refuses
  to resize a buffer while it is held, so on PyPy such a resize is undefined behavior
  that nothing reports.

### Fixed
- `MS3RecordPtr.record` for a record list read from a file or a file-like stream no
  longer serves raw bytes from a released buffer; `MS3Record.record` and `record_mv`
  raise instead.
- `MS3TraceList.add_buffer(record_list=True)` holds the source buffer, so the raw
  record and `unpack_recordlist()` no longer read freed memory after the caller
  releases it.
- `MS3Record.record` and `record_mv` return the record as parsed after `reclen` is set
  as a maximum for repacking, rather than reading past it.
- `get_error_messages()` returns messages oldest first, matching the order libmseed
  generated them, instead of reversed.
- `MS3RecordReader()` raises `TypeError` when passed both `source` and its deprecated
  `input` alias, instead of silently discarding `source`.
- `MS3TraceSeg.datasamples` and `np_datasamples` return views that hold the trace list,
  so it cannot be freed while a view of its samples exists.
- `MS3TraceList.generate()` rejects an unsupported `format_version` on the call, as
  documented, rather than on the first record.
- `MS3Record.with_datasamples()` copies a non-contiguous buffer instead of raising
  `BufferError` from the zero-copy check.
- `MS3TraceSeg.unpack_recordlist()` reports a read-only buffer as `BufferError` on PyPy
  as well as CPython, rather than as a `ValueError` about the buffer protocol.  Every
  entry point taking a buffer now raises `ValueError` for one that is not a buffer and
  `BufferError` for one that is not C-contiguous, instead of leaking CFFI's exception.
- `MS3Record.from_file()`, `from_buffer()`, and `from_filelike()` raise `MiniSEEDError`
  with a status of `MS_ENDOFFILE` when the source ends part way through a record, or
  with bytes remaining that are too few for one, instead of stopping silently.  For
  `from_file()`, `skip_not_data=True` accepts such a remnant as the end of the stream.
  A source holding too little data for any record reports `MS_NOTSEED` from all three.
- `MS3Record.from_buffer()` and `from_filelike()` size a trailing miniSEED v2 record
  that carries no Blockette 1000, as reading from a file does, rather than dropping it.
- `MS3Record.pack()` and `MS3TraceList.pack()` re-raise an exception from the record
  handler instead of reporting records that were never written.
- `MS3TraceList.add_file(record_list=True)` retains one file name buffer per distinct
  path, bounding the memory held when reading a file repeatedly.
- `MS3Record.pack()` and `MS3TraceList.pack()` release their C memory by reference
  counting rather than waiting for the cyclic garbage collector, and drop the
  handler and handler data (commonly an open file) on return.
- `MS3Record.generate()` and `MS3TraceList.generate()` now raise `MiniSEEDError` on
  a packing error instead of silently yielding no records.
- `MS3Record.parse_into()` no longer leaves a freed record pointer behind when
  parsing fails after the header stage (e.g. bad CRC).
- libmseed's message registry is now configured for each thread on first use, so
  warnings and errors are captured on threads that never called `configure_logging()`.
- `configure_logging()` no longer releases the message prefix that libmseed is
  still using, which produced scrambled prefixes on later messages.
- `MS3TraceSeg.recordlist` now keeps the owning `MS3TraceList` alive, so a record
  list or `MS3RecordPtr` obtained from a temporary trace list no longer reads
  freed memory.
- `MS3Record.parse()` and `parse_into()` now keep the source buffer alive, so the
  raw record and delayed `unpack_data()` no longer read freed memory when parsing
  from a temporary buffer.
- An `MS3Record` obtained from `MS3RecordReader`, `MS3Record.from_buffer()`,
  `from_filelike()`, or a record list now keeps its owner alive, so a record that
  outlives the reader or generator no longer reads freed memory.
- `MS3Record.encoding_str()` and `util.encoding_string()` return `"Unset"` for the
  unset encoding (-1) instead of raising, so `repr()` works on an unpopulated record.
- `MS3RecordValidator` now reports a detection failure as soon as it is conclusive
  instead of buffering the rest of the stream, bounding memory on non-miniSEED input.
- `MS3RecordValidator` now reports a record length beyond `MAXRECLEN`, e.g. from a
  corrupt payload length, instead of validating the file as clean.
- `MS3Record.with_datasamples()`, and therefore `generate()`, `to_file()` and
  `MS3TraceList.add_data()`, now reject a multi-dimensional `data_samples` buffer
  instead of silently keeping only the first dimension.
- `MS3Record.validate_extra_headers()` and `valid_extra_headers()` reuse the cached
  schema validator instead of re-reading and recompiling the bundled schema on every
  call. A failure to load the bundled schema now raises `ValueError` rather than the
  underlying `OSError`.
- `MS3Record.parse()` and `parse_into()` report a truncated record as the number of
  bytes still needed instead of `Unknown error code: 414`. `MiniSEEDError` no longer
  looks up an error string for a non-negative status code, which is not an error code.
- `MS3TraceSeg.unpack_recordlist()` raises `BufferError` for a read-only or
  non-contiguous destination buffer instead of letting libmseed write into it, which
  silently corrupted immutable objects such as `bytes`.
- `MS3RecordValidator` now reports an incomplete record at the end of the source, and
  a record length it cannot determine, instead of ending validation silently. A
  truncated file no longer validates with zero errors.
- `MS3TraceList.add_buffer()` and `MS3RecordValidator.from_buffer()` size the buffer in
  bytes rather than elements, so an `array.array` or numpy view with an item size
  greater than one byte is no longer read only in part.

## [0.9.0] - 2026-05-17

### Added
- `sourceid`, `starttime`, and `endtime` filtering kwargs on `MS3TraceList.__init__()`,
  `add_file()`, and `add_buffer()` (and therefore `from_file()` / `from_buffer()`).
  When provided, only records matching the source ID glob pattern and/or overlapping
  the time window are included in the trace list.
- `MS3TraceList.add_filelike()` and `MS3TraceList.from_filelike()` to read miniSEED
  from any object with a `.read(n)` method (e.g. `io.BytesIO`, network streams).
  When `record_list=True`, calling `unpack_recordlist()` is not supported because
  the source bytes do not persist. Slower than `add_file()` / `add_buffer()`;
  use as a last resort when those methods are not possible.

### Changed
- **Breaking:** pymseed exceptions now inherit from RuntimeError (via the new
  PymseedError base) instead of ValueError.
- Where string filenames are accepted also accept os.PathLike (e.g. pathlib.Path) and
  reject other non-str types with a clear TypeError.
- Unify the record-length parameter name across APIs for creating records to
  `max_record_length`; the old names (record_length, max_reclen) are deprecated
  and will be removed in a future release.
- Remove length limit for string values returned from `MS3Record.get_extra_header()`.
- Improve `MS3Record.extra` setter to allow removal of all headers.
- Fix `sample_size()` to take sample type codes instead of encoding codes.
- Fix return typing and improve behavior of `nstime2timestr()`.
- Fix `sourceid2nslc()` to be honest about return type and accurate docs.
- Raise on errors in `timestr2nstime()` instead of returning internal error values.
- Fix return type for `MS3Record.encoding_str()`, will never be None.
- Add ownership test to `MS3Record.parse_into()` to avoid clobbering foreign data.
- Harden `MS3Record.encoding` by checking for values 0..255.
- Harden `MS3Record.packet()` and `MS3Record.generate()` by raising an exception
  when only one of `data_samples` or `sample_type` is provided.
- Fix `MS3TraceList.sample_size_type()` crash on empty record list.
- Free packer when `MS3TraceList.generate()` when the consumer breaks, raises, or
  otherwise exits the generator before exhausting it.
- Simplify `MS3TraceList.unpack_recordlist()` so ffi.from_buffer() is called exactly once.
- Avoid an unbounded retention of per-file C string buffers in `MS3TraceList.add_file()`
  when `record_list=False`.
- Begin deprecation of the typo'd `MS3TraceList.generate(removed_packed=...)`
  parameter in favor of `remove_packed=...`.
- `MS3TraceList.add_data()` now raises `ValueError` if more than one of `start_time_str`,
  `start_time`, `start_time_seconds` is supplied.
- Fix `MS3TraceSeg.sampletype` returning the one-character string '\x00' instead of None.
- `MS3RecordReader`: clarify in docstring that callers retain ownership of file
  descriptors passed as int.  Callers must close the descriptor.
- `MS3RecordReader`: raise ValueError on read() / next() after close() instead of
  silently re-opening the file.
- `MS3RecordReader`: rename constructor parameter input -> source to match
  `MS3Record.iter_records(source, ...)` and stop shadowing the builtin; `input=` kept
  as a deprecated keyword alias with a DeprecationWarning.
- `MS3RecordReader`: initialism all instance attributes up front and perform
  pre-encoding to fail faster and cleanly on errors.
- `MS3RecordValidator`: extra-headers JSON schema loader is now cached at module level.
- `MS3RecordValidator`: emit the "extra headers validation skipped" warning at most
  once per `validate()` call
- Type annotations converted to PEP 604 / PEP 585 form.
- Apply consistent python formatting with ruff.

## [0.8.1] - 2026-04-22

### Added
- `MS3RecordValidator.from_filelike()` to read miniSEED from python file-like streams.
  The streams do not need to be seekable and are always read forward-only.

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
[0.9.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.9.1
[0.9.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.9.0
[0.8.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.8.1
[0.8.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.8.0
[0.7.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.7.0
[0.6.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.6.1
[0.6.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.6.0
[0.5.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.5.0
[0.4.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.4.0
[0.3.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.3.0
[0.2.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.2.0
[0.1.0]: https://github.com/EarthScope/pymseed/releases/tag/v0.1.0
[0.0.5]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.5
[0.0.4]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.4
[0.0.3]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.3
[0.0.2]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.2
[0.0.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.1
