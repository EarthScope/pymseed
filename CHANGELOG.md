# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- List-like access to trace IDs in MS3TraceList (indexing, slicing, iteration)
- List-like access to segments in MS3TraceID (indexing, slicing, iteration)
- Add MS3Record.from_file() and MS3Record.from_buffer() for convenience
- Add MS3TraceList.from_file() for consistency and future flexibility
- Add MS3TraceList.add_file() and MS3TraceList.add_files() for clarity

### Removed
- Remove MS3TraceList.numtraces in favor of supporting len() directly
- Remove MS3TraceList.traceids() in favor of supporting iteration directly
- Remove MS3TraceID.numsegments in favor of supporting len() directly
- Remove MS3TraceID.segments() in favor of supporting iteration directly
- Remove MS3TraceList.read_file() and MS3TraceList.read_files(), these have been renamed

## [0.0.1] - 2024-8-5
### Added
- Initial release
- MS3TraceList class for reading miniSEED files
- MS3Record class for individual records
- CFFI-based bindings to libmseed

[Unreleased]: https://github.com/EarthScope/pymseed/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/EarthScope/pymseed/releases/tag/v0.0.1