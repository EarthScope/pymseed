# Examples

This section contains working examples demonstrating various pymseed use cases.
The example scripts are available in the
[examples directory](https://github.com/EarthScope/pymseed/tree/main/examples)
of the repository.

## Reading Examples

### read_traces.py

Read miniSEED files and display trace information including channel ID,
start/end times, and sample rate for each trace segment.

```{eval-rst}
.. literalinclude:: ../../examples/read_traces.py
   :language: python
   :caption: examples/read_traces.py
```

### read_numpy.py

Read miniSEED files and access data samples as NumPy arrays. Demonstrates
how to extract data without duplicating memory by reading files twice -
once to create the trace list and once to extract samples directly into
pre-allocated NumPy arrays.

```{eval-rst}
.. literalinclude:: ../../examples/read_numpy.py
   :language: python
   :caption: examples/read_numpy.py
```

### read_selected_unpack.py

Read miniSEED files without unpacking data samples, then selectively unpack
only the segments of interest into NumPy arrays. This limits the amount of
data held in memory at any one time when processing large volumes.

```{eval-rst}
.. literalinclude:: ../../examples/read_selected_unpack.py
   :language: python
   :caption: examples/read_selected_unpack.py
```

## Writing Examples

### generate_with_buffer.py

Illustrates how to write miniSEED using a rolling buffer of potentially
multi-channel data. This pattern is useful for:

* Generating miniSEED from continuous streams of unknown duration (real-time)
* Processing large volumes of data while avoiding memory issues

```{eval-rst}
.. literalinclude:: ../../examples/generate_with_buffer.py
   :language: python
   :caption: examples/generate_with_buffer.py
```

### continuous_miniseed_creation.py

Generate miniSEED from a continuous stream of data using MS3TraceList as a
transient buffer. Demonstrates using `MS3TraceList.generate()` to continuously
generate miniSEED output with full records whenever possible.

```{eval-rst}
.. literalinclude:: ../../examples/continuous_miniseed_creation.py
   :language: python
   :caption: examples/continuous_miniseed_creation.py
```

## Streaming Examples

### stream_stats.py

Read miniSEED from stdin, accumulate statistics (record count, sample count,
bytes, per-source timing), write to stdout, and print statistics to stderr
on completion.

```{eval-rst}
.. literalinclude:: ../../examples/stream_stats.py
   :language: python
   :caption: examples/stream_stats.py
```

### stream_timewindow.py

Read miniSEED from stdin, select records that fall within specified earliest
and latest times, trim records at window boundaries, and write to stdout.

```{eval-rst}
.. literalinclude:: ../../examples/stream_timewindow.py
   :language: python
   :caption: examples/stream_timewindow.py
```
