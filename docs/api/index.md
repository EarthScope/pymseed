# API Reference

This section documents the public API of pymseed. Every name listed here is
importable directly from the `pymseed` package.

## Classes

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.MS3Record
   pymseed.MS3RecordReader
   pymseed.MS3RecordValidator
   pymseed.ValidationError
   pymseed.MS3TraceList
```

### Trace list internals

Iterating a {class}`~pymseed.MS3TraceList` yields {class}`~pymseed.mstracelist.MS3TraceID`
objects, which in turn yield {class}`~pymseed.mstracelist.MS3TraceSeg` objects.
Reading with `record_list=True` additionally exposes
{class}`~pymseed.mstracelist.MS3RecordList` and
{class}`~pymseed.mstracelist.MS3RecordPtr`. These types are not imported at
the top level of the package, since they are only ever obtained from a trace
list rather than constructed directly.

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.mstracelist.MS3TraceID
   pymseed.mstracelist.MS3TraceSeg
   pymseed.mstracelist.MS3RecordList
   pymseed.mstracelist.MS3RecordPtr
```

## Enumerations

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.DataEncoding
   pymseed.TimeFormat
   pymseed.SubSecond
```

## Functions

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.nstime2timestr
   pymseed.timestr2nstime
   pymseed.sample_time
   pymseed.system_time
   pymseed.configure_logging
   pymseed.clear_error_messages
   pymseed.get_error_messages
```

### Source Identifiers

Converting between FDSN Source Identifiers and classic SEED NSLC (Network,
Station, Location, Channel) codes deserves special care -- the two are not a
symmetric mapping. See {doc}`../tutorial` for a worked explanation of the
conversion rules and the error cases.

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.nslc2sourceid
   pymseed.sourceid2nslc
```

## Exceptions

```{eval-rst}
.. autosummary::
   :toctree: generated/

   pymseed.PymseedError
   pymseed.MiniSEEDError
```

## Module Constants

```{eval-rst}
.. py:data:: pymseed.libmseed_version

   Version string of the underlying libmseed library.

.. py:data:: pymseed.NSTERROR

   Nanosecond time value indicating an error condition.

.. py:data:: pymseed.NSTUNSET

   Nanosecond time value indicating an unset/unknown time.

.. py:data:: pymseed.NSTMODULUS

   Modulus value for nanosecond time calculations (nanoseconds per second).
```
