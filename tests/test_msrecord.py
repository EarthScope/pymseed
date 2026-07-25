import array
import gc
import json
import math
import os
import weakref

import pytest

from pymseed import DataEncoding, MiniSEEDError, MS3Record
from pymseed.clib import clibmseed, ffi
from tests.gc_helpers import collect_until, requires_buffer_export_lock, requires_refcounting

test_dir = os.path.abspath(os.path.dirname(__file__))
test_pack3 = os.path.join(test_dir, "data", "packtest_sine500.mseed3")
test_pack2 = os.path.join(test_dir, "data", "packtest_sine500.mseed2")
test_repack2_input = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")
test_repack3_output = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")

# A sine wave of 500 samples
sine_500 = [int(math.sin(math.radians(x)) * 500) for x in range(0, 500)]

# A global record buffer
record_buffer = b""


def _churn_heap():
    """Force reclamation and reuse of freed blocks so a stale pointer shows up."""
    gc.collect()
    return [bytearray(4096) for _ in range(3000)]


def record_handler(record, handler_data):
    """A callback function for MS3Record.set_record_handler()
    Stores the record in a global buffer for testing
    """
    print(f"Record handler called, record length: {len(record)}")
    global record_buffer
    record_buffer = bytes(record)


def test_msrecord_time_str_sentinels():
    """A fresh MS3Record has NSTUNSET timestamps; starttime_str/endtime_str
    must surface that as the ``"UNSET"`` sentinel rather than falling
    through to nstime2timestr (which would now raise). Forcing NSTERROR
    confirms the error sentinel branch."""
    from pymseed.clib import clibmseed

    msr = MS3Record()
    assert msr.starttime_str() == "UNSET"
    assert msr.endtime_str() == "UNSET"

    msr._msr.starttime = clibmseed.NSTERROR
    assert msr.starttime_str() == "ERROR"
    assert msr.endtime_str() == "ERROR"


def test_msrecord_unset_encoding_str():
    """A fresh MS3Record has encoding -1; encoding_str() must report that as
    "Unset" so repr() works on an unpopulated record."""
    msr = MS3Record()

    assert msr.encoding == -1
    assert msr.encoding_str() == "Unset"
    assert "encoding: -1 => Unset" in repr(msr)

    msr.encoding = DataEncoding.STEIM2
    assert msr.encoding_str() == "STEIM-2 integer compression"


def test_msrecord_setters():
    """Test the setters for an MS3Record object."""

    # Test populating an MS3Record object with setters
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    msr.reclen = 512
    msr.formatversion = 3
    msr.flags = 0x04  # Set the 4th bit (clock locked) to 1
    msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
    msr.samprate = 50.0
    msr.encoding = DataEncoding.STEIM2
    msr.pubversion = 1
    msr.extra = json.dumps({"FDSN": {"Time": {"Quality": 80}}})

    assert msr.reclen == 512
    assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
    assert msr.formatversion == 3
    assert msr.flags_dict() == {"clock_locked": True}
    assert msr.starttime == 1672621323123456789
    assert msr.starttime_seconds == 1672621323.1234567
    assert msr.samprate == 50.0
    assert msr.encoding == DataEncoding.STEIM2
    assert msr.pubversion == 1
    assert msr.extra == '{"FDSN":{"Time":{"Quality":80}}}'

    # Test nanosecond starttime setter
    msr.starttime = 1672621323999999999
    assert msr.starttime == 1672621323999999999

    # Test setter for starttime_seconds, rounding UP to microsecond precision
    msr.starttime_seconds = 1672621323.123456789
    assert msr.starttime == 1672621323123457000
    assert msr.starttime_seconds == 1672621323.123457

    # Test setter for starttime_seconds, rounding DOWN to microsecond precision
    msr.starttime_seconds = 1672621323.987654321
    assert msr.starttime == 1672621323987654000
    assert msr.starttime_seconds == 1672621323.987654


def test_msrecord_extra_header():
    """Test the extra header functions for an MS3Record object."""

    # Test populating an MS3Record object with extra headers
    msr = MS3Record()
    msr.extra = """{
                   "FDSN": {
                       "Time": {
                       "Quality": 100,
                       "Correction": 1.234
                       },
                       "Flags": {
                           "MassPositionOffscale": true
                       }
                   },
                   "Operator": {
                        "Battery": {
                            "Status": "CHARGING"
                        }
                   }
                }"""

    assert msr.get_extra_header("/FDSN/Time/Quality") == 100
    assert msr.get_extra_header("/FDSN/Time/Correction") == 1.234
    assert msr.get_extra_header("/Operator/Battery/Status") == "CHARGING"
    assert msr.get_extra_header("/FDSN/Flags/MassPositionOffscale") is True

    assert msr.get_extra_header("/Nonexistent/Header") is None

    # Malformed JSON Pointer
    with pytest.raises(ValueError):
        msr.get_extra_header("Invalid/JSON/Pointer")

    # Setting existing headers
    msr.set_extra_header("/FDSN/Time/Quality", 90)
    assert msr.get_extra_header("/FDSN/Time/Quality") == 90
    msr.set_extra_header("/FDSN/Flags/MassPositionOffscale", False)
    assert msr.get_extra_header("/FDSN/Flags/MassPositionOffscale") is False
    msr.set_extra_header("/FDSN/Time/Correction", 4.321)
    assert msr.get_extra_header("/FDSN/Time/Correction") == 4.321
    msr.set_extra_header("/Operator/Battery/Status", "DISCHARGING")
    assert msr.get_extra_header("/Operator/Battery/Status") == "DISCHARGING"

    # Setting a new header
    msr.set_extra_header("/New/Header/String", "Value")
    assert msr.get_extra_header("/New/Header/String") == "Value"
    msr.set_extra_header("/New/Header/Integer", 123)
    assert msr.get_extra_header("/New/Header/Integer") == 123
    msr.set_extra_header("/New/Header/Float", 1.234)
    assert msr.get_extra_header("/New/Header/Float") == 1.234
    msr.set_extra_header("/New/Header/Boolean", True)
    assert msr.get_extra_header("/New/Header/Boolean") is True

    # Malformed JSON Pointer
    with pytest.raises(ValueError):
        msr.set_extra_header("Invalid/JSON/Pointer", "Value")

    # Test merging, replacing the existing value
    msr.merge_extra_headers('{"FDSN": {"Time": {"Quality": 80}}}')
    assert msr.get_extra_header("/FDSN/Time/Quality") == 80

    # Test merging, remove the existing value
    msr.merge_extra_headers('{"FDSN": {"Time": {"Quality": null}}}')
    assert msr.get_extra_header("/FDSN/Time/Quality") is None

    # Test merging, add a new value
    msr.merge_extra_headers('{"New": {"Header2": "Value2"}}')
    assert msr.get_extra_header("/New/Header2") == "Value2"

    # Malformed JSON Merge Patch
    with pytest.raises(ValueError):
        msr.merge_extra_headers("Invalid/JSON/Merge/Patch")


def test_generate_rejects_partial_sample_args():
    """generate() must fail eagerly when only one of data_samples/sample_type is given."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__L_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 1

    # data_samples without sample_type
    with pytest.raises(ValueError, match="together"):
        msr.generate(data_samples=[1, 2, 3])

    # sample_type without data_samples
    with pytest.raises(ValueError, match="together"):
        msr.generate(sample_type="i")

    # Validation must be eager: the iterator returned by a valid call works,
    # but the invalid call raises before any iteration begins. Holding the
    # would-be generator without iterating still triggered the error above,
    # confirming the wrapper validates synchronously.


def test_generate_raises_on_pack_error():
    """A packing failure must raise, not end the generator silently.

    msr3_pack_next() returns 1 for a record, 0 when finished and a negative
    value on error; treating the error as completion would yield no records and
    let the caller write an empty file believing packing succeeded.
    """
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.STEIM2  # integer-only encoding

    # Float samples cannot be Steim2 encoded
    with pytest.raises(MiniSEEDError, match="Steim2"):
        list(msr.generate(data_samples=[1.5, 2.5, 3.5], sample_type="f"))


def test_generate_raises_on_pack_error_without_data_samples():
    """The no-data_samples branch of generate() checks the status too."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.STEIM2

    with msr.with_datasamples([1.5, 2.5, 3.5], "f"):
        with pytest.raises(MiniSEEDError, match="Steim2"):
            list(msr.generate())


def test_with_datasamples_rejects_multidimensional():
    """A multi-dimensional buffer is flattened when shared zero-copy while
    len() reports only the first dimension, so it must be rejected rather than
    silently packing a fraction of the samples."""
    np = pytest.importorskip("numpy")

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.INT32

    for data, sample_type in (
        (np.arange(12, dtype=np.int32).reshape(3, 4), "i"),
        (np.arange(12, dtype=np.float32).reshape(3, 4), "f"),
        (np.arange(12, dtype=np.float64).reshape(3, 4), "d"),
        (np.arange(12, dtype=np.uint8).reshape(3, 4), "t"),
        (np.int32(5), "i"),
    ):
        with pytest.raises(ValueError, match="one-dimensional"):
            with msr.with_datasamples(data, sample_type):
                pass


def test_with_datasamples_accepts_one_dimensional():
    """The dimension check must not reject the supported input types, including
    a non-contiguous array that takes the element-wise copy path."""
    np = pytest.importorskip("numpy")

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.INT32

    for data, sample_type, expected in (
        ([1, 2, 3, 4], "i", 4),
        (array.array("i", [1, 2, 3, 4]), "i", 4),
        (memoryview(array.array("i", [1, 2, 3])), "i", 3),
        (np.arange(4, dtype=np.int32), "i", 4),
        (np.arange(8, dtype=np.int32)[::2], "i", 4),
        (np.arange(4, dtype=np.float64), "d", 4),
        ("hello", "t", 5),
        (b"hello", "t", 5),
    ):
        with msr.with_datasamples(data, sample_type):
            assert msr.numsamples == expected


@requires_buffer_export_lock
def test_with_datasamples_holds_the_buffer_export():
    """A zero-copy source must stay pinned for the whole context, so resizing it
    inside the block raises rather than leaving msr->datasamples dangling.

    Only CPython refuses the resize; PyPy keeps no export count, so there a
    resize inside the block is undefined behavior that nothing reports."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.INT32

    for typecode, sample_type in (("i", "i"), ("f", "f"), ("d", "d")):
        data = array.array(typecode, [1, 2, 3, 4])
        with msr.with_datasamples(data, sample_type):
            with pytest.raises(BufferError, match="exporting buffers"):
                data.extend([0] * 1000)
        data.extend([0] * 1000)  # released again on exit
        assert len(data) == 1004

    # bytearray reached through a memoryview cast
    data = bytearray(array.array("i", [1, 2, 3, 4]).tobytes())
    with msr.with_datasamples(memoryview(data).cast("i"), "i"):
        with pytest.raises(BufferError, match="cannot be re-sized"):
            data.extend(b"\0" * 4096)


def test_with_datasamples_copies_non_contiguous_buffers():
    """A strided buffer cannot be shared as it is, so it must fall back to a copy."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0

    for typecode, sample_type, expected in (
        ("i", "i", [1, 3, 5]),
        ("f", "f", [1.0, 3.0, 5.0]),
        ("d", "d", [1.0, 3.0, 5.0]),
    ):
        strided = memoryview(array.array(typecode, [1, 2, 3, 4, 5, 6]))[::2]
        with msr.with_datasamples(strided, sample_type):
            assert msr.numsamples == 3
            assert list(msr.datasamples) == expected


def test_with_datasamples_packs_non_contiguous_as_the_same_samples():
    """The copy must carry the strided samples, not the bytes they are strided over."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.INT32
    msr.reclen = 512

    strided = memoryview(array.array("i", [10, 20, 30, 40, 50, 60]))[::2]

    assert list(msr.generate(data_samples=strided, sample_type="i")) == list(
        msr.generate(data_samples=[10, 30, 50], sample_type="i")
    )


def test_with_datasamples_holds_the_numpy_export():
    """The same pin must apply to numpy sources."""
    np = pytest.importorskip("numpy")

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 100.0
    msr.encoding = DataEncoding.INT32

    for dtype, sample_type in ((np.int32, "i"), (np.float32, "f"), (np.float64, "d")):
        data = np.arange(4, dtype=dtype)
        with msr.with_datasamples(data, sample_type):
            with pytest.raises(ValueError, match="cannot resize"):
                data.resize(1000, refcheck=True)


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_pack_rejects_partial_sample_args():
    """pack() must fail when only one of data_samples/sample_type is given."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__L_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 1

    def _noop(record, data):
        pass

    with pytest.raises(ValueError, match="together"):
        msr.pack(_noop, None, data_samples=[1, 2, 3])

    with pytest.raises(ValueError, match="together"):
        msr.pack(_noop, None, sample_type="i")


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_pack_reraises_handler_exception():
    """A failing handler must not be reported as a successful pack()."""
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__L_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 1
    msr.reclen = 128
    msr.encoding = DataEncoding.INT32

    calls = []

    def _failing_handler(record, data):
        calls.append(record)
        raise OSError("no space left on device")

    with pytest.raises(OSError, match="no space left on device"):
        msr.pack(_failing_handler, None, data_samples=list(range(200)), sample_type="i")

    # The records after the failure are not handed to the handler
    assert len(calls) == 1


def _pack_once(collected):
    """Pack a record whose handler closure carries a canary.

    MS3Record uses __slots__ and is not weakref-able, so the canary stands in
    for the record's own finalization.
    """

    class Canary:
        def __del__(self):
            collected.append(True)

    canary = Canary()
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__L_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 1
    msr.pack(
        lambda record, data, _canary=canary: None,
        None,
        data_samples=[1, 2, 3],
        sample_type="i",
    )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_pack_leaves_the_handler_collectable():
    """pack() must leave the handler collectable.

    A cycle through the callback cdata the collector cannot traverse would
    strand the record for the life of the process.
    """
    collected = []

    _pack_once(collected)

    assert collect_until(lambda: collected == [True]), "handler was never released"


@requires_refcounting
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_pack_leaves_no_reference_cycle():
    """pack() must not tie the record into a reference cycle.

    A cycle leaves msr3_free() waiting for the cyclic collector.
    """
    collected = []

    # Reference counting alone must release the handler, with the cyclic
    # collector off
    gc.collect()
    gc.disable()
    try:
        _pack_once(collected)
        assert collected == [True]
    finally:
        gc.enable()


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_pack_does_not_retain_handler_data():
    """pack() must not keep the handler or handler data alive after returning."""

    class HandlerData:
        pass

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__L_H_Z"
    msr.set_starttime_str("2024-01-01T00:00:00Z")
    msr.samprate = 1

    handler_data = HandlerData()
    reference = weakref.ref(handler_data)

    msr.pack(lambda record, data: None, handler_data, data_samples=[1, 2, 3], sample_type="i")

    del handler_data
    gc.collect()

    assert reference() is None


def test_msrecord_encoding_setter():
    """Encoding setter validates against the 0..255 miniSEED on-wire range."""
    msr = MS3Record()

    # Fresh record reports -1 (libmseed's "not set" sentinel) for the getter.
    assert msr.encoding == -1

    # Real encoding values should pass through unchanged.
    msr.encoding = DataEncoding.STEIM2
    assert msr.encoding == DataEncoding.STEIM2

    # Range boundaries should both be accepted.
    msr.encoding = 0
    assert msr.encoding == 0
    msr.encoding = 255
    assert msr.encoding == 255

    # Out-of-range values must raise a clear ValueError, not OverflowError.
    with pytest.raises(ValueError, match="0..255"):
        msr.encoding = 256
    with pytest.raises(ValueError, match="0..255"):
        msr.encoding = -1  # getter sentinel, but not a valid assignment
    with pytest.raises(ValueError, match="0..255"):
        msr.encoding = 1_000_000

    # State after a failed assignment is unchanged.
    msr.encoding = DataEncoding.FLOAT32
    with pytest.raises(ValueError):
        msr.encoding = 999
    assert msr.encoding == DataEncoding.FLOAT32


def test_msrecord_sourceid_setter():
    """Setter accepts boundary lengths, overwrites cleanly, and rejects oversize."""

    msr = MS3Record()
    max_bytes = clibmseed.LM_SIDLEN - 1  # NUL terminator reserved

    # Exact-fit length (LM_SIDLEN - 1 bytes) round-trips intact.
    exact = "A" * max_bytes
    msr.sourceid = exact
    assert msr.sourceid == exact

    # Overwriting with a shorter value must not leak bytes from the longer one.
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    assert msr.sourceid == "FDSN:XX_TEST__B_S_X"

    # One byte over the limit must raise, and the field stays unchanged.
    too_long = "B" * (max_bytes + 1)
    with pytest.raises(ValueError):
        msr.sourceid = too_long
    assert msr.sourceid == "FDSN:XX_TEST__B_S_X"

    # The bound is on encoded bytes, not characters: a single 2-byte UTF-8
    # character at the byte boundary must still be rejected.
    just_over_in_bytes = "x" * (max_bytes - 1) + "\u00e9"  # 'é' encodes to 2 bytes
    assert len(just_over_in_bytes) == max_bytes
    assert len(just_over_in_bytes.encode("utf-8")) == max_bytes + 1
    with pytest.raises(ValueError):
        msr.sourceid = just_over_in_bytes


def test_msrecord_extra_header_long_string():
    """Long string extra header values are returned intact, not flagged as truncated."""

    msr = MS3Record()

    # Test a long string value
    long_value = "x" * 4095
    msr.extra = json.dumps({"Long": {"String": long_value}})
    assert msr.get_extra_header("/Long/String") == long_value

    # Test a longer string value
    longer_value = "y" * 10_000
    msr.extra = json.dumps({"Long": {"String": longer_value}})
    assert msr.get_extra_header("/Long/String") == longer_value


def test_msrecord_extra_clear():
    """Test clearing extra headers by assigning a falsy value."""

    msr = MS3Record()

    # No extras initially
    assert msr.extra == ""
    assert msr.extralength == 0

    payload = '{"FDSN":{"Time":{"Quality":80}}}'
    msr.extra = payload
    assert msr.extra == payload
    assert msr.extralength == len(payload)

    # Clearing via empty string should remove all extras
    msr.extra = ""
    assert msr.extra == ""
    assert msr.extralength == 0
    assert msr.get_extra_header("/FDSN/Time/Quality") is None

    # Setting again after clearing should work
    msr.extra = payload
    assert msr.extra == payload
    assert msr.extralength == len(payload)

    # Invalid JSON should raise and not corrupt existing state
    with pytest.raises(ValueError):
        msr.extra = "{not valid json"
    assert msr.extra == payload
    assert msr.extralength == len(payload)


class TestMS3RecordSorting:
    """Test sorting of MS3Record objects."""

    def test_same_time_different_subsource(self):
        msr1 = MS3Record()
        msr1.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr1.sourceid = "FDSN:XX_TEST__B_S_X"

        msr2 = MS3Record()
        msr2.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr2.sourceid = "FDSN:XX_TEST__B_S_Y"

        assert msr1 < msr2, "Less than: Same time but different sourceid (subsource)"
        assert msr1 <= msr2, "Less than: Same time but different sourceid (subsource)"
        assert msr2 > msr1, "Less than: Same time but different sourceid (subsource)"
        assert msr2 >= msr1, "Less than: Same time but different sourceid (subsource)"

    def test_different_time_same_sourceid(self):
        msr1 = MS3Record()
        msr1.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr1.sourceid = "FDSN:XX_TEST__B_S_X"

        msr2 = MS3Record()
        msr2.set_starttime_str("2023-01-02T01:02:04.123456789Z")  # 1 second later
        msr2.sourceid = "FDSN:XX_TEST__B_S_X"

        assert msr1 < msr2, "Less than: Different time but same sourceid"
        assert msr1 <= msr2, "Less than equal: Different time but same sourceid"
        assert msr2 > msr1, "Less than: Different time but same sourceid"
        assert msr2 >= msr1, "Less than: Different time but same sourceid"

    def test_empty_location_last(self):
        msr1 = MS3Record()
        msr1.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr1.sourceid = "FDSN:XX_TEST_00_B_S_X"

        msr2 = MS3Record()
        msr2.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr2.sourceid = "FDSN:XX_TEST_ZZ_B_S_Y"

        msr3 = MS3Record()
        msr3.set_starttime_str("2023-01-02T01:02:03.123456789Z")
        msr3.sourceid = "FDSN:XX_TEST__B_S_Y"

        assert msr1 < msr2 < msr3, "Less than: Same time but different sourceid (location)"
        assert msr1 <= msr2 <= msr3, "Less than equal: Same time but different sourceid (location)"
        assert msr3 > msr2 > msr1, "Greater than: Same time but different sourceid (location)"
        assert msr3 >= msr2 >= msr1, (
            "Greater than equal: Same time but different sourceid (location)"
        )


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_msrecord_pack():

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    msr.reclen = 512
    msr.formatversion = 3
    msr.flags = 0x04  # Set the 4th bit (clock locked) to 1
    msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
    msr.samprate = 50.0
    msr.encoding = DataEncoding.STEIM2
    msr.pubversion = 1
    msr.extra = json.dumps({"FDSN": {"Time": {"Quality": 80}}})

    # Test packing of an miniSEED v3 record
    (packed_samples, packed_records) = msr.pack(
        record_handler, data_samples=sine_500, sample_type="i"
    )

    assert packed_samples == 500
    assert packed_records == 1
    assert len(record_buffer) == 475

    with open(test_pack3, "rb") as f:
        record_v3 = f.read()
        assert record_buffer == record_v3

    # Test packing of an miniSEED v2 record
    msr.formatversion = 2

    (packed_samples, packed_records) = msr.pack(
        record_handler, data_samples=sine_500, sample_type="i"
    )

    assert packed_samples == 500
    assert packed_records == 1
    assert len(record_buffer) == 512

    with open(test_pack2, "rb") as f:
        record_v2 = f.read()
        assert record_buffer == record_v2


def test_msrecord_generate():
    """Test creating miniSEED with MS3Record.generate() method."""

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    msr.reclen = 512
    msr.formatversion = 3
    msr.flags = 0x04  # Set the 4th bit (clock locked) to 1
    msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
    msr.samprate = 50.0
    msr.encoding = DataEncoding.STEIM2
    msr.pubversion = 1
    msr.extra = json.dumps({"FDSN": {"Time": {"Quality": 80}}})

    # Test creation of a miniSEED v3 record
    record_buffer = b""
    for record in msr.generate(data_samples=sine_500, sample_type="i"):
        record_buffer += record

    assert len(record_buffer) == 475

    with open(test_pack3, "rb") as f:
        record_v3 = f.read()
        assert record_buffer == record_v3

    # Test packing of an miniSEED v2 record
    msr.formatversion = 2

    record_buffer = b""
    for record in msr.generate(data_samples=sine_500, sample_type="i"):
        record_buffer += record

    assert len(record_buffer) == 512

    with open(test_pack2, "rb") as f:
        record_v2 = f.read()
        assert record_buffer == record_v2


def test_msrecord_regenerate():
    """Repack miniSEED v2 to v3 and compare to reference file."""

    record_buffer = b""

    with MS3Record.from_file(test_repack2_input, unpack_data=True) as msreader:
        for msr in msreader:
            # Set to format version 3
            msr.formatversion = 3

            # Set record length to 1024 to allow each 512-byte input record to
            # be regenerated in a single record that may be larger than the
            # input record length.
            msr.reclen = 1024

            # Regenerate the record
            for record in msr.generate():
                record_buffer += record

    assert len(record_buffer) == 617142

    with open(test_repack3_output, "rb") as f:
        record_v3 = f.read()
        assert record_buffer == record_v3


def test_msrecord_to_file(tmp_path):
    """Test MS3Record.to_file() method using pytest's tmp_path fixture."""
    # Create a new MS3Record object
    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    msr.reclen = 512
    msr.formatversion = 3
    msr.flags = 0x04  # Set the 4th bit (clock locked) to 1
    msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
    msr.samprate = 50.0
    msr.encoding = DataEncoding.STEIM2
    msr.pubversion = 1
    msr.extra = json.dumps({"FDSN": {"Time": {"Quality": 80}}})

    # Use pytest's tmp_path fixture to create a temporary file
    temp_file = tmp_path / "test_output.mseed"

    with msr.with_datasamples(sine_500, "i"):
        # Write using to_file method
        records_written = msr.to_file(str(temp_file), overwrite=True)

    # Verify number of records written
    assert records_written == 1

    # Verify file was created and has content
    assert temp_file.exists()
    assert temp_file.stat().st_size > 0

    # Compare created file to reference file
    with open(test_pack3, "rb") as f:
        reference_data = f.read()
        with open(temp_file, "rb") as f:
            test_data = f.read()
            assert reference_data == test_data


def test_msrecord_to_file_rejects_invalid_filename_types(tmp_path):
    msr = MS3Record()
    for bad in (b"some/path", None, ["a", "b"], 3.14):
        with pytest.raises(TypeError, match="filename must be"):
            msr.to_file(bad)


def test_msrecord_to_file_accepts_pathlike(tmp_path):
    import pathlib

    msr = MS3Record()
    msr.sourceid = "FDSN:XX_TEST__B_S_X"
    msr.set_starttime_str("2023-01-02T01:02:03.123456789Z")
    msr.samprate = 50.0
    msr.encoding = DataEncoding.STEIM2

    out = pathlib.Path(tmp_path) / "out.mseed3"
    with msr.with_datasamples(sine_500, "i"):
        records_written = msr.to_file(out, overwrite=True)

    assert records_written == 1
    assert out.exists() and out.stat().st_size > 0


class TestMS3RecordParse:
    """Tests for MS3Record.parse() — single-record buffer parsing."""

    def test_parse_truncated_reports_bytes_needed(self):
        """A truncated buffer makes msr3_parse() return a positive byte-count
        hint rather than an error code; it must not be reported as an error
        code ("Unknown error code: 414")."""
        with open(test_pack3, "rb") as f:
            record = f.read(512)

        for call in (
            lambda buf: MS3Record.parse(buf),
            lambda buf: MS3Record().parse_into(buf),
        ):
            with pytest.raises(MiniSEEDError) as excinfo:
                call(record[:64])

            assert excinfo.value.status_code > 0
            assert "Unknown error code" not in str(excinfo.value)
            assert str(excinfo.value) == (
                f"Incomplete miniSEED record, {excinfo.value.status_code} more bytes needed"
            )

    def test_parse_v3_metadata(self):
        """Parse a v3 record and verify all header fields."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf)

        assert msr.formatversion == 3
        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.samprate == 50.0
        assert msr.encoding == DataEncoding.STEIM2
        assert msr.pubversion == 1
        assert msr.samplecnt == 500
        assert msr.reclen == 475

    def test_parse_record_mv_is_memoryview(self):
        """record_mv returns a real memoryview that mirrors record without copying."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf)
        mv = msr.record_mv

        assert isinstance(mv, memoryview)
        assert len(mv) == msr.reclen
        assert mv.tobytes() == msr.record
        # memoryview-only API surface (not present on _cffi_backend.buffer)
        assert mv.format == "B"
        assert mv.itemsize == 1
        assert mv.shape == (msr.reclen,)

    def test_record_keeps_parsed_length_after_reclen_change(self):
        """reclen doubles as the maximum length for packing, so setting it must
        not resize the raw record, which is read from the source buffer."""
        with open(test_pack3, "rb") as f:
            buf = f.read()  # exactly one 475 byte record

        msr = MS3Record.parse(buf, unpack_data=True)
        msr.reclen = 4096

        assert msr.reclen == 4096
        assert msr.record == buf
        assert len(msr.record_mv) == len(buf)

        # The new length is still honored as the maximum for repacking
        records = list(msr.generate())
        assert len(records) == 1
        assert len(records[0]) <= 4096

    def test_parse_into_repins_record_length(self):
        """parse_into() sizes the raw record by the newly parsed length, not by
        a reclen left over from the previous record."""
        with open(test_pack3, "rb") as f:
            first = f.read()
        with open(test_repack3_output, "rb") as f:
            buf = f.read()

        second = buf[: MS3Record.parse(buf).reclen]
        assert len(second) != len(first)

        # parse() owns its struct, which parse_into() requires
        msr = MS3Record.parse(first)
        msr.reclen = 4096
        msr.parse_into(second)

        assert msr.record == second

    def test_parse_into_reuses_owning_wrapper(self):
        """parse_into() works on a default-constructed (owning) wrapper."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record()
        original_ptr = msr._msr
        result = msr.parse_into(buf)

        assert result is msr
        # libmseed reuses the existing struct in place; pointer is stable.
        assert msr._msr == original_ptr
        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.samplecnt == 500
        assert msr._msr_allocated is True

        # Second call on the same wrapper must also work (the reuse-in-loop case).
        msr.parse_into(buf)
        assert msr.samplecnt == 500

    def test_parse_into_rejects_borrowed_wrapper(self):
        """parse_into() on a non-owning view must raise instead of corrupting
        the foreign owner's struct."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        # Establish a real owner and a borrowed view into the same C struct.
        owner = MS3Record.parse(buf)
        view = MS3Record(recordptr=owner._msr)  # owns=False by default
        assert view._msr_allocated is False

        with pytest.raises(ValueError, match="own"):
            view.parse_into(buf)

        # Owner's state must be untouched by the failed call.
        assert owner.sourceid == "FDSN:XX_TEST__B_S_X"
        assert owner.samplecnt == 500

    def test_parse_owns_record(self):
        """parse() returns an owning record; non-recordptr construction also owns."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        # parse() must produce a wrapper that owns and will free its C struct.
        parsed = MS3Record.parse(buf)
        assert parsed._msr_allocated is True

        # Default constructor allocates and owns.
        fresh = MS3Record()
        assert fresh._msr_allocated is True

        # Wrapping an existing pointer without owns=True is a non-owning view
        # (the default for callers like from_buffer / from_filelike / readers).
        view = MS3Record(recordptr=parsed._msr)
        assert view._msr_allocated is False

        # Explicit owns=True flag flips ownership for the recordptr case.
        # NOTE: we don't actually free here — just assert the flag plumbs through.
        flagged = MS3Record(recordptr=parsed._msr, owns=True)
        assert flagged._msr_allocated is True
        # Defuse the duplicate-free that would otherwise happen at GC: parsed
        # holds the real ownership; clear the test wrappers' flags before exit.
        view._msr_allocated = False
        flagged._msr_allocated = False

    def test_parse_v2_metadata(self):
        """Parse a v2 record and verify all header fields."""
        with open(test_pack2, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf)

        assert msr.formatversion == 2
        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.samprate == 50.0
        assert msr.encoding == DataEncoding.STEIM2
        assert msr.samplecnt == 500
        assert msr.reclen == 512

    def test_parse_header_only(self):
        """Parse without unpacking data — header fields accessible, samples not."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf, unpack_data=False)

        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.samplecnt == 500
        assert msr.numsamples == 0

    def test_parse_with_data(self):
        """Parse with unpack_data=True — samples decoded and accessible."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf, unpack_data=True)

        assert msr.numsamples == 500
        assert msr.sampletype == "i"
        samples = list(msr.datasamples)
        assert samples == sine_500

    def test_parse_ownership(self):
        """Returned MS3Record owns its C struct — valid after parse() returns."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        msr = MS3Record.parse(buf, unpack_data=True)
        buf = None  # drop the input buffer; msr must remain valid

        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.numsamples == 500
        assert msr.samplecnt == 500
        assert list(msr.datasamples) == sine_500

    def test_parse_v2_to_v3_repack(self):
        """Parse v2 with data, switch formatversion, generate() produces a valid v3 record."""
        with open(test_pack2, "rb") as f:
            v2_buf = f.read()

        msr = MS3Record.parse(v2_buf, unpack_data=True)
        assert msr.formatversion == 2

        original_samples = list(msr.datasamples)
        original_sourceid = msr.sourceid
        original_samprate = msr.samprate
        original_samplecnt = msr.samplecnt

        msr.formatversion = 3

        output = b"".join(msr.generate())

        # Re-parse the output to verify it is a valid v3 record with the same data
        reparsed = MS3Record.parse(output, unpack_data=True)
        assert reparsed.formatversion == 3
        assert reparsed.sourceid == original_sourceid
        assert reparsed.samprate == original_samprate
        assert reparsed.samplecnt == original_samplecnt
        assert list(reparsed.datasamples) == original_samples

    @staticmethod
    def _bad_crc_record() -> bytes:
        """A v3 record whose payload has been altered so its CRC no longer matches."""
        with open(test_repack3_output, "rb") as f:
            buf = f.read()

        reclen = MS3Record.parse(buf).reclen
        corrupted = bytearray(buf[:reclen])
        corrupted[100] ^= 0xFF
        return bytes(corrupted)

    def test_parse_into_error_leaves_record_usable(self):
        """A post-header parse failure must not leave a freed record behind.

        msr3_parse() frees the supplied record and NULLs the pointer when it
        fails after the header stage (e.g. bad CRC).  Keeping the stale pointer
        meant subsequent property access read freed memory and finalization
        freed it a second time.
        """
        msr = MS3Record()

        with pytest.raises(MiniSEEDError, match="CRC"):
            msr.parse_into(self._bad_crc_record())

        # The wrapper still owns a valid, freshly initialized record
        assert msr._msr_allocated is True
        assert msr._msr != ffi.NULL
        assert msr.reclen == -1
        assert msr.sourceid == ""

        # ... and remains usable for a subsequent parse
        with open(test_pack3, "rb") as f:
            msr.parse_into(f.read())
        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"
        assert msr.samplecnt == 500

    def test_parse_into_error_survives_finalization(self):
        """Repeated failed parses then teardown must not double free."""
        bad = self._bad_crc_record()

        msr = MS3Record()
        for _ in range(50):
            with pytest.raises(MiniSEEDError):
                msr.parse_into(bad)

        # Dropping the wrapper runs __del__ -> msr3_free(); a stale pointer here
        # aborted the process under the malloc guard.
        del msr
        gc.collect()

    def test_parse_into_detection_error_keeps_record(self):
        """A detection-stage failure does not free the record; it stays usable."""
        msr = MS3Record()

        with pytest.raises(MiniSEEDError):
            msr.parse_into(b"\x00" * 512)

        assert msr._msr != ffi.NULL
        with open(test_pack3, "rb") as f:
            msr.parse_into(f.read())
        assert msr.sourceid == "FDSN:XX_TEST__B_S_X"

    def test_parse_survives_temporary_source_buffer(self):
        """parse() must keep the buffer it parsed from alive.

        msr->record points into the supplied buffer rather than a copy, so a
        record parsed from a temporary read the buffer's freed memory: raw record
        bytes came back as zeros and delayed unpacking produced wrong samples.
        """
        with open(test_pack3, "rb") as f:
            held = f.read()
        # Bind the record: datasamples is a view into memory it owns.
        reference = MS3Record.parse(held, unpack_data=True)
        expected = list(reference.datasamples)

        def parse_from_temporary():
            with open(test_pack3, "rb") as f:
                return MS3Record.parse(f.read())

        msr = parse_from_temporary()
        _churn_heap()

        assert msr.record[:2] == b"MS"
        assert msr.unpack_data() == msr.samplecnt
        assert list(msr.datasamples) == expected

    def test_parse_into_survives_temporary_source_buffer(self):
        """parse_into() must keep the buffer it parsed from alive."""
        with open(test_pack3, "rb") as f:
            held = f.read()
        # Bind the record: datasamples is a view into memory it owns.
        reference = MS3Record.parse(held, unpack_data=True)
        expected = list(reference.datasamples)

        msr = MS3Record()

        def parse_into_from_temporary():
            with open(test_pack3, "rb") as f:
                msr.parse_into(f.read())

        parse_into_from_temporary()
        _churn_heap()

        assert msr.record[:2] == b"MS"
        assert msr.unpack_data() == msr.samplecnt
        assert list(msr.datasamples) == expected

    def test_parse_error_truncated_buffer(self):
        """Raise MiniSEEDError when buffer is too small to contain a record."""
        with open(test_pack3, "rb") as f:
            buf = f.read()

        with pytest.raises(MiniSEEDError):
            MS3Record.parse(buf[:10])

    def test_parse_error_empty_buffer(self):
        """Raise MiniSEEDError on empty buffer."""
        with pytest.raises(MiniSEEDError):
            MS3Record.parse(b"")


class TestHeaderOnlyMS3Record:
    """Tests for reading and writing header-only miniSEED records (numsamples == 0)."""

    # Headers to use for header-only records
    headers = {"FDSN": {"Time": {"Quality": 99}}}

    def _make_header_only_msr(self, formatversion: int = 3) -> MS3Record:
        """Create an MS3Record with no samples for use as a header-only record."""
        msr = MS3Record()
        msr.sourceid = "FDSN:XX_TEST__B_S_X"
        msr.formatversion = formatversion
        msr.set_starttime_str("2024-01-01T00:00:00Z")
        msr.samprate = 0
        msr.pubversion = 1
        # numsamples is 0 by default — this is the header-only condition

        msr.extra = json.dumps(self.headers)

        return msr

    def test_generate_v3_roundtrip(self):
        """generate() on a header-only v3 record yields exactly one record with the correct headers."""
        msr = self._make_header_only_msr(formatversion=3)
        records = list(msr.generate())

        assert len(records) == 1
        assert isinstance(records[0], bytes)
        assert len(records[0]) > 0

        # Parse the record and verify the extra headers
        parsed = MS3Record.parse(records[0])
        assert parsed.formatversion == 3
        assert parsed.sourceid == "FDSN:XX_TEST__B_S_X"
        assert parsed.samprate == 0
        assert parsed.samplecnt == 0
        assert parsed.numsamples == 0
        assert parsed.pubversion == 1
        assert json.loads(parsed.extra) == self.headers

    def test_generate_v2_roundtrip(self):
        """generate() on a header-only v2 record yields exactly one record with the correct headers."""
        msr = self._make_header_only_msr(formatversion=2)
        msr.reclen = 256
        records = list(msr.generate())

        assert len(records) == 1
        assert isinstance(records[0], bytes)
        assert len(records[0]) > 0

        # Parse the record and verify the extra headers
        parsed = MS3Record.parse(records[0])
        assert parsed.formatversion == 2
        assert parsed.sourceid == "FDSN:XX_TEST__B_S_X"
        assert parsed.samprate == 0
        assert parsed.samplecnt == 0
        assert parsed.numsamples == 0
        assert parsed.pubversion == 1
        assert json.loads(parsed.extra) == self.headers


class TestValidateExtraHeaders:
    """MS3Record.validate_extra_headers() shares the process-wide schema cache
    with MS3RecordValidator instead of re-reading, re-parsing and re-compiling
    the bundled schema on every call."""

    VALID = '{"FDSN":{"Time":{"Quality":100,"Correction":1.234}}}'
    INVALID = '{"FDSN":{"Time":{"Quality":"really good"}}}'

    @pytest.fixture(autouse=True)
    def _clear_schema_cache(self):
        from pymseed import _extra_headers_jsonschema as ehjs

        ehjs.load_extra_headers_validator.cache_clear()
        yield
        ehjs.load_extra_headers_validator.cache_clear()

    def test_repeated_calls_reuse_the_cached_validator(self):
        """The bundled schema must be loaded once, not once per call."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import _extra_headers_jsonschema as ehjs

        msr = MS3Record()
        msr.extra = self.VALID

        for _ in range(5):
            assert msr.validate_extra_headers() == []

        info = ehjs.load_extra_headers_validator.cache_info()
        assert info.misses == 1
        assert info.hits == 4

    def test_validation_results_unchanged(self):
        """Caching the validator must not change what it reports."""
        pytest.importorskip("jsonschema_rs")

        msr = MS3Record()

        msr.extra = self.VALID
        assert msr.validate_extra_headers() == []
        assert msr.valid_extra_headers() is True

        msr.extra = self.INVALID
        assert len(msr.validate_extra_headers()) == 1
        assert msr.valid_extra_headers() is False

    def test_no_extra_headers_returns_empty_without_loading(self):
        """An empty extra-header string short-circuits before the schema load."""
        from pymseed import _extra_headers_jsonschema as ehjs

        assert MS3Record().validate_extra_headers() == []
        assert ehjs.load_extra_headers_validator.cache_info().misses == 0

    def test_unknown_schema_id_rejected(self):
        """The unknown-schema_id error must not be flattened into the loader's
        generic 'failed to load' message."""
        msr = MS3Record()
        msr.extra = self.VALID

        with pytest.raises(ValueError, match="Unknown schema_id: bogus"):
            msr.validate_extra_headers(schema_id="bogus")

    def test_schema_file_takes_precedence_and_is_not_cached(self, tmp_path):
        """An explicit schema_file bypasses the cache, which is keyed on the
        bundled schema_id only."""
        pytest.importorskip("jsonschema_rs")
        from pymseed import _extra_headers_jsonschema as ehjs

        # A schema that rejects everything, unlike the bundled FDSN schema.
        schema = tmp_path / "reject-all.json"
        schema.write_text(
            json.dumps({"$schema": "https://json-schema.org/draft/2020-12/schema", "not": {}})
        )

        msr = MS3Record()
        msr.extra = self.VALID

        assert len(msr.validate_extra_headers(schema_file=str(schema))) == 1
        assert ehjs.load_extra_headers_validator.cache_info().misses == 0

    def test_load_failure_raises(self, monkeypatch):
        """Unlike MS3RecordValidator, which downgrades a load failure to a
        per-record warning, a direct call must not silently report 'valid'."""
        from pymseed import _extra_headers_jsonschema as ehjs

        class _FakeJoin:
            def joinpath(self, *_args):
                return self

            def read_bytes(self):
                raise FileNotFoundError("simulated missing schema file")

        monkeypatch.setattr(ehjs, "files", lambda _pkg: _FakeJoin())

        msr = MS3Record()
        msr.extra = self.VALID

        with pytest.raises(ValueError, match="bundled schema file unavailable"):
            msr.validate_extra_headers()

    def test_missing_jsonschema_reported_as_importerror(self, monkeypatch):
        """The optional dependency stays an ImportError rather than becoming a
        generic load failure once routed through the loader."""
        from pymseed import _extra_headers_jsonschema as ehjs

        def no_jsonschema(_schema):
            raise ImportError(ehjs._IMPORT_ERROR_MESSAGE)

        monkeypatch.setattr(ehjs, "validator_for_extra_headers_schema", no_jsonschema)

        msr = MS3Record()
        msr.extra = self.VALID

        with pytest.raises(ImportError, match="jsonschema-rs is not installed"):
            msr.validate_extra_headers()
