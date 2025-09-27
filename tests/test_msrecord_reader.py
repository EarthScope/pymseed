import pytest
import sys
import os
from pymseed import MS3Record, DataEncoding, TimeFormat, SubSecond, NSTMODULUS
from pymseed.exceptions import MiniSEEDError

test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")
test_path2 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed2")
test_60sec = os.path.join(test_dir, "data", "testdata-60sec-period.mseed3")


def test_msrecord_read_record_details():
    with MS3Record.from_file(test_path3, unpack_data=True) as msreader:

        # Read first record
        msr = msreader.read()

        assert msr.reclen == 542
        assert msr.swapflag == 2
        assert msr.swapflag_dict() == {"header_swapped": False, "payload_swapped": True}
        assert msr.sourceid == "FDSN:IU_COLA_00_B_H_1"
        assert msr.formatversion == 3
        assert msr.flags == 4
        assert msr.flags_dict() == {"clock_locked": True}
        assert msr.starttime == 1267253400019539000
        assert msr.starttime_seconds == pytest.approx(1267253400.019539)
        assert (
            msr.starttime_str(timeformat=TimeFormat.ISOMONTHDAY_Z)
            == "2010-02-27T06:50:00.019539Z"
        )
        assert (
            msr.starttime_str(
                timeformat=TimeFormat.SEEDORDINAL, subsecond=SubSecond.NONE
            )
            == "2010,058,06:50:00"
        )
        assert msr.samprate == 20.0
        assert msr.samprate_raw == 20.0
        assert msr.samprate_period_ns == 0.05 * NSTMODULUS
        assert msr.samprate_period_seconds == pytest.approx(0.05)
        assert msr.encoding == DataEncoding.STEIM2
        assert msr.encoding_str() == "STEIM-2 integer compression"
        assert msr.pubversion == 4
        assert msr.samplecnt == 296
        assert msr.crc == 1977151071
        assert msr.extralength == 33
        assert msr.datalength == 448
        assert msr.extra == '{"FDSN":{"Time":{"Quality":100}}}'
        assert msr.numsamples == 296
        assert msr.sampletype == "i"
        assert msr.endtime == 1267253414769539000
        assert msr.endtime_seconds == pytest.approx(1267253414.769539)

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [-508722, -508764, -508809, -508866, -508927, -508986]

def test_msrecord_read_unpack_data():
    with MS3Record.from_file(test_path3, unpack_data=False) as msreader:

        # Read first record
        msr = msreader.read()

        assert msr.samplecnt == 296
        assert msr.numsamples == 0
        assert not msr.datasamples
        assert msr.sampletype is None

        # Unpack data
        unpacked = msr.unpack_data()
        assert unpacked == 296

        assert msr.numsamples == 296
        assert msr.datasamples
        assert msr.sampletype == "i"

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [-508722, -508764, -508809, -508866, -508927, -508986]

def test_msrecord_read_record_60sec():
    with MS3Record.from_file(test_60sec, unpack_data=True) as msreader:

        # Read first record
        msr = msreader.read()

        assert msr.reclen == 4090
        assert msr.sourceid == "FDSN:XX_SIN__W_X_Y"
        assert msr.samprate == pytest.approx(0.01666667)
        assert msr.samprate_raw == -60.0
        assert msr.samprate_period_ns == 60 * NSTMODULUS
        assert msr.samprate_period_seconds == pytest.approx(60.0)

def test_msrecord_read_record_details_fd():
    # Test reading from a file descriptor - we simulate this using the buffer reader

    # File descriptor support is not implemented on Windows
    if sys.platform.lower().startswith("win"):
        return

    # Using a file for testing, but this could be stdin or any other input stream
    file_descriptor = None
    with open(test_path2, "rb", buffering=0) as fp:
        original_file_descriptor = fp.fileno()
        file_descriptor = os.dup(original_file_descriptor)

    # Provide the reader with the file descriptor
    with MS3Record.from_file(file_descriptor, unpack_data=True) as msreader:
        # Read first record
        msr = msreader.read()

        # Verify we got a valid record
        assert msr is not None

        # Check first 6 samples
        assert msr.datasamples[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

        # Check last 6 samples
        assert msr.datasamples[-6:].tolist() == [-508722, -508764, -508809, -508866, -508927, -508986]


def test_msrecord_read_records_summary():
    record_count = 0
    sample_count = 0

    # Direct iteration without context manager
    for msr in MS3Record.from_file(test_path2):
        record_count += 1
        sample_count += msr.samplecnt

    assert record_count == 1141
    assert sample_count == 252000


def test_msrecord_nosuchfile():
    with pytest.raises(MiniSEEDError):
        with MS3Record.from_file("NOSUCHFILE") as msreader:
            msr = msreader.read()
