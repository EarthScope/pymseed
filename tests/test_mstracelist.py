import math
import os

import pytest

from pymseed import MiniSEEDError, MS3TraceList, sample_time, timestr2nstime


test_dir = os.path.abspath(os.path.dirname(__file__))
test_path3 = os.path.join(test_dir, "data", "testdata-COLA-signal.mseed3")


def test_tracelist_read():
    # Read test data from test file into a trace list
    traces = MS3TraceList.from_file(test_path3, unpack_data=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Fetch first traceID
    traceid = traces[0]

    assert traceid.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert traceid.pubversion == 4
    assert traceid.earliest == 1267253400019539000
    assert traceid.earliest_seconds == 1267253400.019539
    assert traceid.latest == 1267257599969538000
    assert traceid.latest_seconds == 1267257599.969538

    # Fetch first trace segment
    segment = traceid[0]

    assert segment.starttime == 1267253400019539000
    assert segment.starttime_seconds == 1267253400.019539
    assert segment.endtime == 1267257599969538000
    assert segment.endtime_seconds == 1267257599.969538
    assert segment.samprate == 20.0
    assert segment.samplecnt == 84000
    assert segment.numsamples == 84000
    assert segment.sampletype == "i"
    assert segment.sampletype == "i"

    # Data sample array tests
    data = segment.datasamples

    # Check first 6 samples
    assert data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

    # Check last 6 samples
    assert data[-6:].tolist() == [-929184, -928936, -928632, -928248, -927779, -927206]

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert foundid.pubversion == 4
    assert foundid.earliest == 1267253400019539000
    assert foundid.earliest_seconds == 1267253400.019539
    assert foundid.latest == 1267257599969538000
    assert foundid.latest_seconds == 1267257599.969538

    foundseg = foundid[0]

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]


def test_tracelist_read_buffer():
    # Read test data from test file into a buffer
    with open(test_path3, "rb") as fp:
        buffer = fp.read()

    # Read miniSEED data from buffer into a trace list
    traces = MS3TraceList.from_buffer(buffer, unpack_data=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Fetch first traceID
    traceid = traces[0]

    assert traceid.sourceid == "FDSN:IU_COLA_00_B_H_1"
    assert traceid.pubversion == 4
    assert traceid.earliest == 1267253400019539000
    assert traceid.earliest_seconds == 1267253400.019539
    assert traceid.latest == 1267257599969538000
    assert traceid.latest_seconds == 1267257599.969538

    # Fetch first trace segment
    segment = traceid[0]

    assert segment.starttime == 1267253400019539000
    assert segment.starttime_seconds == 1267253400.019539
    assert segment.endtime == 1267257599969538000
    assert segment.endtime_seconds == 1267257599.969538
    assert segment.samprate == 20.0
    assert segment.samplecnt == 84000
    assert segment.numsamples == 84000
    assert segment.sampletype == "i"
    assert segment.sampletype == "i"

    # Data sample array tests
    data = segment.datasamples

    # Check first 6 samples
    assert data[0:6].tolist() == [-502916, -502808, -502691, -502567, -502433, -502331]

    # Check last 6 samples
    assert data[-6:].tolist() == [-929184, -928936, -928632, -928248, -927779, -927206]

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    assert foundid.pubversion == 4
    assert foundid.earliest == 1267253400019539000
    assert foundid.earliest_seconds == 1267253400.019539
    assert foundid.latest == 1267257599969538000
    assert foundid.latest_seconds == 1267257599.969538

    foundseg = foundid[0]

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]


def test_tracelist_read_recordlist():
    traces = MS3TraceList(test_path3, unpack_data=False, record_list=True)

    assert len(traces) == 3

    assert list(traces.sourceids()) == [
        "FDSN:IU_COLA_00_B_H_1",
        "FDSN:IU_COLA_00_B_H_2",
        "FDSN:IU_COLA_00_B_H_Z",
    ]

    # Search for a specific trace ID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    foundseg = foundid[0]

    assert foundseg.numsamples == 0

    # Get sample size and type from first trace ID
    (sample_size, sample_type) = foundseg.sample_size_type

    assert sample_size == 4
    assert sample_type == "i"

    # Unpack data samples using in-place buffer
    foundseg.unpack_recordlist()

    assert foundseg.numsamples == 84000

    # Check first 6 samples
    assert foundseg.datasamples[0:6].tolist() == [
        -231394,
        -231367,
        -231376,
        -231404,
        -231437,
        -231474,
    ]

    # Check last 6 samples
    assert foundseg.datasamples[-6:].tolist() == [
        -165263,
        -162103,
        -159002,
        -155907,
        -152810,
        -149774,
    ]

    # Traverse the record list counting records and samples
    record_count = 0
    sample_count = 0
    for record_ptr in foundseg.recordlist.records():
        record_count += 1
        sample_count += record_ptr.record.samplecnt

    assert record_count == 386
    assert sample_count == 84000


def test_tracelist_slicing():
    traces = MS3TraceList(test_path3, unpack_data=True)

    assert len(traces) == 3

    # Test slicing (trace has 1 segment, so test valid slices)
    traceid = traces[0]

    assert len(traceid[0:1]) == 1
    assert traceid[0:1][0].starttime == 1267253400019539000

    # Test empty slice
    assert len(traceid[1:3]) == 0  # No segments at indices 1-2

    # Test full slice
    assert len(traceid[:]) == 1
    assert len(traceid) == 1

    # Test slicing (trace has 1 segment, so test valid slices)

    assert len(traces[0:1]) == 1
    assert len(traces[0:1][0]) == 1


def test_tracelist_numpy():
    np = pytest.importorskip("numpy")

    traces = MS3TraceList(test_path3, record_list=True)

    # Fetch first traceID
    traceid = traces[0]

    # Fetch first trace segment
    segment = traceid[0]

    # Unpack data samples from record list before accessing numpy data
    segment.unpack_recordlist()

    # Data sample array tests
    np_data = segment.np_datasamples

    assert np_data.dtype == np.int32

    assert np_data.shape == (84000,)

    # Check first 6 samples
    assert np.all(
        np_data[0:6] == [-502916, -502808, -502691, -502567, -502433, -502331]
    )

    # Check last 6 samples
    assert np.all(
        np_data[-6:] == [-929184, -928936, -928632, -928248, -927779, -927206]
    )

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    foundseg = foundid[0]

    # Unpack data samples from record list before accessing numpy data
    foundseg.unpack_recordlist()

    # Check first 6 samples
    assert np.all(
        foundseg.np_datasamples[0:6].tolist()
        == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]
    )

    # Check last 6 samples
    assert np.all(
        foundseg.np_datasamples[-6:].tolist()
        == [
            -165263,
            -162103,
            -159002,
            -155907,
            -152810,
            -149774,
        ]
    )

def test_tracelist_numpy_arrayfrom_recordlist():
    np = pytest.importorskip("numpy")

    with pytest.raises(ValueError):
        # Must specify record_list=True
        traces = MS3TraceList(test_path3)
        traceid = traces[0]
        segment = traceid[0]
        np_data = segment.create_numpy_array_from_recordlist()

    traces = MS3TraceList(test_path3, record_list=True)

    # Search for a specific TraceID
    foundid = traces.get_traceid("FDSN:IU_COLA_00_B_H_Z")

    assert foundid.sourceid == "FDSN:IU_COLA_00_B_H_Z"
    foundseg = foundid[0]

    np_data = foundseg.create_numpy_array_from_recordlist()

    assert np_data.dtype == np.int32

    assert np_data.shape == (84000,)

    # Check first 6 samples
    assert np.all(
        np_data[0:6].tolist()
        == [
            -231394,
            -231367,
            -231376,
            -231404,
            -231437,
            -231474,
        ]
    )

    # Check last 6 samples
    assert np.all(
        np_data[-6:].tolist()
        == [
            -165263,
            -162103,
            -159002,
            -155907,
            -152810,
            -149774,
        ]
    )


# A sine wave generator
def sine_generator(start_degree=0, yield_count=100, total=1000):
    """A generator returning a continuing sequence of sine values."""
    generated = 0
    while generated < total:
        chunk_size = min(yield_count, total - generated)

        # Yield a list of continuing sine values
        yield [
            int(math.sin(math.radians(x)) * 500)
            for x in range(start_degree, start_degree + chunk_size)
        ]

        start_degree += chunk_size
        generated += chunk_size


# A global record buffer
record_buffer = bytearray()


def record_handler(record, handler_data):
    """A callback function for MSTraceList.set_record_handler()
    Adds the record to a global buffer for testing
    """
    global record_buffer
    record_buffer.extend(bytes(record))


test_pack3 = os.path.join(test_dir, "data", "packtest_sine2000.mseed3")

@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_mstracelist_pack():
    # Create a new MSTraceList object
    traces = MS3TraceList()

    total_samples = 0
    total_records = 0
    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    format_version = 3
    record_length = 512

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

        (packed_samples, packed_records) = traces.pack(
            record_handler,
            flush_data=False,
            format_version=format_version,
            record_length=record_length,
        )

        total_samples += packed_samples
        total_records += packed_records

    (packed_samples, packed_records) = traces.pack(
        record_handler, format_version=format_version, record_length=record_length
    )

    total_samples += packed_samples
    total_records += packed_records

    assert total_samples == 2000
    assert total_records == 5

    with open(test_pack3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3

def test_mstracelist_generate_rollingbuffer():
    """Test creation of miniSEED v3 records from a trace list using a rolling buffer.

    The rolling buffer usage removes packed data from the trace list after each
    pack, data is then added and packed in later calls.  After the final pack to
    flush any remaining data, the trace list is empty.
    """
    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    format_version = 3
    record_length = 512

    # Test creation of a miniSEED v3 records
    record_buffer = b""
    record_count = 0

    # Mimic generating miniSEED records from a continuous data stream by adding
    # data to the trace list and generating filled records
    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

        # Generate filled records during regular data flow
        for record in traces.generate(
            record_length=record_length,
            format_version=format_version,
            flush_data=False,
            flush_idle_seconds=10,
            removed_packed=True,
        ):
            record_buffer += record
            record_count += 1

    # Final record creation to flush any remaining data
    for record in traces.generate(
        record_length=record_length,
        format_version=format_version,
        flush_data=True,
        removed_packed=True,
    ):
        record_buffer += record
        record_count += 1

    assert record_count == 5
    assert len(record_buffer) == 2471

    with open(test_pack3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3

    assert len(traces) == 0 # Trace list should be empty after final pack

test_pack3_x3 = os.path.join(test_dir, "data", "packtest_sine500x3.mseed3")
test_pack2_x3 = os.path.join(test_dir, "data", "packtest_sine500x3.mseed2")

def test_mstracelist_generate():
    """Test creation of miniSEED v3 and v2 records from a trace list.

    The same trace list is used for both versions to ensure that the data is
    maintained in the trace list after packing.
    """

    # A sine wave of 500 samples
    sine_500 = [int(math.sin(math.radians(x)) * 500) for x in range(0, 500)]

    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")
    record_length = 512

    # Add 3 traces to the list
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_1",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_2",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )
    traces.add_data(
        sourceid="FDSN:XX_TEST__B_S_3",
        data_samples=sine_500,
        sample_type="i",
        sample_rate=sample_rate,
        start_time=start_time,
    )

    # Test creation of a miniSEED v3 records
    record_buffer = b""
    record_count = 0

    for record in traces.generate(
        record_length=record_length,
        format_version=3
    ):
        record_buffer += record
        record_count += 1

    assert record_count == 6
    assert len(record_buffer) == 2082

    with open(test_pack3_x3, "rb") as f:
        data_v3 = f.read()
        assert record_buffer == data_v3

    # Test creation of a miniSEED v2 records
    record_buffer = b""
    record_count = 0

    for record in traces.generate(
        record_length=record_length,
        format_version=2
    ):
        record_buffer += record
        record_count += 1

    assert record_count == 6
    assert len(record_buffer) == 3072

    with open(test_pack2_x3, "rb") as f:
        data_v2 = f.read()
        assert record_buffer == data_v2

    assert len(traces) == 3 # Traces should remain in the list
    assert traces[0][0].numsamples == 500
    assert traces[1][0].numsamples == 500
    assert traces[2][0].numsamples == 500


test_pack2 = os.path.join(test_dir, "data", "packtest_sine2000.mseed2")

def test_mstracelist_to_file(tmp_path):
    """Test MS3TraceList.to_file() method using pytest's tmp_path fixture."""
    # Create a new MSTraceList object
    traces = MS3TraceList()

    sample_rate = 40.0
    start_time = timestr2nstime("2024-01-01T15:13:55.123456789Z")

    for new_data in sine_generator(yield_count=100, total=2000):
        traces.add_data(
            sourceid="FDSN:XX_TEST__B_S_X",
            data_samples=new_data,
            sample_type="i",
            sample_rate=sample_rate,
            start_time=start_time,
        )

        start_time = sample_time(start_time, len(new_data), sample_rate)

    # Use pytest's tmp_path fixture to create a temporary file
    temp_file = tmp_path / "test_output.mseed3"

    # Write using to_file method
    records_written = traces.to_file(
        str(temp_file), overwrite=True, format_version=2, max_reclen=512
    )

    # Verify number of records written
    assert records_written == 5

    # Verify file was created and has content
    assert temp_file.exists()
    assert temp_file.stat().st_size > 0

    # Compare created file to reference file
    with open(test_pack2, "rb") as f:
        reference_data = f.read()
        with open(temp_file, "rb") as f:
            test_data = f.read()
            assert reference_data == test_data


def test_mstracelist_nosuchfile():
    with pytest.raises(MiniSEEDError):
        traces = MS3TraceList("NOSUCHFILE")
