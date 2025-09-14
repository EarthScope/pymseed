import time

import pytest

from pymseed import clibmseed
from pymseed.definitions import TimeFormat, SubSecond, DataEncoding
from pymseed.util import (
    nstime2timestr,
    timestr2nstime,
    sourceid2nslc,
    nslc2sourceid,
    encoding_string,
    error_string,
    sample_size,
    encoding_sizetype,
    sample_time,
    system_time,
)


class TestTimeConversion:
    """Test time conversion functions"""

    def test_nstime2timestr_basic(self):
        """Test basic nanosecond to time string conversion"""
        # Test with Unix epoch (0 nanoseconds = 1970-01-01T00:00:00Z)
        result = nstime2timestr(0)
        assert result == "1970-01-01T00:00:00Z"

        # Test with a known timestamp
        nstime = 1672574445123456789
        result = nstime2timestr(nstime)
        assert result == "2023-01-01T12:00:45.123456789Z"

    def test_nstime2timestr_different_formats(self):
        """Test different time format options"""
        nstime = 1672574445123456789

        # Test ISO month-day format (without Z)
        result = nstime2timestr(nstime, TimeFormat.ISOMONTHDAY)
        assert result == "2023-01-01T12:00:45.123456789"

        # Test SEED ordinal format
        result = nstime2timestr(nstime, TimeFormat.SEEDORDINAL)
        assert result == "2023,001,12:00:45.123456789"

        # Test space-separated format
        result = nstime2timestr(nstime, TimeFormat.ISOMONTHDAY_SPACE_Z)
        assert result == "2023-01-01 12:00:45.123456789Z"

    def test_nstime2timestr_different_subseconds(self):
        """Test different subsecond precision options"""
        nstime = 1672574445123456789  # With nanosecond precision

        # Test no subseconds
        result = nstime2timestr(nstime, subsecond=SubSecond.NONE)
        assert result == "2023-01-01T12:00:45Z"

        # Test microsecond precision
        result = nstime2timestr(nstime, subsecond=SubSecond.MICRO)
        assert result == "2023-01-01T12:00:45.123456Z"

        # Test nanosecond precision
        result = nstime2timestr(nstime, subsecond=SubSecond.NANO)
        assert result == "2023-01-01T12:00:45.123456789Z"

    def test_timestr2nstime_basic(self):
        """Test basic time string to nanosecond conversion"""
        # Test Unix epoch
        result = timestr2nstime("1970-01-01T00:00:00.000000000Z")
        assert result == 0

        # Test known timestamp
        result = timestr2nstime("2023-01-01T12:00:45.123456789Z")
        assert result == 1672574445123456789

    def test_timestr2nstime_various_formats(self):
        """Test time string parsing with various formats"""
        expected_nstime = 1672574445123456789

        # Test with Z suffix
        result = timestr2nstime("2023-01-01T12:00:45.123456789Z")
        assert result == expected_nstime

        # Test without Z suffix
        result = timestr2nstime("2023-01-01T12:00:45.123456789")
        assert result == expected_nstime

        # Test with space separator
        result = timestr2nstime("2023-01-01 12:00:45.123456789")
        assert result == expected_nstime

    def test_time_conversion_roundtrip(self):
        """Test that time conversion functions are inverse operations"""
        original_nstime = 1672574445123456789

        # Convert to string and back
        timestr = nstime2timestr(original_nstime)
        converted_nstime = timestr2nstime(timestr)

        assert converted_nstime == original_nstime

    def test_nstime2timestr_edge_cases(self):
        """Test edge cases for time conversion"""

        # Test with negative timestamp
        result = nstime2timestr(-1)
        assert result == "1969-12-31T23:59:59.999999999Z"


class TestSourceIdConversion:
    """Test source ID conversion functions"""

    def test_sourceid2nslc_basic(self):
        """Test basic source ID to NSLC conversion"""
        sourceid = "FDSN:XX_TEST__B_H_Z"
        net, sta, loc, chan = sourceid2nslc(sourceid)

        assert net == "XX"
        assert sta == "TEST"
        assert loc == ""
        assert chan == "BHZ"

    def test_sourceid2nslc_with_location(self):
        """Test source ID conversion with location code"""
        sourceid = "FDSN:IU_COLA_00_B_H_Z"
        net, sta, loc, chan = sourceid2nslc(sourceid)

        assert net == "IU"
        assert sta == "COLA"
        assert loc == "00"
        assert chan == "BHZ"

    def test_nslc2sourceid_basic(self):
        """Test basic NSLC to source ID conversion"""
        result = nslc2sourceid("IU", "ANMO", "00", "BHZ")
        assert result == "FDSN:IU_ANMO_00_B_H_Z"

    def test_nslc2sourceid_empty_location(self):
        """Test NSLC to source ID conversion with empty location"""
        result = nslc2sourceid("IU", "ANMO", "", "BHZ")
        assert result == "FDSN:IU_ANMO__B_H_Z"

    def test_sourceid_conversion_roundtrip(self):
        """Test that source ID conversion functions are inverse operations"""
        original_net, original_sta, original_loc, original_chan = "IU", "ANMO", "00", "BHZ"

        # Convert to source ID and back
        sourceid = nslc2sourceid(original_net, original_sta, original_loc, original_chan)
        net, sta, loc, chan = sourceid2nslc(sourceid)

        assert net == original_net
        assert sta == original_sta
        assert loc == original_loc
        assert chan == original_chan


class TestEncodingFunctions:
    """Test encoding-related utility functions"""

    def test_encoding_string_basic(self):
        """Test getting encoding description strings"""
        # Test known encoding types using integer values
        result = encoding_string(int(DataEncoding.TEXT))
        assert "text" in result.lower() or "utf" in result.lower()

        result = encoding_string(int(DataEncoding.INT32))
        assert "32" in result and ("int" in result.lower() or "integer" in result.lower())

        result = encoding_string(int(DataEncoding.FLOAT32))
        assert "32" in result and "float" in result.lower()

        result = encoding_string(int(DataEncoding.FLOAT64))
        assert "64" in result and "float" in result.lower()

        result = encoding_string(int(DataEncoding.STEIM1))
        assert "steim" in result.lower() and "1" in result

        result = encoding_string(int(DataEncoding.STEIM2))
        assert "steim" in result.lower() and "2" in result

    def test_sample_size_basic(self):
        """Test getting sample sizes for different encodings"""
        # ms_samplesize expects a char sampletype, not encoding number
        # Test with sample type characters as bytes
        assert sample_size(b"i") == 4  # integer type
        assert sample_size(b"f") == 4  # float type
        assert sample_size(b"d") == 8  # double type
        assert sample_size(b"t") == 1  # text type

    def test_encoding_sizetype_basic(self):
        """Test getting encoding size and type information"""
        # Test INT16
        size, sample_type = encoding_sizetype(int(DataEncoding.INT16))
        assert size == 4  # From the C code, INT16 also returns 4
        assert sample_type == "i"  # integer type

        # Test INT32
        size, sample_type = encoding_sizetype(int(DataEncoding.INT32))
        assert size == 4
        assert sample_type == "i"  # integer type

        # Test FLOAT32
        size, sample_type = encoding_sizetype(int(DataEncoding.FLOAT32))
        assert size == 4
        assert sample_type == "f"  # float type

        # Test FLOAT64
        size, sample_type = encoding_sizetype(int(DataEncoding.FLOAT64))
        assert size == 8
        assert sample_type == "d"  # double type (not 'f')

        # Test Steim1
        size, sample_type = encoding_sizetype(int(DataEncoding.STEIM1))
        assert size == 4
        assert sample_type == "i"  # integer type

        # Test Steim2
        size, sample_type = encoding_sizetype(int(DataEncoding.STEIM2))
        assert size == 4
        assert sample_type == "i"  # integer type

    def test_encoding_sizetype_invalid_encoding(self):
        """Test error handling for invalid encoding"""
        with pytest.raises(ValueError):
            encoding_sizetype(255)  # Invalid encoding number that fits in uint8_t


class TestErrorHandling:
    """Test error handling functions"""

    def test_error_string_basic(self):
        """Test getting error description strings"""
        # Test with error code 0 (usually means no error/success)
        result = error_string(0)
        assert isinstance(result, str)
        assert len(result) > 0

        # Test with common error codes
        for error_code in [-1, -2, -3]:
            result = error_string(error_code)
            assert isinstance(result, str)
            assert len(result) > 0


class TestSampleTime:
    """Test sample time calculation function"""

    def test_sample_time_basic(self):
        """Test basic sample time calculations"""
        # Test with 1 Hz sampling rate
        base_time = 1672574445000000000  # Some base time in nanoseconds
        samprate = 1.0  # 1 Hz = 1 sample per second

        # Sample at offset 0 should be at base time
        result = sample_time(base_time, 0, samprate)
        assert result == base_time

        # Sample at offset 1 should be 1 second later (1e9 nanoseconds)
        result = sample_time(base_time, 1, samprate)
        expected = base_time + int(1e9)  # 1 second in nanoseconds
        assert result == expected

    def test_sample_time_high_frequency(self):
        """Test sample time calculations with high frequency sampling"""
        base_time = 1672574445000000000
        samprate = 100.0  # 100 Hz = 100 samples per second

        # Sample at offset 0
        result = sample_time(base_time, 0, samprate)
        assert result == base_time

        # Sample at offset 1 should be 0.01 seconds later (1e7 nanoseconds)
        result = sample_time(base_time, 1, samprate)
        expected = base_time + int(1e7)  # 0.01 seconds in nanoseconds
        assert result == expected

        # Sample at offset 100 should be 1 second later
        result = sample_time(base_time, 100, samprate)
        expected = base_time + int(1e9)  # 1 second in nanoseconds
        assert result == expected

    def test_sample_time_fractional_offset(self):
        """Test sample time calculations with fractional sampling rates"""
        base_time = 1672574445000000000
        samprate = 0.1  # 0.1 Hz = 1 sample per 10 seconds

        # Sample at offset 1 should be 10 seconds later
        result = sample_time(base_time, 1, samprate)
        expected = base_time + int(10e9)  # 10 seconds in nanoseconds
        assert result == expected

    def test_sample_time_zero_offset(self):
        """Test sample time calculation with zero offset"""
        base_time = 1672574445123456789
        result = sample_time(base_time, 0, 50.0)
        assert result == base_time


class TestSystemtime:
    """Test systemtime function"""

    def test_systemtime(self):
        """Test systemtime function"""
        system_time_ns = system_time()

        system_time_sec = float(system_time_ns / clibmseed.NSTMODULUS)

        # Verify within a 100 milliseconds of time.time()
        assert abs(time.time() - system_time_sec) < 0.1, (
            f"Time difference {abs(time.time() - system_time_sec) * 1e3:.3f} ms exceeds 100 ms tolerance"
        )
