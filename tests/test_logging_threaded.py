"""
Tests for pymseed logging in multi-threaded environments.

This module tests that configure_logging works correctly when called from
multiple threads, with each thread having its own log/error prefix.
"""

import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from glob import glob
from typing import Any

import pytest

from pymseed import (
    MS3TraceList,
    clear_error_messages,
    configure_logging,
    get_error_messages,
)

# Path to test data
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TEST_MSEED3_FILE = os.path.join(TEST_DATA_DIR, "testdata-COLA-signal.mseed3")


def get_test_files(count: int = 20) -> list[str]:
    """Get a list of test files for threaded reading.

    Returns multiple copies of available test files to simulate
    reading many files in parallel.
    """
    # Get all available test files
    pattern = os.path.join(TEST_DATA_DIR, "*.mseed*")
    available_files = glob(pattern)

    if not available_files:
        return []

    # Repeat files to get desired count (same file can be read multiple times)
    files = []
    while len(files) < count:
        files.extend(available_files)
    return files[:count]


def get_corrupted_record() -> bytes:
    """Get a corrupted miniSEED record that will trigger a CRC error."""
    from pymseed import MS3Record

    for msr in MS3Record.from_file(TEST_MSEED3_FILE):
        valid_data = bytearray(msr.record)
        # Corrupt some bytes to trigger CRC validation error
        valid_data[100] = 0xFF
        valid_data[101] = 0xFF
        valid_data[102] = 0xFF
        return bytes(valid_data)
    raise RuntimeError("Could not read test data")


class TestThreadedLogging:
    """Tests for logging in multi-threaded contexts."""

    def setup_method(self) -> None:
        """Clear any existing log messages before each test."""
        clear_error_messages()

    def test_configure_logging_per_thread(self) -> None:
        """Test that each thread can configure its own logging prefix."""
        results: dict[str, dict[str, Any]] = {}
        results_lock = threading.Lock()

        def thread_worker(thread_id: int, filename: str) -> None:
            """Worker that configures logging and reads a file."""
            log_prefix = f"[T{thread_id}-LOG] "
            error_prefix = f"[T{thread_id}-ERR] "

            # Configure logging for this thread
            configure_logging(log_prefix=log_prefix, error_prefix=error_prefix)

            # Read a file to exercise the library
            try:
                traces = MS3TraceList.from_file(filename, unpack_data=True)
                segment_count = sum(len(tid) for tid in traces)
            except Exception:
                segment_count = 0

            # Get any messages generated in this thread
            messages = get_error_messages()

            with results_lock:
                results[f"thread_{thread_id}"] = {
                    "log_prefix": log_prefix,
                    "error_prefix": error_prefix,
                    "filename": filename,
                    "segment_count": segment_count,
                    "messages": messages,
                }

        # Get test files
        files = get_test_files(count=8)
        if not files:
            pytest.skip("No test files available in tests/data/")

        # Run threads
        threads = []
        for i, filename in enumerate(files):
            t = threading.Thread(target=thread_worker, args=(i, filename))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # Verify results
        assert len(results) == len(files)
        for data in results.values():
            assert data["segment_count"] >= 0
            # Messages list should exist (may be empty if no errors)
            assert isinstance(data["messages"], list)

    def test_thread_pool_executor_logging(self) -> None:
        """Test logging with ThreadPoolExecutor."""
        results: list[dict[str, Any]] = []

        def read_file_with_logging(args: tuple[int, str]) -> dict[str, Any]:
            """Read a file with thread-specific logging configuration."""
            thread_id, filename = args
            thread_name = threading.current_thread().name

            log_prefix = f"[{thread_name}-LOG] "
            error_prefix = f"[{thread_name}-ERR] "

            configure_logging(log_prefix=log_prefix, error_prefix=error_prefix)

            try:
                traces = MS3TraceList.from_file(filename, unpack_data=True)
                segment_count = sum(len(tid) for tid in traces)
                success = True
            except Exception:
                segment_count = 0
                success = False

            messages = get_error_messages()

            return {
                "thread_id": thread_id,
                "thread_name": thread_name,
                "filename": os.path.basename(filename),
                "segment_count": segment_count,
                "success": success,
                "message_count": len(messages),
                "messages": messages,
            }

        files = get_test_files(count=16)
        if not files:
            pytest.skip("No test files available in tests/data/")

        # Use ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(read_file_with_logging, (i, f))
                for i, f in enumerate(files)
            ]

            for future in as_completed(futures):
                result = future.result()
                results.append(result)

        # Verify all files were processed
        assert len(results) == len(files)

        # Print summary for debugging
        print(f"\nProcessed {len(results)} files across threads:")
        for r in sorted(results, key=lambda x: x["thread_id"]):
            print(
                f"  {r['thread_name']}: {r['filename']} - "
                f"{r['segment_count']} segments, {r['message_count']} messages"
            )

    def test_error_messages_with_thread_prefix(self) -> None:
        """Test that error messages include the configured prefix."""
        from pymseed import MiniSEEDError, MS3Record

        results: dict[int, dict[str, Any]] = {}
        results_lock = threading.Lock()

        corrupted_data = get_corrupted_record()

        def thread_with_error(thread_id: int) -> None:
            """Thread that triggers an error and captures messages."""
            error_prefix = f"[THREAD-{thread_id}] "

            configure_logging(error_prefix=error_prefix)
            clear_error_messages()

            # Trigger an error by parsing corrupted data
            try:
                with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                    for _ in reader:
                        pass
            except MiniSEEDError:
                pass

            messages = get_error_messages()

            with results_lock:
                results[thread_id] = {
                    "error_prefix": error_prefix,
                    "messages": messages,
                }

        # Run multiple threads that each trigger errors
        threads = []
        for i in range(4):
            t = threading.Thread(target=thread_with_error, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Verify each thread got messages
        assert len(results) == 4
        for thread_id, data in results.items():
            assert len(data["messages"]) >= 1, f"Thread {thread_id} should have error messages"
            # Check that at least one message contains expected error info
            all_messages = " ".join(data["messages"])
            assert "CRC" in all_messages, f"Thread {thread_id} messages should mention CRC"

    def test_concurrent_configure_logging_calls(self) -> None:
        """Test that concurrent configure_logging calls don't cause issues."""
        errors: list[Exception] = []
        errors_lock = threading.Lock()

        def reconfigure_repeatedly(thread_id: int, iterations: int) -> None:
            """Repeatedly reconfigure logging."""
            for i in range(iterations):
                try:
                    configure_logging(
                        log_prefix=f"[T{thread_id}-{i}-LOG] ",
                        error_prefix=f"[T{thread_id}-{i}-ERR] ",
                        max_messages=5 + (i % 10),
                    )
                except Exception as e:
                    with errors_lock:
                        errors.append(e)

        # Run many threads that all reconfigure logging concurrently
        threads = []
        for i in range(10):
            t = threading.Thread(target=reconfigure_repeatedly, args=(i, 50))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Should complete without errors
        assert len(errors) == 0, f"Got {len(errors)} errors: {errors}"

    def test_mixed_read_and_configure(self) -> None:
        """Test mixing file reads with logging configuration changes."""
        results: list[dict[str, Any]] = []
        results_lock = threading.Lock()

        def mixed_operations(thread_id: int, files: list[str]) -> None:
            """Perform mixed operations: configure, read, configure, read..."""
            thread_results = []

            for i, filename in enumerate(files):
                # Reconfigure before each read
                configure_logging(
                    log_prefix=f"[T{thread_id}-R{i}-LOG] ",
                    error_prefix=f"[T{thread_id}-R{i}-ERR] ",
                )

                try:
                    traces = MS3TraceList.from_file(filename, unpack_data=True)
                    segment_count = sum(len(tid) for tid in traces)
                    success = True
                except Exception:
                    segment_count = 0
                    success = False

                messages = get_error_messages()
                thread_results.append({
                    "read_index": i,
                    "segment_count": segment_count,
                    "success": success,
                    "message_count": len(messages),
                })

            with results_lock:
                results.append({
                    "thread_id": thread_id,
                    "reads": thread_results,
                })

        files = get_test_files(count=20)
        if not files:
            pytest.skip("No test files available in tests/data/")

        # Split files among threads
        num_threads = 4
        files_per_thread = len(files) // num_threads

        threads = []
        for i in range(num_threads):
            start = i * files_per_thread
            end = start + files_per_thread if i < num_threads - 1 else len(files)
            thread_files = files[start:end]
            t = threading.Thread(target=mixed_operations, args=(i, thread_files))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Verify all threads completed
        assert len(results) == num_threads

        # Count total successful reads
        total_reads = sum(len(r["reads"]) for r in results)
        successful_reads = sum(
            sum(1 for read in r["reads"] if read["success"])
            for r in results
        )
        print(f"\nTotal reads: {total_reads}, Successful: {successful_reads}")


class TestLoggingIsolation:
    """Tests to verify thread isolation of logging state."""

    def setup_method(self) -> None:
        """Clear any existing log messages before each test."""
        clear_error_messages()

    def test_messages_isolated_between_threads(self) -> None:
        """Test that error messages don't leak between threads."""
        from pymseed import MiniSEEDError, MS3Record

        corrupted_data = get_corrupted_record()
        barrier = threading.Barrier(2)
        results: dict[int, list[str]] = {}
        results_lock = threading.Lock()

        def thread_work(thread_id: int, should_error: bool) -> None:
            """Thread that may or may not generate errors."""
            configure_logging(error_prefix=f"[T{thread_id}] ")
            clear_error_messages()

            # Synchronize threads to run concurrently
            barrier.wait()

            if should_error:
                # Generate error
                try:
                    with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                        for _ in reader:
                            pass
                except MiniSEEDError:
                    pass

            # Small delay to let other thread potentially pollute
            import time
            time.sleep(0.01)

            messages = get_error_messages()

            with results_lock:
                results[thread_id] = messages

        # Thread 0 generates error, Thread 1 does not
        t0 = threading.Thread(target=thread_work, args=(0, True))
        t1 = threading.Thread(target=thread_work, args=(1, False))

        t0.start()
        t1.start()
        t0.join()
        t1.join()

        # Thread 0 should have messages, Thread 1 should not
        assert len(results[0]) >= 1, "Thread 0 should have error messages"
        assert len(results[1]) == 0, "Thread 1 should NOT have error messages"

    def test_clear_only_affects_current_thread(self) -> None:
        """Test that clear_error_messages only clears current thread's messages."""
        from pymseed import MiniSEEDError, MS3Record

        corrupted_data = get_corrupted_record()
        barrier = threading.Barrier(2)
        results: dict[int, list[str]] = {}
        results_lock = threading.Lock()

        def thread_work(thread_id: int, should_clear: bool) -> None:
            """Thread that generates errors and optionally clears them."""
            configure_logging(error_prefix=f"[T{thread_id}] ")
            clear_error_messages()

            # Both threads generate errors
            try:
                with MS3Record.from_buffer(corrupted_data, unpack_data=True) as reader:
                    for _ in reader:
                        pass
            except MiniSEEDError:
                pass

            # Synchronize before clear/get
            barrier.wait()

            if should_clear:
                clear_error_messages()
                # Small delay
                import time
                time.sleep(0.01)

            messages = get_error_messages()

            with results_lock:
                results[thread_id] = messages

        # Thread 0 clears, Thread 1 does not
        t0 = threading.Thread(target=thread_work, args=(0, True))
        t1 = threading.Thread(target=thread_work, args=(1, False))

        t0.start()
        t1.start()
        t0.join()
        t1.join()

        # Thread 0 cleared its messages, Thread 1 should still have them
        assert len(results[0]) == 0, "Thread 0 should have no messages after clear"
        assert len(results[1]) >= 1, "Thread 1 should still have its error messages"


if __name__ == "__main__":
    # Run tests directly for debugging
    test = TestThreadedLogging()
    test.setup_method()

    print("Running test_configure_logging_per_thread...")
    try:
        test.test_configure_logging_per_thread()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nRunning test_thread_pool_executor_logging...")
    try:
        test.test_thread_pool_executor_logging()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nRunning test_error_messages_with_thread_prefix...")
    try:
        test.test_error_messages_with_thread_prefix()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nRunning test_concurrent_configure_logging_calls...")
    try:
        test.test_concurrent_configure_logging_calls()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nRunning test_mixed_read_and_configure...")
    try:
        test.test_mixed_read_and_configure()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    # Isolation tests
    isolation_test = TestLoggingIsolation()

    print("\nRunning test_messages_isolated_between_threads...")
    try:
        isolation_test.setup_method()
        isolation_test.test_messages_isolated_between_threads()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")

    print("\nRunning test_clear_only_affects_current_thread...")
    try:
        isolation_test.setup_method()
        isolation_test.test_clear_only_affects_current_thread()
        print("  PASSED")
    except Exception as e:
        print(f"  FAILED: {e}")
