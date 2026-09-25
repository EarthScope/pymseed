"""Tests for pymseed exception classes.

These regress the contract that exception subclasses delegate to
BaseException.__init__ so that:
  * exc.args matches the constructor arguments,
  * repr(exc) shows the arguments,
  * the exception survives pickle round-trips.

Without super().__init__(...), all three of those invariants quietly break,
and downstream tooling (Sentry, traceback formatters, multiprocessing
workers re-raising in the parent) misbehaves.
"""

from __future__ import annotations

import copy
import pickle

import pytest

from pymseed import MiniSEEDError, PymseedError
from pymseed.clib import clibmseed


class TestPymseedErrorHierarchy:
    """Tests for the shared base class so callers can write a single
    `except PymseedError:` catch and reach every pymseed-defined exception."""

    def test_miniseed_error_is_pymseed_error(self) -> None:
        assert issubclass(MiniSEEDError, PymseedError)
        assert isinstance(MiniSEEDError(-1, "x"), PymseedError)

    def test_pymseed_error_is_runtime_error(self) -> None:
        # The base intentionally inherits from RuntimeError because the
        # concrete subclasses describe runtime / I/O / data-corruption
        # conditions (bad CRC, EOF, wrong length, allocation failure)
        # rather than "right type, wrong value" inputs that ValueError is
        # meant for. This was a deliberate breaking change from ValueError;
        # callers catching `except ValueError` to pick up pymseed errors
        # must now use PymseedError (preferred) or RuntimeError.
        assert issubclass(PymseedError, RuntimeError)
        assert isinstance(MiniSEEDError(-1, "x"), RuntimeError)

        # Make the breaking change explicit: pymseed errors are no longer
        # ValueError. Regression-protect anyone tempted to "fix" the base.
        assert not issubclass(PymseedError, ValueError)
        assert not isinstance(MiniSEEDError(-1, "x"), ValueError)

    def test_single_except_catches_all_pymseed_errors(self) -> None:
        # The whole point of the base class: one except clause catches every
        # concrete subclass.
        exc = MiniSEEDError(-1, "x")
        try:
            raise exc
        except PymseedError as caught:
            assert caught is exc
        else:
            pytest.fail(f"PymseedError did not catch {type(exc).__name__}")


class TestMiniSEEDError:
    def test_args_match_constructor_arguments(self) -> None:
        exc = MiniSEEDError(-1, "boom")
        assert exc.args == (-1, "boom")
        assert exc.status_code == -1
        assert exc.message == "boom"

    def test_repr_shows_constructor_arguments(self) -> None:
        exc = MiniSEEDError(-1, "boom")
        rendering = repr(exc)
        # Don't pin the exact format (CPython differs by minor version),
        # but the arguments must be visible.
        assert "-1" in rendering
        assert "boom" in rendering
        # An empty-args repr like "MiniSEEDError()" must NOT match.
        assert rendering != "MiniSEEDError()"

    def test_str_unchanged_by_super_init(self) -> None:
        # str() goes through the custom __str__ which formats via
        # error_string(status_code) — it must keep working.
        exc = MiniSEEDError(-1, "boom")
        rendering = str(exc)
        assert "boom" in rendering

    def test_str_renders_once_and_caches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Logging frameworks often stringify an exception more than once.
        Verify that ``str(exc)`` is cached: ``error_string`` (a C-call) is
        invoked at most during ``__init__``, never again on subsequent
        ``str()`` calls.
        """
        from pymseed import exceptions as ex_mod

        call_count = {"n": 0}
        real_error_string = ex_mod.error_string

        def counting_error_string(code: int) -> str | None:
            call_count["n"] += 1
            return real_error_string(code)

        monkeypatch.setattr(ex_mod, "error_string", counting_error_string)

        exc = MiniSEEDError(-1, "boom")
        baseline = call_count["n"]
        # Render the exception many times.
        for _ in range(50):
            str(exc)
        assert call_count["n"] == baseline, (
            f"str(exc) re-invoked error_string: {call_count['n']} calls "
            f"(expected {baseline} after construction)"
        )

    def test_str_has_no_trailing_space_when_message_is_none(self) -> None:
        # Regression: the old f-string concatenation produced a trailing space
        # like "Error reading miniSEED record " when message was None/empty.
        # That broke test assertions and log scraping. Verify the trailing
        # space is gone for None, empty string, and a non-empty message.
        for message in (None, ""):
            exc = MiniSEEDError(-1, message)
            rendering = str(exc)
            assert rendering == rendering.rstrip(), (
                f"str(MiniSEEDError(-1, {message!r})) has trailing whitespace: {rendering!r}"
            )

        # Sanity: non-empty message still gets the ` :: <message>` suffix.
        exc = MiniSEEDError(-1, "boom")
        assert str(exc).endswith(" :: boom")

    def test_positive_status_defers_to_the_callers_message(self) -> None:
        """Only negative values are error codes. A positive one means whatever
        the producing function says it means — MS_ENDOFFILE and msr3_parse()'s
        "one more byte needed" are both 1 — so no error string may be looked up
        for it. Previously error_string(414) returned nothing and the caller's
        description was buried under a bogus "Unknown error code: 414"."""
        exc = MiniSEEDError(414, "Incomplete miniSEED record, 414 more bytes needed")

        assert exc.status_code == 414
        assert str(exc) == "Incomplete miniSEED record, 414 more bytes needed"

        # 1 collides with MS_ENDOFFILE; the caller's meaning must still win.
        assert str(MiniSEEDError(1, "Incomplete miniSEED record, 1 more byte needed")) == (
            "Incomplete miniSEED record, 1 more byte needed"
        )

        # With no message there is nothing better to report than the raw value.
        assert str(MiniSEEDError(414)) == "Unknown status code: 414"

    def test_negative_status_still_renders_error_string(self) -> None:
        """Codes libmseed does have a string for are unaffected."""
        assert str(MiniSEEDError(clibmseed.MS_INVALIDCRC)) == "Invalid CRC detected"
        assert str(MiniSEEDError(clibmseed.MS_NOTSEED, "ctx")) == (
            "No miniSEED data detected :: ctx"
        )

    @pytest.mark.parametrize(
        "args",
        [
            (-1, "boom"),
            (-1, None),
            (clibmseed.MS_GENERROR, "generic"),
        ],
    )
    def test_pickle_roundtrip(self, args: tuple) -> None:
        original = MiniSEEDError(*args)
        # Force a non-trivial error_messages attribute so we also verify
        # __dict__ state survives the round-trip.
        if not original.error_messages:
            original.error_messages = ["captured warning A", "captured warning B"]

        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            data = pickle.dumps(original, protocol)
            restored = pickle.loads(data)
            assert type(restored) is MiniSEEDError
            assert restored.args == original.args
            assert restored.status_code == original.status_code
            assert restored.message == original.message
            assert restored.error_messages == original.error_messages

    def test_copy_roundtrip(self) -> None:
        # copy.deepcopy relies on the same __reduce__ machinery as pickle.
        original = MiniSEEDError(-1, "boom")
        original.error_messages = ["state"]
        clone = copy.deepcopy(original)
        assert clone.args == original.args
        assert clone.status_code == original.status_code
        assert clone.message == original.message
        assert clone.error_messages == original.error_messages
        # deepcopy must produce a distinct list, not share the original.
        assert clone.error_messages is not original.error_messages


class TestMiniSEEDErrorDraining:
    """MiniSEEDError drains the log registry for any non-success status, and
    every pymseed entry point clears it first (logging.begin_operation()), so
    an error never carries diagnostics left over from an earlier, unrelated
    call. See CHANGELOG entry for the bug this regresses."""

    def test_negative_non_generic_status_captures_its_own_messages(self) -> None:
        import os

        from pymseed import MS3Record

        with open(os.path.join("tests", "data", "testdata-COLA-signal.mseed3"), "rb") as f:
            record = bytearray(f.read(512))
        record[100] ^= 0xFF  # corrupt the payload to trip CRC validation

        with pytest.raises(MiniSEEDError) as excinfo:
            MS3Record.parse(bytes(record), validate_crc=True)

        exc = excinfo.value
        assert exc.status_code == clibmseed.MS_INVALIDCRC
        assert exc.error_messages
        assert any("CRC" in msg for msg in exc.error_messages)

    def test_unrelated_later_error_does_not_carry_a_stale_message(self) -> None:
        import os

        from pymseed import MS3Record, MS3TraceList

        with open(os.path.join("tests", "data", "testdata-COLA-signal.mseed3"), "rb") as f:
            record = bytearray(f.read(512))
        record[100] ^= 0xFF

        with pytest.raises(MiniSEEDError) as first:
            MS3Record.parse(bytes(record), validate_crc=True)
        assert first.value.error_messages  # precondition: it did leave a message

        # A later, unrelated failure must not carry the CRC message above.
        with pytest.raises(MiniSEEDError) as second:
            MS3TraceList().add_file("/nonexistent/file-for-pymseed-tests.mseed")

        assert not any("CRC" in msg for msg in second.value.error_messages)
