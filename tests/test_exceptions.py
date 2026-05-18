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

from pymseed import MiniSEEDError
from pymseed.clib import clibmseed
from pymseed.exceptions import NoSuchSourceID


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


class TestNoSuchSourceID:
    def test_args_match_constructor_arguments(self) -> None:
        exc = NoSuchSourceID("FDSN:XX_TEST__B_H_Z")
        assert exc.args == ("FDSN:XX_TEST__B_H_Z",)
        assert exc.sourceid == "FDSN:XX_TEST__B_H_Z"

    def test_repr_shows_constructor_arguments(self) -> None:
        exc = NoSuchSourceID("FDSN:XX_TEST__B_H_Z")
        assert "FDSN:XX_TEST__B_H_Z" in repr(exc)

    def test_pickle_roundtrip(self) -> None:
        original = NoSuchSourceID("FDSN:XX_TEST__B_H_Z")
        for protocol in range(pickle.HIGHEST_PROTOCOL + 1):
            restored = pickle.loads(pickle.dumps(original, protocol))
            assert type(restored) is NoSuchSourceID
            assert restored.args == original.args
            assert restored.sourceid == original.sourceid
