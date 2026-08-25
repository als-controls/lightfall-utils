"""Tests for the SharedContext singleton (no IOC required)."""

from lightfall_utils.ca import SharedContext


def test_singleton_identity():
    a = SharedContext.get_instance()
    b = SharedContext.get_instance()
    assert a is b


def test_reset_drops_singleton():
    a = SharedContext.get_instance()
    SharedContext.reset()
    b = SharedContext.get_instance()
    assert a is not b
