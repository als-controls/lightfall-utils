"""Tests for the logging module."""

import time

import pytest
from loguru import logger

from lightfall_utils.logging import (
    get_cumulative_stats,
    log_time,
    reset_cumulative_stats,
)
from lightfall_utils import logging as lfu_logging


def test_log_time_basic() -> None:
    """Test basic log_time functionality."""
    messages: list[str] = []
    handler_id = logger.add(lambda m: messages.append(str(m)), level="INFO")
    try:
        with log_time("Test operation"):
            time.sleep(0.01)

        assert any("Test operation" in m for m in messages)
        assert any("elapsed:" in m for m in messages)
    finally:
        logger.remove(handler_id)


def test_log_time_cumulative() -> None:
    """Test cumulative timing statistics."""
    reset_cumulative_stats()

    for _ in range(3):
        with log_time("Repeated op", cumulative_key="test_op"):
            time.sleep(0.01)

    stats = get_cumulative_stats("test_op")
    assert "test_op" in stats
    assert stats["test_op"]["count"] == 3.0
    assert stats["test_op"]["total_ms"] >= 30.0  # At least 30ms total
    assert stats["test_op"]["avg_ms"] >= 10.0  # At least 10ms average


def test_reset_cumulative_stats() -> None:
    """Test resetting cumulative stats."""
    reset_cumulative_stats()

    with log_time("Op 1", cumulative_key="key1"):
        pass
    with log_time("Op 2", cumulative_key="key2"):
        pass

    assert "key1" in get_cumulative_stats()
    assert "key2" in get_cumulative_stats()

    reset_cumulative_stats("key1")
    stats = get_cumulative_stats()
    assert "key1" not in stats
    assert "key2" in stats

    reset_cumulative_stats()
    assert get_cumulative_stats() == {}


def _record(level_name: str, name: str) -> dict:
    return {"level": lfu_logging.logger.level(level_name), "name": name}


def test_mute_debug_modules_filters_debug_only():
    lfu_logging.configure_logging(level="DEBUG", console=False, mute_debug_modules=("noisy",))
    assert not lfu_logging._third_party_filter(_record("DEBUG", "noisy.core"))
    assert lfu_logging._third_party_filter(_record("INFO", "noisy.core"))
    assert lfu_logging._third_party_filter(_record("DEBUG", "quiet.core"))


def test_mute_debug_modules_defaults_to_empty():
    lfu_logging.configure_logging(level="DEBUG", console=False)
    assert lfu_logging._third_party_filter(_record("DEBUG", "noisy.core"))


def test_log_time_logs_even_when_block_raises():
    captured: list[str] = []
    handler_id = lfu_logging.logger.add(
        lambda message: captured.append(str(message)), level="INFO", format="{message}"
    )
    try:
        with pytest.raises(ValueError):
            with lfu_logging.log_time("boom-block"):
                raise ValueError("boom")
    finally:
        lfu_logging.logger.remove(handler_id)
    assert any("boom-block" in line and "elapsed" in line for line in captured)
