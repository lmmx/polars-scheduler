import polars as pl
import pytest
from polars_scheduler import Scheduler


def test_exact_time_window():
    """Test scheduling with exact time window (e.g., '08:00')."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast"],
            "Category": ["meal"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["08:00"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest")
    breakfast = result.filter(pl.col("entity_name") == "breakfast")
    assert breakfast.get_column("time_hhmm").item() == "08:00"


@pytest.mark.failing(reason="Schedules both at 7am")
def test_range_time_window():
    """Test scheduling with a time range window (e.g., '12:00-13:00')."""
    df = pl.DataFrame(
        {
            "Event": ["lunch"],
            "Category": ["meal"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["12:00-13:00"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    # With earliest strategy
    earliest = scheduler.create(strategy="earliest")
    lunch_time = (
        earliest.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 12 <= lunch_time == 13  # 12:00 - 13:00
    # With latest strategy
    latest = scheduler.create(strategy="latest")
    lunch_time = (
        latest.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 12 <= lunch_time <= 13  # 12:00 - 13:00


@pytest.mark.failing(reason="Schedules both at 7am")
def test_multiple_windows():
    """Test scheduling with multiple windows."""
    df = pl.DataFrame(
        {
            "Event": ["shake"],
            "Category": ["supplement"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["17:00-19:00"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    # With earliest strategy, should pick first window
    earliest = scheduler.create(strategy="earliest")
    shake_time = (
        earliest.filter(pl.col("entity_name") == "shake")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert shake_time == 17  # 17:00
    # With latest strategy, should pick last window
    latest = scheduler.create(strategy="latest")
    shake_time = (
        latest.filter(pl.col("entity_name") == "shake")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 17 <= shake_time <= 19  # 17:00 - 19:00
