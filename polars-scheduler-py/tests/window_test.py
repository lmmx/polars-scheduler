import polars as pl
import pytest
from polars_scheduler import SchedulerPlugin


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
        }
    )

    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="earliest")

    breakfast = result.filter(pl.col("entity_name") == "breakfast")
    assert breakfast.select("time_hhmm").item() == "08:00"


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
        }
    )

    scheduler = SchedulerPlugin(df)

    # With earliest strategy
    earliest = scheduler.schedule(strategy="earliest")
    lunch_time = (
        earliest.filter(pl.col("entity_name") == "lunch").select("time_minutes").item()
    )
    assert lunch_time == 720  # 12:00

    # With latest strategy
    latest = scheduler.schedule(strategy="latest")
    lunch_time = (
        latest.filter(pl.col("entity_name") == "lunch").select("time_minutes").item()
    )
    assert lunch_time == 780  # 13:00


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
            "Windows": [["08:00", "17:00-19:00"]],
            "Note": [None],
        }
    )

    scheduler = SchedulerPlugin(df)

    # With earliest strategy, should pick first window
    earliest = scheduler.schedule(strategy="earliest")
    shake_time = (
        earliest.filter(pl.col("entity_name") == "shake").select("time_minutes").item()
    )
    assert shake_time == 480  # 08:00

    # With latest strategy, should pick last window
    latest = scheduler.schedule(strategy="latest")
    shake_time = (
        latest.filter(pl.col("entity_name") == "shake").select("time_minutes").item()
    )
    assert shake_time == 1140  # 19:00
