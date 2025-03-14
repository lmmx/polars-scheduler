import polars as pl
import pytest
from polars_scheduler import Scheduler


def test_exact_time_window():
    """Test scheduling with exact time window (e.g., '08:30')."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast"],
            "Category": ["meal"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["08:30"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest")
    breakfast = result.filter(pl.col("entity_name") == "breakfast")
    assert breakfast.get_column("time_hhmm").item() == "08:30"


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


def test_one_meal_window_usage():
    """Test that scheduling respects time ranges."""
    df = pl.DataFrame(
        {
            "Event": ["Chicken and rice"],
            "Category": ["food"],
            "Unit": ["meal"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [[]],
            "Windows": [["12:00-13:00"]],  # Range
            "Note": [None],
        },
    )

    # Test with earliest strategy
    # First instance should be scheduled at 12:00 (lower bound)
    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest")
    assert result.height == 2
    first = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first.get_column("time_hhmm").item() == "12:00"
    # Test with latest strategy
    # First instance should be at 13:00 (upper bound)
    result_latest = scheduler.create(strategy="latest")
    first_latest = result_latest.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first_latest.get_column("time_hhmm").item() == "13:00"


def test_two_meal_window_usage():
    """Test that scheduling respects both anchor points and time ranges."""
    df = pl.DataFrame(
        {
            "Event": ["Chicken and rice"],
            "Category": ["food"],
            "Unit": ["meal"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [[]],
            "Windows": [["08:30-09:30", "18:00-20:00"]],  # Two ranges
            "Note": [None],
        },
    )

    # Test with earliest strategy
    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest")
    # First instance should be scheduled at 08:00 (anchor point)
    first = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first.get_column("time_hhmm").item() == "08:30"
    # Second instance should be at start of range (18:00) with earliest strategy
    second = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 2),
    )
    assert second.get_column("time_hhmm").item() == "18:00"
    # Test with latest strategy
    result_latest = scheduler.create(strategy="latest")
    # First instance should be at 09:30 (upper bound)
    first_latest = result_latest.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first_latest.get_column("time_hhmm").item() == "09:30"
    # Second instance should be at end of range (20:00) with latest strategy
    second_latest = result_latest.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 2),
    )
    assert second_latest.get_column("time_hhmm").item() == "20:00"
