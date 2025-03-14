import polars as pl
import pytest
from polars_scheduler import Scheduler


@pytest.mark.failing(reason="Schedules both at 7am")
def test_earliest_strategy():
    """Test the 'earliest' scheduling strategy."""
    df = pl.DataFrame(
        {
            "Event": ["pill"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["08:00-20:00"]],  # Wide window
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest", day_start="07:00", day_end="22:00")

    # With earliest strategy, should be at start of window
    pill_time = (
        result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    )
    assert pill_time == 480  # 08:00 (start of window)


@pytest.mark.failing(reason="Schedules both at 7am")
def test_latest_strategy():
    """Test the 'latest' scheduling strategy."""
    df = pl.DataFrame(
        {
            "Event": ["pill"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["08:00-20:00"]],  # Wide window
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    result = scheduler.create(strategy="latest", day_start="07:00", day_end="22:00")

    # With latest strategy, should be at end of window
    pill_time = (
        result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    )
    assert pill_time == 1200  # 20:00 (end of window)
