import pytest
import polars as pl
from polars_scheduler import SchedulerPlugin


def test_apart_constraint():
    """Test the '≥Xh apart' constraint."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["2x daily"],
        "Constraints": [["≥6h apart"]],
        "Windows": [[]],
        "Note": [None]
    })

    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="earliest", day_start="07:00", day_end="22:00")

    # Should have 2 instances
    instances = result.filter(pl.col("entity_name") == "pill")
    assert instances.height == 2

    # Get times and make sure they're at least 6h apart
    times = instances.select("time_minutes").to_series().sort()
    assert times[1] - times[0] >= 360  # 6 hours = 360 minutes


def test_before_constraint():
    """Test the '≥Xh before Y' constraint."""
    df = pl.DataFrame({
        "Event": ["breakfast", "pill"],
        "Category": ["meal", "medication"],
        "Unit": ["serving", "pill"],
        "Amount": [None, None],
        "Divisor": [None, None],
        "Frequency": ["1x daily", "1x daily"],
        "Constraints": [[], ["≥1h before food"]],
        "Windows": [["08:00"], []],
        "Note": [None, None]
    })

    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="earliest", day_start="07:00", day_end="22:00")

    breakfast_time = result.filter(pl.col("entity_name") == "breakfast").select("time_minutes").item()
    pill_time = result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()

    # Pill should be at least 1h before breakfast
    assert breakfast_time - pill_time >= 60  # 1 hour = 60 minutes


def test_after_constraint():
    """Test the '≥Xh after Y' constraint."""
    df = pl.DataFrame({
        "Event": ["breakfast", "vitamin"],
        "Category": ["meal", "supplement"],
        "Unit": ["serving", "pill"],
        "Amount": [None, None],
        "Divisor": [None, None],
        "Frequency": ["1x daily", "1x daily"],
        "Constraints": [[], ["≥1h after meal"]],
        "Windows": [["08:00"], []],
        "Note": [None, None]
    })

    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="earliest", day_start="07:00", day_end="22:00")

    breakfast_time = result.filter(pl.col("entity_name") == "breakfast").select("time_minutes").item()
    vitamin_time = result.filter(pl.col("entity_name") == "vitamin").select("time_minutes").item()

    # Vitamin should be at least 1h after breakfast
    assert vitamin_time - breakfast_time >= 60  # 1 hour = 60 minutes
