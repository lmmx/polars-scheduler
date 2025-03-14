import pytest
import polars as pl
from polars_scheduler import SchedulerPlugin


def test_earliest_strategy():
    """Test the 'earliest' scheduling strategy."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["1x daily"],
        "Constraints": [[]],
        "Windows": [["08:00-20:00"]],  # Wide window
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="earliest", day_start="07:00", day_end="22:00")
    
    # With earliest strategy, should be at start of window
    pill_time = result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    assert pill_time == 480  # 08:00 (start of window)


def test_latest_strategy():
    """Test the 'latest' scheduling strategy."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["1x daily"],
        "Constraints": [[]],
        "Windows": [["08:00-20:00"]],  # Wide window
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule(strategy="latest", day_start="07:00", day_end="22:00")
    
    # With latest strategy, should be at end of window
    pill_time = result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    assert pill_time == 1200  # 20:00 (end of window)


def test_day_boundaries():
    """Test day_start and day_end parameters."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["1x daily"],
        "Constraints": [[]],
        "Windows": [[]],  # No specific window
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    
    # With earliest strategy, should be at day_start
    result = scheduler.schedule(strategy="earliest", day_start="09:00", day_end="21:00")
    pill_time = result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    assert pill_time == 540  # 09:00 (day_start)
    
    # With latest strategy, should be at day_end
    result = scheduler.schedule(strategy="latest", day_start="09:00", day_end="21:00")
    pill_time = result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    assert pill_time == 1260  # 21:00 (day_end)