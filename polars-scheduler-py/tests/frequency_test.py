import pytest
import polars as pl
from polars_scheduler import SchedulerPlugin


def test_daily_frequency():
    """Test '1x daily' frequency."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["1x daily"],
        "Constraints": [[]],
        "Windows": [[]],
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule()
    
    # Should have exactly 1 instance
    instances = result.filter(pl.col("entity_name") == "pill")
    assert instances.height == 1


def test_multiple_daily_instances():
    """Test '2x daily' frequency."""
    df = pl.DataFrame({
        "Event": ["pill"],
        "Category": ["medication"],
        "Unit": ["pill"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["2x daily"],
        "Constraints": [[]],
        "Windows": [[]],
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule()
    
    # Should have exactly 2 instances
    instances = result.filter(pl.col("entity_name") == "pill")
    assert instances.height == 2


def test_weekly_frequency():
    """Test weekly frequency (3x weekly)."""
    df = pl.DataFrame({
        "Event": ["workout"],
        "Category": ["exercise"],
        "Unit": ["session"],
        "Amount": [None],
        "Divisor": [None],
        "Frequency": ["3x weekly"],
        "Constraints": [[]],
        "Windows": [[]],
        "Note": [None]
    })
    
    scheduler = SchedulerPlugin(df)
    result = scheduler.schedule()
    
    # Should have exactly 3 instances
    instances = result.filter(pl.col("entity_name") == "workout")
    assert instances.height == 3
    
    # Should have instance IDs 1, 2, and 3
    instance_ids = instances.select("instance").to_series().unique().sort()
    assert instance_ids.to_list() == [1, 2, 3]