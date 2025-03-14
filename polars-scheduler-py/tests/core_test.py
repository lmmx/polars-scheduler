import polars as pl
from polars_scheduler import Scheduler


def test_namespace_existence():
    """Test that the scheduler namespace exists on DataFrame objects."""
    df = pl.DataFrame()
    assert hasattr(df, "scheduler")


def test_scheduler_methods():
    """Test that the scheduler namespace has the expected methods."""
    df = pl.DataFrame()
    assert hasattr(df.scheduler, "add")
    assert hasattr(df.scheduler, "create")


def test_empty_schedule():
    """Test scheduling from an empty schedule."""
    scheduler = Scheduler()
    result = scheduler.create()

    # Should return an empty DataFrame with expected schema
    assert isinstance(result, pl.DataFrame)
    assert result.height == 0
    print(result)
    assert "entity_name" in result.columns
    assert "instance" in result.columns
    assert "time_minutes" in result.columns
    assert "time_hhmm" in result.columns


def test_direct_construction():
    """Test creating a scheduler directly from a DataFrame."""
    # Create a simple DataFrame
    df = pl.DataFrame(
        {
            "Event": ["pill"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [[]],
            "Note": [None],
        },
    )

    # Create scheduler directly
    scheduler = Scheduler(df)
    result = scheduler.create()

    # Should have expected output
    assert result.height == 1
    assert result.filter(pl.col("entity_name") == "pill").height == 1


def test_plugin_api_works():
    """Test using the plugin API (add method)."""
    # Start with an empty schedule
    scheduler = Scheduler()

    # Add an event
    scheduler.add(
        event="pill",
        category="medication",
        unit="pill",
        frequency="1x daily",
    )

    # Schedule it
    result = scheduler.create()

    # Should have one pill event
    assert result.filter(pl.col("entity_name") == "pill").height == 1
