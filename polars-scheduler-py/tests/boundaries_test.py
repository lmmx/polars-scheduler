import polars as pl
from polars_scheduler import SchedulerPlugin


def test_day_boundaries():
    """Test day_start and day_end parameters."""
    df = pl.DataFrame(
        {
            "Event": ["pill"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [[]],  # No specific window
            "Note": [None],
        },
    )

    scheduler = SchedulerPlugin(df)

    # With earliest strategy, should be at day_start
    result = scheduler.schedule(strategy="earliest", day_start="09:00", day_end="21:00")
    pill_time = (
        result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    )
    assert pill_time == 540  # 09:00 (day_start)

    # With latest strategy, should be at day_end
    result = scheduler.schedule(strategy="latest", day_start="09:00", day_end="21:00")
    pill_time = (
        result.filter(pl.col("entity_name") == "pill").select("time_minutes").item()
    )
    assert pill_time == 1260  # 21:00 (day_end)
