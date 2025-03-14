import polars as pl
from polars_scheduler import Scheduler


def test_apart_constraint():
    """Test the '≥Xh apart' constraint."""
    df = pl.DataFrame(
        {
            "Event": ["paracetamol"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [["≥6h apart"]],
            "Windows": [[]],
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )

    # Should have 2 instances
    assert instances.height == 2

    # Get times and make sure they're at least 6h apart
    times = instances.get_column("time_minutes").sort()
    gap = times.diff().drop_nulls().item()
    assert gap >= (6 * 60)  # 6 hours = 360 minutes
