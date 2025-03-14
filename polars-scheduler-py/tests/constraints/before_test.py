import polars as pl
import pytest
from polars_scheduler import Scheduler


@pytest.mark.failing(reason="Schedules both at 7am")
def test_after_constraint():
    """Test the '≥Xh after Y' constraint."""
    df = pl.DataFrame(
        {
            "Event": ["porridge", "creatine"],
            "Category": ["food", "supplement"],
            "Unit": ["serving", "capsule"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "1x daily"],
            "Constraints": [[], ["≥1h before food"]],
            "Windows": [[], []],
            "Note": [None, None],
        },
    )
    print(df)

    scheduler = Scheduler(df)
    instances = scheduler.schedule(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )
    print(instances)
    # Supplement should be at least 1h after breakfast
    timings = instances.get_column("time_minutes")
    assert timings.diff().drop_nulls().item() >= (1 * 60)
