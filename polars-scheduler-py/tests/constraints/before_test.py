import polars as pl
import pytest
from polars_scheduler import Scheduler


@pytest.mark.failing()
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
    scheduler = Scheduler(df)
    print(scheduler._df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )
    print(instances)
    # Supplement should be at least 1h after breakfast
    timings = instances.get_column("time_minutes") / 60
    assert timings.diff().drop_nulls().item() >= 1
