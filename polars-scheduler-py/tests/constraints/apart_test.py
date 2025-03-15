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
    
    
def test_apart_constraint_with_different_intervals():
    """Test various time intervals for the apart constraint."""
    intervals = [4, 6, 8]  # Hours
    
    for interval in intervals:
        df = pl.DataFrame(
            {
                "Event": ["medication"],
                "Category": ["pill"],
                "Unit": ["tablet"],
                "Amount": [None],
                "Divisor": [None],
                "Frequency": ["2x daily"],
                "Constraints": [[f"≥{interval}h apart"]],
                "Windows": [[]],
                "Note": [None],
            },
        )

        scheduler = Scheduler(df)
        instances = scheduler.create(
            strategy="earliest",
            day_start="07:00",
            day_end="22:00",
            debug=True,  # Enable debugging
        )

        # Get times and check spacing
        times = instances.get_column("time_minutes").sort()
        gap = times.diff().drop_nulls().item()
        
        print(f"For {interval}h apart: First dose at {times[0]/60:.2f}h, Second dose at {times[1]/60:.2f}h")
        print(f"Gap: {gap/60:.2f}h (required: {interval}h)")
        
        assert gap >= (interval * 60), f"Expected gap of at least {interval}h, but got {gap/60:.2f}h"


def test_apart_constraint_with_more_instances():
    """Test apart constraint with more than 2 instances per day."""
    df = pl.DataFrame(
        {
            "Event": ["ibuprofen"],
            "Category": ["medication"],
            "Unit": ["pill"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["3x daily"],
            "Constraints": [["≥4h apart"]],
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

    # Should have 3 instances
    assert instances.height == 3

    # Get times and make sure each pair is at least 4h apart
    times = instances.filter(pl.col("entity_name") == "ibuprofen").get_column("time_minutes").sort()
    
    # Check if there are 3 times
    assert len(times) == 3, f"Expected 3 instances, got {len(times)}"
    
    # Check spacing between consecutive times
    for i in range(1, len(times)):
        gap = times[i] - times[i-1]
        assert gap >= (4 * 60), f"Gap between dose {i} and {i+1} is {gap/60:.2f}h, less than 4h"


def test_apart_constraint_with_earliest_strategy():
    """Test that earliest strategy spaces things out properly with the apart constraint."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["pill"],
            "Unit": ["tablet"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [["≥8h apart"]],
            "Windows": [[]],
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    instances = scheduler.create(
        strategy="earliest",
        day_start="06:00",  # Day starts at 6am
        day_end="22:00",    # Day ends at 10pm
    )
    
    # With earliest strategy and 8h apart, we expect:
    # First dose: 06:00 (as early as possible)
    # Second dose: 14:00 (8h later)
    
    times = instances.get_column("time_minutes").sort()
    
    # First dose should be at the start of the day (6:00 = 360 minutes)
    assert times[0] == 360, f"First dose should be at 06:00, but was at {times[0]/60:.2f}"
    
    # Second dose should be exactly 8h later (14:00 = 840 minutes)
    assert times[1] == 840, f"Second dose should be at 14:00, but was at {times[1]/60:.2f}"


def test_apart_constraint_with_latest_strategy():
    """Test that latest strategy spaces things out properly with the apart constraint."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["pill"],
            "Unit": ["tablet"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [["≥8h apart"]],
            "Windows": [[]],
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    instances = scheduler.create(
        strategy="latest",
        day_start="06:00",  # Day starts at 6am
        day_end="22:00",    # Day ends at 10pm
    )
    
    # With latest strategy and 8h apart, we expect:
    # Second dose: 22:00 (as late as possible)
    # First dose: 14:00 (8h earlier)
    
    times = instances.get_column("time_minutes").sort()
    
    # First dose should be 8h before end of day (14:00 = 840 minutes)
    assert times[0] == 840, f"First dose should be at 14:00, but was at {times[0]/60:.2f}"
    
    # Second dose should be at the end of the day (22:00 = 1320 minutes)
    assert times[1] == 1320, f"Second dose should be at 22:00, but was at {times[1]/60:.2f}"


def test_apart_constraint_with_window():
    """Test the interaction between 'apart' constraint and window preferences."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["pill"],
            "Unit": ["tablet"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [["≥8h apart"]],
            "Windows": [["08:00", "20:00"]],  # Preferred times are 8am and 8pm
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    instances = scheduler.create(
        strategy="earliest",
        day_start="06:00",
        day_end="22:00",
        penalty_weight=1000,  # High penalty to enforce windows
    )
    
    # With earliest strategy, windows at 08:00 and 20:00, and ≥8h apart:
    # First dose should be at 08:00
    # Second dose should be at 20:00 (12h later, which satisfies ≥8h)
    
    times = instances.get_column("time_hhmm").sort()
    
    assert times[0] == "08:00", f"First dose should be at 08:00, but was at {times[0]}"
    assert times[1] == "20:00", f"Second dose should be at 20:00, but was at {times[1]}"