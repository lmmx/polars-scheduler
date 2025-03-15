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

    # Should have 2 instances of the same entity
    assert instances.height == 2
    assert instances.get_column("entity_name").unique().to_list() == ["paracetamol"]
    assert instances.get_column("instance").to_list() == [1, 2]

    # Get times and make sure they're at least 6h apart
    times = instances.sort("instance").get_column("time_minutes")
    gap = times[1] - times[0]
    assert gap >= (6 * 60), "Expected gap of at least 6h"


def test_apart_constraint_with_different_intervals():
    """Test various time intervals for the apart constraint."""
    intervals = [4, 6, 8]  # Hours

    for interval in intervals:
        df = pl.DataFrame(
            {
                "Event": ["medication"],
                "Category": ["medication"],
                "Unit": ["pill"],
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
            debug=True,
        )

        # Get times in order of instance
        times = instances.sort("instance").get_column("time_minutes")
        gap = times[1] - times[0]

        print(
            "For {}h apart: First dose at {}h, Second dose at {}h".format(
                interval,
                times[0] // 60,
                times[1] // 60,
            ),
        )
        print(f"Gap: {gap // 60}h (required: {interval}h)")

        assert gap >= (interval * 60), f"Expected gap of at least {interval}h"


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

    # Should have 3 instances of the same entity
    assert instances.height == 3
    assert instances.get_column("entity_name").unique().to_list() == ["ibuprofen"]
    instance_list = instances.get_column("instance").to_list()
    instance_list.sort()
    assert instance_list == [1, 2, 3], (
        f"Expected instances 1, 2, 3 but got {instance_list}"
    )

    # Get times in ascending order by instance
    times = instances.sort("instance").get_column("time_minutes")

    # Check spacing between consecutive times
    for i in range(1, len(times)):
        gap = times[i] - times[i - 1]
        assert gap >= (4 * 60), (
            "Gap between instance {} and {} is {}h, less than 4h".format(
                i,
                i + 1,
                gap // 60,
            )
        )


def test_apart_constraint_with_earliest_strategy():
    """Test that earliest strategy spaces things out properly with the apart constraint."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["medication"],
            "Unit": ["pill"],
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
        day_end="22:00",  # Day ends at 10pm
    )

    # With earliest strategy and 8h apart, we expect:
    # First dose: 06:00 (as early as possible)
    # Second dose: 14:00 (8h later)

    times = instances.sort("instance").get_column("time_minutes")

    # First dose should be at the start of the day (6:00 = 360 minutes)
    assert times[0] == 360, (
        f"First dose should be at 06:00, but was at {times[0] // 60}"
    )

    # Second dose should be exactly 8h later (14:00 = 840 minutes)
    assert times[1] == 840, (
        f"Second dose should be at 14:00, but was at {times[1] // 60}"
    )


def test_apart_constraint_with_latest_strategy():
    """Test that latest strategy spaces things out properly with the apart constraint."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["medication"],
            "Unit": ["pill"],
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
        day_end="22:00",  # Day ends at 10pm
    )

    # With latest strategy and 8h apart, we expect:
    # Second dose: 22:00 (as late as possible)
    # First dose: 14:00 (8h earlier)

    times = instances.sort("instance").get_column("time_minutes")

    # First dose should be 8h before end of day (14:00 = 840 minutes)
    assert times[0] == 840, (
        f"First dose should be at 14:00, but was at {times[0] // 60}"
    )

    # Second dose should be at the end of the day (22:00 = 1320 minutes)
    assert times[1] == 1320, (
        f"Second dose should be at 22:00, but was at {times[1] // 60}"
    )


def test_apart_constraint_with_window():
    """Test the interaction between 'apart' constraint and window preferences."""
    df = pl.DataFrame(
        {
            "Event": ["medicine"],
            "Category": ["medication"],
            "Unit": ["pill"],
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

    times = instances.sort("instance").get_column("time_hhmm")

    assert times[0] == "08:00", f"First dose should be at 08:00, but was at {times[0]}"
    assert times[1] == "20:00", f"Second dose should be at 20:00, but was at {times[1]}"
