import polars as pl
import pytest
from polars_scheduler import Scheduler


@pytest.mark.failing()
def test_exact_time_window():
    """Test scheduling with exact time window (e.g., '08:30')."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast"],
            "Category": ["meal"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["08:30-09:30"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    result = scheduler.create(strategy="earliest")  # penalty_weight=1.0
    breakfast = result.filter(pl.col("entity_name") == "breakfast")
    assert breakfast.get_column("time_hhmm").item() == "08:30"


@pytest.mark.failing()
def test_range_time_window():
    """Test scheduling with a time range window (e.g., '12:00-13:00')."""
    df = pl.DataFrame(
        {
            "Event": ["lunch"],
            "Category": ["meal"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["12:00-13:00"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    # With earliest strategy
    earliest = scheduler.create(strategy="earliest")
    lunch_time = (
        earliest.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 12 <= lunch_time == 13  # 12:00 - 13:00
    # With latest strategy
    latest = scheduler.create(strategy="latest")
    lunch_time = (
        latest.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 12 <= lunch_time <= 13  # 12:00 - 13:00


@pytest.mark.failing()
def test_multiple_windows():
    """Test scheduling with multiple windows."""
    df = pl.DataFrame(
        {
            "Event": ["shake"],
            "Category": ["supplement"],
            "Unit": ["serving"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["17:00-19:00"]],
            "Note": [None],
        },
    )
    scheduler = Scheduler(df)
    # With earliest strategy, should pick first window
    earliest = scheduler.create(strategy="earliest")
    shake_time = (
        earliest.filter(pl.col("entity_name") == "shake")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert shake_time == 17  # 17:00
    # With latest strategy, should pick last window
    latest = scheduler.create(strategy="latest")
    shake_time = (
        latest.filter(pl.col("entity_name") == "shake")
        .get_column("time_minutes")
        .item()
    ) / 60
    assert 17 <= shake_time <= 19  # 17:00 - 19:00


@pytest.mark.parametrize(
    "strategy,hhmm_1",
    [
        ("earliest", "12:00"),
        ("latest", "13:00"),
    ],
)
def test_one_meal_window_usage(strategy, hhmm_1):
    """Test that scheduling respects time ranges."""
    # Bug report in issue #20
    df = pl.DataFrame(
        {
            "Event": ["Chicken and rice"],
            "Category": ["food"],
            "Unit": ["meal"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["1x daily"],
            "Constraints": [[]],
            "Windows": [["12:00-13:00"]],  # Range
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    result = scheduler.create(strategy=strategy, debug=True, penalty_weight=1000)
    print(result)
    assert result.height == 1
    # First instance should be at 12:00 (lower bound) with earliest strategy
    # First instance should be at 13:00 (upper bound) with latest strategy
    first = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first.get_column("time_hhmm").item() == hhmm_1


@pytest.mark.parametrize(
    "strategy,hhmm_1,hhmm_2",
    [
        ("earliest", "08:00", "18:00"),
        ("latest", "09:00", "20:00"),
    ],
)
def test_two_meal_window_usage(strategy, hhmm_1, hhmm_2):
    """Test that scheduling respects both anchor points and time ranges."""
    df = pl.DataFrame(
        {
            "Event": ["Chicken and rice"],
            "Category": ["food"],
            "Unit": ["meal"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [[]],
            "Windows": [["08:00-09:00", "18:00-20:00"]],  # Two ranges
            "Note": [None],
        },
    )

    # Test with strategy
    scheduler = Scheduler(df)
    print(scheduler._df)
    result = scheduler.create(
        strategy=strategy,
        day_start="06:00",
        day_end="22:00",
        debug=True,
    )
    print(result)
    # First instance should be at 08:00 (lower point) with strategy earliest
    # First instance should be at 09:00 (upper bound) with strategy latest
    first = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 1),
    )
    assert first.get_column("time_hhmm").item() == hhmm_1

    # Second instance should be at start of range (18:00) with earliest strategy
    # Second instance should be at  end  of range (20:00) with  latest  strategy
    second = result.filter(
        (pl.col("entity_name") == "Chicken and rice") & (pl.col("instance") == 2),
    )
    assert second.get_column("time_hhmm").item() == hhmm_2


def test_instance_order():
    """
    Attempts to reproduce a 'flipped instance' scenario for a 2x-daily meal
    with 2 partially overlapping windows, under a 'latest' strategy.
    If there's no forced anchor or ordering, the solver may label
    the later slot as #1 and the earlier slot as #2.
    """
    # Bug report in issue #19
    df = pl.DataFrame(
        {
            "Event": ["Chicken and rice"],
            "Category": ["food"],
            "Unit": ["meal"],
            "Amount": [None],
            "Divisor": [None],
            "Frequency": ["2x daily"],
            "Constraints": [[]],
            # Two partial-range windows, no single anchor
            "Windows": [["08:30-09:30", "18:00-20:00"]],
            "Note": [None],
        },
    )

    scheduler = Scheduler(df)
    result = scheduler.create(
        strategy="latest",
        day_start="06:00",
        day_end="22:00",
        debug=True,
    )

    print(
        result,
    )  # This is where you'll often see #1 => 20:00, #2 => 09:30 in some environments

    # If the solver picks the "pathological" labeling, the next assertion fails
    # because instance #1 (time_minutes_1) is actually greater than instance #2 (time_minutes_2).
    times = (
        result.filter(pl.col("entity_name") == "Chicken and rice")
        .select(["instance", "time_minutes"])
        .sort("instance")
    )
    time_inst1 = times.row(0)[1]
    time_inst2 = times.row(1)[1]

    assert time_inst1 < time_inst2, (
        f"Saw the 'flipped' scenario: instance #1 => {time_inst1}, instance #2 => {time_inst2}"
    )
