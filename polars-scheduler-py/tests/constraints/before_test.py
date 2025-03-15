import polars as pl
import pytest
from polars_scheduler import Scheduler


def test_before_constraint():
    """Test the '≥Xh before Y' constraint."""
    df = pl.DataFrame(
        {
            "Event": ["creatine", "porridge"],
            "Category": ["supplement", "food"],
            "Unit": ["capsule", "serving"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "1x daily"],
            "Constraints": [["≥1h before food"], []],
            "Windows": [[], []],
            "Note": [None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
        debug=True,  # Enable debug output
    )

    # Get event times
    creatine_time = (
        instances.filter(pl.col("entity_name") == "creatine")
        .get_column("time_minutes")
        .item()
    )
    porridge_time = (
        instances.filter(pl.col("entity_name") == "porridge")
        .get_column("time_minutes")
        .item()
    )

    # Convert to hours for easier debugging
    creatine_hour = creatine_time / 60
    porridge_hour = porridge_time / 60
    print(f"Creatine time: {creatine_hour:.2f}h, Porridge time: {porridge_hour:.2f}h")

    # Creatine should be at least 1h before porridge
    time_diff = porridge_time - creatine_time
    print(f"Time difference: {time_diff / 60:.2f}h")

    assert (
        time_diff >= 60
    ), f"Expected creatine to be at least 1h before porridge, but difference was {time_diff/60:.2f}h"


def test_before_constraint_with_category():
    """Test the '≥Xh before Category' constraint works with category reference."""
    df = pl.DataFrame(
        {
            "Event": ["preworkout", "lunch", "dinner"],
            "Category": ["supplement", "meal", "meal"],
            "Unit": ["serving", "serving", "serving"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [["≥0.5h before meal"], [], []],  # Should be before ANY meal
            "Windows": [[], [], []],
            "Note": [None, None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )

    # Get event times
    preworkout_time = (
        instances.filter(pl.col("entity_name") == "preworkout")
        .get_column("time_minutes")
        .item()
    )
    lunch_time = (
        instances.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    )
    dinner_time = (
        instances.filter(pl.col("entity_name") == "dinner")
        .get_column("time_minutes")
        .item()
    )

    # Preworkout should be before at least one meal with at least 30 min difference
    is_before_lunch = lunch_time - preworkout_time >= 30
    is_before_dinner = dinner_time - preworkout_time >= 30

    assert (
        is_before_lunch or is_before_dinner
    ), "Preworkout should be at least 0.5h before one meal"


def test_before_constraint_with_specific_entity():
    """Test the '≥Xh before SpecificEntity' constraint works with a specific entity reference."""
    df = pl.DataFrame(
        {
            "Event": ["caffeine", "breakfast", "lunch"],
            "Category": ["supplement", "meal", "meal"],
            "Unit": ["pill", "serving", "serving"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [
                ["≥0.25h before breakfast"],
                [],
                [],
            ],  # Specifically before breakfast
            "Windows": [[], [], []],
            "Note": [None, None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )

    # Get event times
    caffeine_time = (
        instances.filter(pl.col("entity_name") == "caffeine")
        .get_column("time_minutes")
        .item()
    )
    breakfast_time = (
        instances.filter(pl.col("entity_name") == "breakfast")
        .get_column("time_minutes")
        .item()
    )

    # Convert to hours for easier debugging
    caffeine_hour = caffeine_time / 60
    breakfast_hour = breakfast_time / 60
    print(f"Caffeine time: {caffeine_hour:.2f}h, Breakfast time: {breakfast_hour:.2f}h")

    # Caffeine should be at least 0.25h before breakfast
    time_diff = breakfast_time - caffeine_time
    print(f"Time difference: {time_diff / 60:.2f}h")

    assert time_diff >= 15  # at least 0.25 hours (15 minutes)


def test_before_constraint_with_multiple_instances():
    """Test that the 'before' constraint works properly when the referenced entity has multiple instances."""
    df = pl.DataFrame(
        {
            "Event": ["supplement", "meal"],
            "Category": ["vitamin", "food"],
            "Unit": ["capsule", "serving"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "3x daily"],
            "Constraints": [["≥0.5h before food"], []],
            "Windows": [[], []],
            "Note": [None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="07:00",
        day_end="22:00",
    )

    # Get meal times (should be 3)
    meal_times = (
        instances.filter(pl.col("entity_name") == "meal")
        .get_column("time_minutes")
        .sort()
    )

    # Get supplement time (should be 1)
    supplement_time = (
        instances.filter(pl.col("entity_name") == "supplement")
        .get_column("time_minutes")
        .item()
    )

    # Supplement should be at least 0.5h before at least one meal
    is_before_any_meal = False
    for meal_time in meal_times:
        if meal_time - supplement_time >= 30:  # 30 minutes = 0.5h
            is_before_any_meal = True
            break

    assert (
        is_before_any_meal
    ), "Supplement should be at least 0.5h before at least one meal instance"


def test_earliest_strategy_with_before_constraint():
    """Test that 'earliest' strategy works with 'before' constraint while still pushing events as early as possible."""
    df = pl.DataFrame(
        {
            "Event": ["supplement", "breakfast"],
            "Category": ["vitamin", "meal"],
            "Unit": ["pill", "serving"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "1x daily"],
            "Constraints": [["≥1h before breakfast"], []],
            "Windows": [[], ["09:00"]],  # Breakfast at 9:00
            "Note": [None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="06:00",
        day_end="22:00",
        penalty_weight=1000,  # High penalty to enforce window
    )

    # Breakfast should be at 9:00 (540 minutes)
    breakfast_time = (
        instances.filter(pl.col("entity_name") == "breakfast")
        .get_column("time_minutes")
        .item()
    )
    assert breakfast_time == 540, "Breakfast should be at 09:00 (540 minutes)"

    # Supplement should be at least 1h before breakfast, and as early as possible
    # So expected time would be 8:00 (480 minutes) or earlier
    supplement_time = (
        instances.filter(pl.col("entity_name") == "supplement")
        .get_column("time_minutes")
        .item()
    )

    assert (
        supplement_time <= 480
    ), "With earliest strategy, supplement should be at 08:00 or earlier"
    assert (
        breakfast_time - supplement_time >= 60
    ), "Supplement should be at least 1h before breakfast"
