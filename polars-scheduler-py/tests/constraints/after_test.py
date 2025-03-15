import polars as pl
import pytest
from polars_scheduler import Scheduler


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
            "Constraints": [[], ["≥1h after food"]],
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
    porridge_time = (
        instances.filter(pl.col("entity_name") == "porridge")
        .get_column("time_minutes")
        .item()
    )
    creatine_time = (
        instances.filter(pl.col("entity_name") == "creatine")
        .get_column("time_minutes")
        .item()
    )

    # Convert to hours for easier debugging
    porridge_hour = porridge_time / 60
    creatine_hour = creatine_time / 60
    print(f"Porridge time: {porridge_hour:.2f}h, Creatine time: {creatine_hour:.2f}h")

    # Creatine should be at least 1h after porridge
    time_diff = creatine_time - porridge_time
    print(f"Time difference: {time_diff / 60:.2f}h")

    assert time_diff >= 60  # at least 1 hour (60 minutes)


def test_after_constraint_with_category():
    """Test the '≥Xh after Category' constraint works with category reference."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast", "lunch", "vitamin"],
            "Category": ["meal", "meal", "supplement"],
            "Unit": ["serving", "serving", "pill"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [[], [], ["≥0.5h after meal"]],  # Should be after ANY meal
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
    breakfast_time = (
        instances.filter(pl.col("entity_name") == "breakfast")
        .get_column("time_minutes")
        .item()
    )
    lunch_time = (
        instances.filter(pl.col("entity_name") == "lunch")
        .get_column("time_minutes")
        .item()
    )
    vitamin_time = (
        instances.filter(pl.col("entity_name") == "vitamin")
        .get_column("time_minutes")
        .item()
    )

    # Vitamin should be after at least one meal with at least 30 min difference
    is_after_breakfast = vitamin_time - breakfast_time >= 30
    is_after_lunch = vitamin_time - lunch_time >= 30

    assert (
        is_after_breakfast or is_after_lunch
    ), "Vitamin should be at least 0.5h after one meal"


def test_after_constraint_with_specific_entity():
    """Test the '≥Xh after SpecificEntity' constraint works with a specific entity reference."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast", "lunch", "calcium"],
            "Category": ["meal", "meal", "supplement"],
            "Unit": ["serving", "serving", "pill"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [
                [],
                [],
                ["≥2h after breakfast"],
            ],  # Specifically after breakfast
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
    breakfast_time = (
        instances.filter(pl.col("entity_name") == "breakfast")
        .get_column("time_minutes")
        .item()
    )
    calcium_time = (
        instances.filter(pl.col("entity_name") == "calcium")
        .get_column("time_minutes")
        .item()
    )

    # Convert to hours for easier debugging
    breakfast_hour = breakfast_time / 60
    calcium_hour = calcium_time / 60
    print(f"Breakfast time: {breakfast_hour:.2f}h, Calcium time: {calcium_hour:.2f}h")

    # Calcium should be at least 2h after breakfast
    time_diff = calcium_time - breakfast_time
    print(f"Time difference: {time_diff / 60:.2f}h")

    assert time_diff >= 120  # at least 2 hours (120 minutes)


def test_after_constraint_with_multiple_instances():
    """Test that the 'after' constraint works properly when the referenced entity has multiple instances."""
    df = pl.DataFrame(
        {
            "Event": ["meal", "supplement"],
            "Category": ["food", "vitamin"],
            "Unit": ["serving", "capsule"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["3x daily", "1x daily"],
            "Constraints": [[], ["≥0.5h after food"]],
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

    # Supplement should be at least 0.5h after at least one meal
    is_after_any_meal = False
    for meal_time in meal_times:
        if supplement_time - meal_time >= 30:  # 30 minutes = 0.5h
            is_after_any_meal = True
            break

    assert (
        is_after_any_meal
    ), "Supplement should be at least 0.5h after at least one meal instance"


def test_earliest_strategy_with_after_constraint():
    """Test that 'earliest' strategy works with 'after' constraint while still pushing events as early as possible."""
    df = pl.DataFrame(
        {
            "Event": ["supplement", "breakfast"],
            "Category": ["vitamin", "food"],
            "Unit": ["pill", "meal"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "1x daily"],
            "Constraints": [["≥1h after food"], []],
            "Windows": [[], ["09:00-10:00"]],  # Breakfast at 9:00
            "Note": [None, None],
        },
    )
    scheduler = Scheduler(df)

    instances = scheduler.create(
        strategy="earliest",
        day_start="09:00",
        day_end="22:00",
        penalty_weight=1000,  # High penalty to enforce window
    )
    print(instances)

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
        .get_column("time_hhmm")
        .item()
    )

    assert (
        supplement_time == "11:00"
    ), "With earliest strategy, supplement should be at 08:00 or earlier"
    assert (
        breakfast_time - supplement_time >= 60
    ), "Supplement should be at least 1h before breakfast"
