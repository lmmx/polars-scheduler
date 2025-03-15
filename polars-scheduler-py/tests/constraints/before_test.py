import polars as pl
import pytest
from polars_scheduler import Scheduler


def test_before_constraint():
    """Test the '≥Xh before Y' constraint."""
    df = pl.DataFrame(
        {
            "Event": ["creatine", "porridge"],
            "Category": ["supplement", "food"],
            "Unit": ["capsule", "meal"],
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
        debug=True,
    )
    
    # Get event times
    creatine_time = instances.filter(pl.col("entity_name") == "creatine").get_column("time_minutes").item()
    porridge_time = instances.filter(pl.col("entity_name") == "porridge").get_column("time_minutes").item()
    
    # Convert to hours for easier debugging
    creatine_hour = creatine_time // 60
    porridge_hour = porridge_time // 60
    print("Creatine time: {}h, Porridge time: {}h".format(creatine_hour, porridge_hour))
    
    # Creatine should be at least 1h before porridge
    time_diff = porridge_time - creatine_time
    print("Time difference: {}h".format(time_diff // 60))
    
    assert time_diff >= 60, "Expected creatine to be at least 1h before porridge"


def test_before_constraint_with_category():
    """Test the '≥Xh before Category' constraint works with category reference."""
    df = pl.DataFrame(
        {
            "Event": ["preworkout", "lunch", "dinner"],
            "Category": ["supplement", "food", "food"],
            "Unit": ["scoop", "meal", "meal"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [["≥1h before food"], [], []],  # Should be before ANY food
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
    preworkout_time = instances.filter(pl.col("entity_name") == "preworkout").get_column("time_minutes").item()
    lunch_time = instances.filter(pl.col("entity_name") == "lunch").get_column("time_minutes").item()
    dinner_time = instances.filter(pl.col("entity_name") == "dinner").get_column("time_minutes").item()
    
    # Preworkout should be before at least one meal with at least 60 min difference
    is_before_lunch = lunch_time - preworkout_time >= 60
    is_before_dinner = dinner_time - preworkout_time >= 60
    
    assert is_before_lunch or is_before_dinner, "Preworkout should be at least 1h before one food item"


def test_before_constraint_multiple_daily():
    """Test that the 'before' constraint works properly when an event happens multiple times per day."""
    df = pl.DataFrame(
        {
            "Event": ["preworkout", "meal"],
            "Category": ["supplement", "food"],
            "Unit": ["scoop", "meal"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["1x daily", "3x daily"],
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
    )
    
    # Get meal times - should be 3 instances but all one entity
    meal_times = instances.filter(pl.col("entity_name") == "meal").sort("instance").get_column("time_minutes")
    assert len(meal_times) == 3, "Expected 3 meal instances"
    
    # Get preworkout time
    preworkout_time = instances.filter(pl.col("entity_name") == "preworkout").get_column("time_minutes").item()
    
    # Preworkout should be before at least one meal instance
    is_before_any_meal = False
    for meal_time in meal_times:
        if meal_time - preworkout_time >= 60:  # 60 minutes = 1h
            is_before_any_meal = True
            break
    
    assert is_before_any_meal, "Preworkout should be at least 1h before at least one meal instance"


def test_earliest_strategy_with_before_constraint():
    """Test that 'earliest' strategy works with 'before' constraint while still pushing events as early as possible."""
    df = pl.DataFrame(
        {
            "Event": ["supplement", "breakfast"],
            "Category": ["supplement", "food"],
            "Unit": ["pill", "meal"],
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
    breakfast_time = instances.filter(pl.col("entity_name") == "breakfast").get_column("time_minutes").item()
    assert breakfast_time == 540, "Breakfast should be at 09:00 (540 minutes)"
    
    # Supplement should be at least 1h before breakfast, and as early as possible
    # So expected time would be 8:00 (480 minutes) or earlier
    supplement_time = instances.filter(pl.col("entity_name") == "supplement").get_column("time_minutes").item()
    
    assert supplement_time <= 480, "With earliest strategy, supplement should be at 08:00 or earlier"
    assert breakfast_time - supplement_time >= 60, "Supplement should be at least 1h before breakfast"