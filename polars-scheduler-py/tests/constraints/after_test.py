import polars as pl
import pytest
from polars_scheduler import Scheduler


def test_after_constraint():
    """Test the '≥Xh after Y' constraint."""
    df = pl.DataFrame(
        {
            "Event": ["porridge", "creatine"],
            "Category": ["food", "supplement"],
            "Unit": ["meal", "capsule"],
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
        debug=True,
    )
    
    # Get event times
    porridge_time = instances.filter(pl.col("entity_name") == "porridge").get_column("time_minutes").item()
    creatine_time = instances.filter(pl.col("entity_name") == "creatine").get_column("time_minutes").item()
    
    # Convert to hours for easier debugging
    porridge_hour = porridge_time // 60
    creatine_hour = creatine_time // 60
    print("Porridge time: {}h, Creatine time: {}h".format(porridge_hour, creatine_hour))
    
    # Creatine should be at least 1h after porridge
    time_diff = creatine_time - porridge_time
    print("Time difference: {}h".format(time_diff // 60))
    
    assert time_diff >= 60  # at least 1 hour (60 minutes)
    
    
def test_after_constraint_with_category():
    """Test the '≥Xh after Category' constraint works with category reference."""
    df = pl.DataFrame(
        {
            "Event": ["breakfast", "lunch", "vitamin"],
            "Category": ["food", "food", "supplement"],
            "Unit": ["meal", "meal", "pill"],
            "Amount": [None, None, None],
            "Divisor": [None, None, None],
            "Frequency": ["1x daily", "1x daily", "1x daily"],
            "Constraints": [[], [], ["≥1h after food"]],  # Should be after ANY food
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
    breakfast_time = instances.filter(pl.col("entity_name") == "breakfast").get_column("time_minutes").item()
    lunch_time = instances.filter(pl.col("entity_name") == "lunch").get_column("time_minutes").item()
    vitamin_time = instances.filter(pl.col("entity_name") == "vitamin").get_column("time_minutes").item()
    
    # Vitamin should be after at least one meal with at least 60 min difference
    is_after_breakfast = vitamin_time - breakfast_time >= 60
    is_after_lunch = vitamin_time - lunch_time >= 60
    
    assert is_after_breakfast or is_after_lunch, "Vitamin should be at least 1h after one food item"
    
    
def test_after_constraint_multiple_daily():
    """Test that the 'after' constraint works properly when an event happens multiple times per day."""
    df = pl.DataFrame(
        {
            "Event": ["meal", "vitamin"],
            "Category": ["food", "supplement"],
            "Unit": ["meal", "pill"],
            "Amount": [None, None],
            "Divisor": [None, None],
            "Frequency": ["3x daily", "1x daily"],
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
    )
    
    # Get meal times - should be 3 instances but all one entity
    meal_times = instances.filter(pl.col("entity_name") == "meal").sort("instance").get_column("time_minutes")
    assert len(meal_times) == 3, "Expected 3 meal instances"
    
    # Get vitamin time
    vitamin_time = instances.filter(pl.col("entity_name") == "vitamin").get_column("time_minutes").item()
    
    # Vitamin should be after at least one meal instance
    is_after_any_meal = False
    for meal_time in meal_times:
        if vitamin_time - meal_time >= 60:  # 60 minutes = 1h
            is_after_any_meal = True
            break
    
    assert is_after_any_meal, "Vitamin should be at least 1h after at least one meal instance"