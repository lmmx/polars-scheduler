import polars as pl
import polars_scheduler  # This registers the 'scheduler' namespace

# Create a new empty schedule
schedule = pl.DataFrame().scheduler.new()

# Add simple meal and medication schedule
schedule = schedule.scheduler.add(
    event="breakfast",
    category="meal",
    unit="serving",
    frequency="1x daily",
    windows=["08:00"],
)

schedule = schedule.scheduler.add(
    event="lunch",
    category="meal",
    unit="serving",
    frequency="1x daily",
    windows=["12:00-13:00"],
)

schedule = schedule.scheduler.add(
    event="dinner",
    category="meal",
    unit="serving",
    frequency="1x daily",
    windows=["18:00-20:00"],
)

schedule = schedule.scheduler.add(
    event="vitamin",
    category="supplement",
    unit="pill",
    frequency="1x daily",
    constraints=["≥1h after meal"],
)

schedule = schedule.scheduler.add(
    event="antibiotic",
    category="medication",
    unit="pill",
    frequency="2x daily",
    constraints=["≥6h apart", "≥1h before meal"],
)

schedule = schedule.scheduler.add(
    event="probiotic",
    category="supplement",
    unit="capsule",
    frequency="1x daily",
    constraints=["≥2h after antibiotic"],
)

schedule = schedule.scheduler.add(
    event="protein shake",
    category="supplement",
    unit="gram",
    amount=30,
    frequency="1x daily",
    constraints=["≥30m after gym OR with breakfast"],
    windows=["08:00", "17:00-19:00"],
    note="mix with 300ml water",
)

schedule = schedule.scheduler.add(
    event="ginger",
    category="supplement",
    unit="shot",
    frequency="1x daily",
    constraints=["before breakfast"],
)

schedule = schedule.scheduler.add(
    event="gym",
    category="exercise",
    unit="session",
    frequency="3x weekly",
)

# Print the original schedule
print("--- Original Schedule ---")
print(schedule)

# Generate an optimized schedule
result = schedule.scheduler.schedule(
    strategy="earliest",
    day_start="07:00",
    day_end="22:00",
)

# Print the optimized schedule
print("\n--- Optimized Schedule ---")
print(result.select(["Event", "Instance", "TimeHHMM"]))

# Try a "latest" schedule
result_latest = schedule.scheduler.schedule(
    strategy="latest",
    day_start="07:00",
    day_end="22:00",
)

# Print the optimized schedule
print("\n--- Latest Schedule ---")
print(result_latest.select(["Event", "Instance", "TimeHHMM"]))