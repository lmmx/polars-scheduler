from polars_scheduler import Scheduler

# Create a new empty schedule
scheduler = Scheduler()

# Add simple meal and medication schedule
scheduler.add(
    event="chicken",
    category="food",
    unit="meal",
    frequency="3x daily",
    windows=["08:00", "12:00-14:00", "19:00"],
)

scheduler.add(
    event="vitamin",
    category="supplement",
    unit="pill",
    frequency="1x daily",
    constraints=["≥1h after food"],
)

scheduler.add(
    event="antibiotic",
    category="medication",
    unit="pill",
    frequency="2x daily",
    constraints=["≥6h apart", "≥1h before food"],
)

scheduler.add(
    event="probiotic",
    category="supplement",
    unit="capsule",
    frequency="1x daily",
    constraints=["≥2h after antibiotic"],
)

scheduler.add(
    event="protein shake",
    category="supplement",
    unit="gram",
    amount=30,
    frequency="1x daily",
    constraints=[],
    windows=["11:00"],
    note="mix with 300ml water",
)

scheduler.add(
    event="ginger",
    category="supplement",
    unit="shot",
    frequency="1x daily",
    constraints=["≥1h before food"],
)

# Print the original schedule
print("--- Schedule Constraints ---")
print(scheduler._df)

# Generate an optimized schedule (Earliest)
result = scheduler.create(
    strategy="earliest",
    day_start="07:00",
    day_end="22:00",
)

print("\n--- Optimized Schedule (Earliest) ---")
print(result.select(["entity_name", "instance", "time_hhmm", "Category"]))

# Generate an optimized schedule (Latest)
result_latest = scheduler.create(
    strategy="latest",
    day_start="07:00",
    day_end="22:00",
)

print("\n--- Latest Schedule ---")
print(result_latest.select(["entity_name", "instance", "time_hhmm", "Category"]))
