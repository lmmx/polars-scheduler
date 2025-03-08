import polars as pl
import polars_scheduler  # noqa: F401

# Create a new empty schedule
schedule = pl.DataFrame().scheduler.new()

# Add simple meal and medication schedule
schedule = schedule.scheduler.add(
    event="breakfast",
    category="meal",
    unit="serving",
    frequency="1x daily",
)

schedule = schedule.scheduler.add(
    event="lunch",
    category="meal",
    unit="serving",
    frequency="1x daily",
)

schedule = schedule.scheduler.add(
    event="dinner",
    category="meal",
    unit="serving",
    frequency="1x daily",
)

schedule = schedule.scheduler.add(
    event="vitamin",
    category="supplement",
    unit="pill",
    frequency="1x daily",
    constraints=["with breakfast"],
)

schedule = schedule.scheduler.add(
    event="antibiotic",
    category="medication",
    unit="pill",
    frequency="2x daily",
    constraints=["≥1h after meal"],
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
    constraints=["≤30m after gym OR with breakfast"],
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

# Print the schedule
print("Schedule:")
cfg = pl.Config()
cfg.set_tbl_hide_dataframe_shape(True)
cfg.set_fmt_str_lengths(100)
print(schedule)
