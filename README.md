# Polars Scheduler

[![downloads](https://static.pepy.tech/badge/polars-scheduler/month)](https://pepy.tech/project/polars-scheduler)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)
[![PyPI](https://img.shields.io/pypi/v/polars-scheduler.svg)](https://pypi.org/project/polars-scheduler)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/polars-scheduler.svg)](https://pypi.org/project/polars-scheduler)
[![License](https://img.shields.io/pypi/l/polars-scheduler.svg)](https://pypi.python.org/pypi/polars-scheduler)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/lmmx/polars-scheduler/master.svg)](https://results.pre-commit.ci/latest/github/lmmx/polars-scheduler/master)

A Polars plugin for easily scheduling recurring events with constraints.

## Installation

```bash
pip install polars-scheduler[polars]
```

On older CPUs run:

```bash
pip install polars-scheduler[polars-lts-cpu]
```

## Features

- **Powerful Constraint System**: Schedule events with constraints like "≥6h apart", "≥1h before food", or "≥2h after medicine"
- **Time Windows**: Define preferred time windows for events (e.g., breakfast at "08:00", or dinner between "18:00-20:00")
- **Optimization Strategies**: Choose between "earliest" (soonest possible scheduling) or "latest"
  (just-in-time)
- **Smart Distribution**: Automatically distributes multiple daily instances across different time windows
- **Mixed Integer Linear Programming**: Uses MILP solver to find optimal schedules that satisfy all constraints

## Usage

The plugin adds a `scheduler` namespace to Polars DataFrames with methods for registering events and constraints:

```python
import polars as pl
import polars_scheduler

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

# Generate an optimized schedule
result = schedule.scheduler.schedule(
    strategy="earliest",
    day_start="07:00",
    day_end="22:00",
)

# Print the optimized schedule
print(result.select(["Event", "Instance", "TimeHHMM"]))
```

The resulting schedule might look like:

```
┌─────────────┬──────────┬─────────┐
│ Event       ┆ Instance ┆ TimeHHMM │
│ ---         ┆ ---      ┆ ---      │
│ str         ┆ i32      ┆ str      │
╞═════════════╪══════════╪═════════╡
│ antibiotic  ┆ 1        ┆ 07:00    │
│ breakfast   ┆ 1        ┆ 08:00    │
│ vitamin     ┆ 1        ┆ 09:00    │
│ lunch       ┆ 1        ┆ 12:00    │
│ antibiotic  ┆ 2        ┆ 13:00    │
│ dinner      ┆ 1        ┆ 18:00    │
└─────────────┴──────────┴─────────┘
```

## Constraint Types

The scheduler supports several constraint types:

- **Apart constraint**: `"≥6h apart"` - Ensures that multiple instances of the same entity are scheduled at least 6 hours apart
- **Before constraint**: `"≥1h before food"` - Ensures that an entity is scheduled at least 1 hour before any entity in the "food" category
- **After constraint**: `"≥2h after medication"` - Ensures that an entity is scheduled at least 2 hours after any entity in the "medication" category

When both "before" and "after" constraints are specified for the same entity-category pair, they are combined as an "OR" constraint using big-M formulation, not an "AND" constraint, which would often be impossible to satisfy.

## Window Specifications

Windows can be specified in two formats:

- **Anchor**: `"08:00"` - A specific time point
- **Range**: `"18:00-20:00"` - A time range

When multiple instances of the same entity need to be scheduled, the solver will try to distribute them across different windows.

## Optimization Strategies

- **Earliest**: Places events as early as possible while satisfying all constraints
- **Latest**: Places events as late as possible while satisfying all constraints

## Standalone CLI Tool

The project also includes a standalone command-line tool for scheduling:

```bash
cd scheduler-cli
cargo run -- --strategy earliest --start=07:00 --end=22:00
```

## Development

To build the project:

1. Build the core library:
   ```bash
   cd scheduler-core
   cargo build
   ```

2. Build the CLI tool:
   ```bash
   cd scheduler-cli
   cargo build
   ```

3. Build the Python bindings:
   ```bash
   cd polars-scheduler-py
   maturin develop
   ```

## License

MIT License
