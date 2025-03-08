# Polars Scheduler

<!-- [![downloads](https://static.pepy.tech/badge/polars-scheduler/month)](https://pepy.tech/project/polars-scheduler) -->
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![pdm-managed](https://img.shields.io/badge/pdm-managed-blueviolet)](https://pdm.fming.dev)
[![PyPI](https://img.shields.io/pypi/v/polars-scheduler.svg)](https://pypi.org/project/polars-scheduler)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/polars-scheduler.svg)](https://pypi.org/project/polars-scheduler)
[![License](https://img.shields.io/pypi/l/polars-scheduler.svg)](https://pypi.python.org/pypi/polars-scheduler)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/lmmx/polars-scheduler/master.svg)](https://results.pre-commit.ci/latest/github/lmmx/polars-scheduler/master)

A Polars plugin for easily scheduling recurring events with constraints.

## Installation

```python
pip install polars-scheduler[polars]
```

On older CPUs run:

```python
pip install polars-scheduler[polars-lts-cpu]
```

## Usage

The plugin adds a `scheduler` namespace to Polars DataFrames with methods for registering events and
constraints:

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
)

...
```

The idea is that you have a DataFrame with a schedule of the day so far perhaps
(in this case I don't as it's still a proof-of-concept) and then you have the recurring events
expressed as a schedule with constraints.

This schedule of constraints would get built up (as in this example, see `examples/nutrition.py`)
and then you would validate or schedule ahead.

```py
┌──────────────┬────────────┬─────────┬────────┬─────────┬───────────┬──────────────┬──────────────┐
│ Event        ┆ Category   ┆ Unit    ┆ Amount ┆ Divisor ┆ Frequency ┆ Constraints  ┆ Note         │
│ ---          ┆ ---        ┆ ---     ┆ ---    ┆ ---     ┆ ---       ┆ ---          ┆ ---          │
│ str          ┆ str        ┆ str     ┆ f64    ┆ i64     ┆ str       ┆ list[str]    ┆ str          │
╞══════════════╪════════════╪═════════╪════════╪═════════╪═══════════╪══════════════╪══════════════╡
│ breakfast    ┆ meal       ┆ serving ┆ null   ┆ null    ┆ 1x daily  ┆ []           ┆ null         │
│ lunch        ┆ meal       ┆ serving ┆ null   ┆ null    ┆ 1x daily  ┆ []           ┆ null         │
│ dinner       ┆ meal       ┆ serving ┆ null   ┆ null    ┆ 1x daily  ┆ []           ┆ null         │
│ vitamin      ┆ supplement ┆ pill    ┆ null   ┆ null    ┆ 1x daily  ┆ ["with       ┆ null         │
│              ┆            ┆         ┆        ┆         ┆           ┆ breakfast"]  ┆              │
│ antibiotic   ┆ medication ┆ pill    ┆ null   ┆ null    ┆ 2x daily  ┆ ["≥1h after  ┆ null         │
│              ┆            ┆         ┆        ┆         ┆           ┆ meal"]       ┆              │
│ probiotic    ┆ supplement ┆ capsule ┆ null   ┆ null    ┆ 1x daily  ┆ ["≥2h after  ┆ null         │
│              ┆            ┆         ┆        ┆         ┆           ┆ antibiotic"] ┆              │
│ protein      ┆ supplement ┆ gram    ┆ 30.0   ┆ null    ┆ 1x daily  ┆ ["≤30m after ┆ mix with     │
│ shake        ┆            ┆         ┆        ┆         ┆           ┆ gym OR with  ┆ 300ml water  │
│              ┆            ┆         ┆        ┆         ┆           ┆ breakfast"]  ┆              │
│ ginger       ┆ supplement ┆ shot    ┆ null   ┆ null    ┆ 1x daily  ┆ ["before     ┆ null         │
│              ┆            ┆         ┆        ┆         ┆           ┆ breakfast"]  ┆              │
│ gym          ┆ exercise   ┆ session ┆ null   ┆ null    ┆ 3x weekly ┆ []           ┆ null         │
└──────────────┴────────────┴─────────┴────────┴─────────┴───────────┴──────────────┴──────────────┘
```
