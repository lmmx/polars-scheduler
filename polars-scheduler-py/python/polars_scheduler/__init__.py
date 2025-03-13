from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Union, Literal

import polars as pl
from polars.api import register_dataframe_namespace
from polars_scheduler._polars_scheduler import schedule_dataframe

__all__ = ["schedule_dataframe"]

# Register the DataFrame namespace extension
@register_dataframe_namespace("scheduler")
class SchedulerPlugin:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def new(self) -> pl.DataFrame:
        """Create a new empty schedule with the proper schema."""
        return pl.DataFrame(
            schema={
                "Event": pl.String,
                "Category": pl.String,
                "Unit": pl.String,
                "Amount": pl.Float64,
                "Divisor": pl.Int64,
                "Frequency": pl.String,
                "Constraints": pl.List(pl.String),
                "Windows": pl.List(pl.String),
                "Note": pl.String,
            },
        )

    def add(
        self,
        event: str,
        category: str,
        unit: str,
        amount: float | None = None,
        divisor: int | None = None,
        frequency: str | None = None,
        constraints: list[str] | None = None,
        windows: list[str] | None = None,
        note: str | None = None,
    ) -> pl.DataFrame:
        """
        Add a new resource event to the schedule.

        Args:
            event: Name of the event
            category: Category type
            unit: Unit of measurement
            amount: Numeric amount value
            divisor: Number to divide by
            frequency: How often to use/take
            constraints: List of constraints
            windows: List of time windows
            note: Additional notes
        """
        if constraints is None:
            constraints = []
            
        if windows is None:
            windows = []
            
        if frequency is None:
            frequency = "1x daily"

        # Create a new row
        new_row = pl.DataFrame(
            {
                "Event": [event],
                "Category": [category],
                "Unit": [unit],
                "Amount": [amount],
                "Divisor": [divisor],
                "Frequency": [frequency],
                "Constraints": [constraints],
                "Windows": [windows],
                "Note": [note],
            },
            schema={
                "Event": pl.String,
                "Category": pl.String,
                "Unit": pl.String,
                "Amount": pl.Float64,
                "Divisor": pl.Int64,
                "Frequency": pl.String,
                "Constraints": pl.List(pl.String),
                "Windows": pl.List(pl.String),
                "Note": pl.String,
            },
        )

        # Append to existing DataFrame
        return pl.concat([self._df, new_row], how="vertical")

    def schedule(
        self,
        strategy: str = "earliest",
        day_start: str = "08:00",
        day_end: str = "22:00",
        windows: Optional[List[str]] = None,
        debug: bool = False,
    ) -> pl.DataFrame:
        """
        Schedule events based on the constraints in the DataFrame.

        Args:
            strategy: Either "earliest" or "latest"
            day_start: Start time in "HH:MM" format
            day_end: End time in "HH:MM" format
            windows: Optional list of global time windows in "HH:MM" or "HH:MM-HH:MM" format
            debug: Whether to print debug information

        Returns:
            A DataFrame with the scheduled events
        """
        # Call the Rust function
        result = schedule_dataframe(
            self._df,
            strategy=strategy,
            day_start=day_start,
            day_end=day_end,
            windows=windows,
            debug=debug,
        )
        
        # Join with original dataframe to keep all columns
        joined = self._df.join(
            result,
            left_on="Event",
            right_on="Event",
            how="left",
        )
        
        return joined.sort("TimeMinutes")

def validate_frequency(frequency: str) -> bool:
    """
    Validate that a frequency string is in the correct format.
    Examples: "1x daily", "2x daily", "3x weekly"
    """
    parts = frequency.lower().split()
    if len(parts) != 2:
        return False
        
    # Check first part is like "1x" or "2x"
    if not parts[0].endswith("x"):
        return False
    try:
        int(parts[0][:-1])
    except ValueError:
        return False
        
    # Check second part is a valid period
    valid_periods = ["daily", "weekly", "monthly", "yearly"]
    return parts[1] in valid_periods