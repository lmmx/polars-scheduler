from __future__ import annotations

from typing import List, Dict, Any, Optional

import polars as pl
from polars.api import register_dataframe_namespace


@register_dataframe_namespace("scheduler")
class SchedulerPlugin:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def new(self) -> pl.DataFrame:
        """Create a new empty schedule or clear the existing one."""
        # Create with proper schema using proper data types
        return pl.DataFrame(
            schema={
                "Event": pl.String,
                "Category": pl.String,
                "Unit": pl.String,
                "Amount": pl.Float64,
                "Divisor": pl.Int64,
                "Frequency": pl.String,
                "Constraints": pl.List(pl.String),
                "Note": pl.String,
            }
        )

    def add(
        self,
        event: str,
        category: str,
        unit: str,
        amount: Optional[float] = None,
        divisor: Optional[int] = None,
        frequency: Optional[str] = None,
        constraints: List[str] = None,
        note: Optional[str] = None,
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
            note: Additional notes
        """
        if constraints is None:
            constraints = []

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
                "Note": pl.String,
            },
        )

        # Append to existing DataFrame
        return pl.concat([self._df, new_row], how="vertical")
