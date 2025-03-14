from __future__ import annotations

import inspect
from pathlib import Path

import polars as pl
from polars.api import register_dataframe_namespace
from polars.plugins import register_plugin_function

from .utils import parse_into_expr, parse_version  # noqa: F401

# Determine the correct plugin path
if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

__all__ = ["schedule_events"]


def plug(expr: pl.Expr, **kwargs) -> pl.Expr:
    """
    Wrap Polars' `register_plugin_function` helper to always
    pass the same `lib` (the directory where _polars_scheduler.so/pyd lives).
    """
    func_name = inspect.stack()[1].function
    return register_plugin_function(
        plugin_path=lib,
        function_name=func_name,
        args=expr,
        is_elementwise=True,
        kwargs=kwargs,
    )


def schedule_events(
    expr: pl.Expr,
    *,
    strategy: str = "earliest",
    day_start: str = "08:00",
    day_end: str = "22:00",
    windows: list[str] | None = None,
    debug: bool = False,
) -> pl.Expr:
    """
    Schedule events based on the constraints in a DataFrame.
    Calls the Rust `schedule_events` function from `_polars_scheduler`.

    Parameters
    ----------
    expr : pl.Expr
        Expression representing a struct column containing the event definitions
    strategy : str, default "earliest"
        Scheduling strategy, either "earliest" or "latest"
    day_start : str, default "08:00"
        Start of day in "HH:MM" format
    day_end : str, default "22:00"
        End of day in "HH:MM" format
    windows : List[str], optional
        Global time windows in "HH:MM" or "HH:MM-HH:MM" format
    debug : bool, default False
        Whether to print debug information

    Returns
    -------
    pl.Expr
        Expression representing the scheduled events
    """
    kwargs = {
        "strategy": strategy,
        "day_start": day_start,
        "day_end": day_end,
        "debug": debug,
    }

    if windows is not None:
        kwargs["windows"] = windows

    return plug(expr, **kwargs)


@register_dataframe_namespace("scheduler")
class Scheduler:
    _schema = {
        "Event": pl.String,
        "Category": pl.String,
        "Unit": pl.String,
        "Amount": pl.Float64,
        "Divisor": pl.Int64,
        "Frequency": pl.String,
        "Constraints": pl.List(pl.String),
        "Windows": pl.List(pl.String),
        "Note": pl.String,
    }

    def __init__(self, df: pl.DataFrame | None = None):
        """Store schedule constraints, recreate the DataFrame if its schema is wrong."""
        if df is None or df.height == 0:
            # Create a new empty DataFrame with the correct schema
            self._df = pl.DataFrame(schema=self._schema)
        else:
            # Check if existing DataFrame has correct schema
            usable = df.schema == self._schema
            self._df = (
                df if usable else pl.DataFrame(df.to_dicts(), schema=self._schema)
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
    ) -> None:
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
            schema=self._schema,
        )

        # Append to existing DataFrame
        self._df = pl.concat([self._df, new_row], how="vertical")
        return

    def create(
        self,
        strategy: str = "earliest",
        day_start: str = "08:00",
        day_end: str = "22:00",
        windows: list[str] | None = None,
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
        # Convert DataFrame to struct column
        struct_col = pl.struct(self._df.get_columns()).alias("events")

        # Call the schedule_events function on the struct column
        result = pl.select(
            schedule_events(
                struct_col,
                strategy=strategy,
                day_start=day_start,
                day_end=day_end,
                windows=windows,
                debug=debug,
            ),
        ).unnest("events")

        # Join with original dataframe for context
        entity_columns = [
            "Event",
            "Category",
            "Unit",
            "Amount",
            "Divisor",
            "Frequency",
            "Constraints",
            "Windows",
            "Note",
        ]

        joined = result.join(
            self._df.select(entity_columns),
            left_on="entity_name",
            right_on="Event",
            how="left",
        )

        # Return sorted by time
        return joined.sort("time_minutes")
