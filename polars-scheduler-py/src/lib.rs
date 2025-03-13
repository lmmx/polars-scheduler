use pyo3::prelude::*;
use pyo3_polars::{
    derive::polars_expr,
    error::PolarsResult,
    export::polars_core::{chunked_array::builder::ListBuilder, prelude::*},
};

use scheduler_core::{
    format_minutes_to_hhmm, parse_from_table, parse_hhmm_to_minutes, parse_one_constraint,
    parse_one_window, solve_schedule, ConstraintExpr, ConstraintRef, ConstraintType, Entity,
    Frequency, ScheduleResult, ScheduleStrategy, ScheduledEvent, SchedulerConfig, WindowSpec,
};

/// Format for the input DataFrame:
/// - "Event" (str) - The event name
/// - "Category" (str) - Event category  
/// - "Unit" (str, optional) - Unit of measure
/// - "Amount" (f64, optional) - Numeric amount
/// - "Divisor" (i64, optional) - Division factor
/// - "Frequency" (str) - How often (e.g., "1x daily", "2x daily")
/// - "Constraints" (list[str]) - Constraints like "≥8h apart"
/// - "Note" (str, optional) - Additional notes
///
/// Output columns added:
/// - "TimeMinutes" (i32) - Scheduled time in minutes from midnight
/// - "TimeHHMM" (str) - Formatted time as "HH:MM"
/// - "Instance" (i32) - Which instance (1, 2, 3, etc.)
#[pyfunction]
#[pyo3(signature = (
    df,
    strategy = "earliest",
    day_start = "08:00",
    day_end = "22:00",
    windows = None,
    debug = false
))]
fn schedule_dataframe(
    df: &PyAny,
    strategy: &str,
    day_start: &str,
    day_end: &str,
    windows: Option<Vec<String>>,
    debug: bool,
) -> PyResult<PyObject> {
    // 1) Convert input pandas DataFrame to Polars DataFrame
    Python::with_gil(|py| {
        // First convert the DataFrame to a Polars DataFrame if needed
        let df = if pyo3_polars::is_polars_dataframe(df) {
            df.into()
        } else {
            let to_polars = PyModule::import(py, "polars")?.getattr("from_pandas")?;
            to_polars.call1((df,))?
        };

        // Get Polars DataFrame reference
        let pldf = pyo3_polars::to_rust_df(df)?;

        // 2) Convert DataFrame to entities
        let entities = df_to_entities(&pldf).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Error converting DataFrame to entities: {}",
                e
            ))
        })?;

        // 3) Parse scheduler config
        let strategy = match strategy.to_lowercase().as_str() {
            "earliest" => ScheduleStrategy::Earliest,
            "latest" => ScheduleStrategy::Latest,
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Invalid strategy: {}. Must be 'earliest' or 'latest'",
                    strategy
                )));
            }
        };

        let day_start_min = parse_hhmm_to_minutes(day_start).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid day_start: {}", e))
        })?;

        let day_end_min = parse_hhmm_to_minutes(day_end).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid day_end: {}", e))
        })?;

        let global_windows = if let Some(window_specs) = windows {
            window_specs
                .iter()
                .map(|spec| parse_one_window(spec))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error parsing windows: {}",
                        e
                    ))
                })?
        } else {
            Vec::new()
        };

        let config = SchedulerConfig {
            day_start_minutes: day_start_min,
            day_end_minutes: day_end_min,
            strategy,
            global_windows,
            penalty_weight: 0.3,
        };

        // 4) Solve the schedule
        let schedule_result = solve_schedule(entities, config, debug).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Scheduler error: {}", e))
        })?;

        // 5) Create result DataFrame
        let result_df = schedule_result_to_df(&schedule_result)?;

        // 6) Convert back to Python
        let pydf = pyo3_polars::to_py_df(py, result_df)?;
        Ok(pydf)
    })
}

/// Convert a Polars DataFrame to a vector of entities
fn df_to_entities(df: &DataFrame) -> Result<Vec<Entity>, String> {
    // Validate required columns
    let required_cols = vec!["Event", "Category", "Frequency"];
    for col in &required_cols {
        if !df.schema().contains(col) {
            return Err(format!("Required column '{}' not found", col));
        }
    }

    // Create entities
    let mut entities = Vec::with_capacity(df.height());

    for row_idx in 0..df.height() {
        let event_name = df
            .column("Event")
            .map_err(|e| format!("Error accessing Event column: {}", e))?
            .str()
            .map_err(|_| "Event column must be string type".to_string())?
            .get(row_idx)
            .ok_or_else(|| format!("Missing Event value at row {}", row_idx))?
            .to_string();

        let category = df
            .column("Category")
            .map_err(|e| format!("Error accessing Category column: {}", e))?
            .str()
            .map_err(|_| "Category column must be string type".to_string())?
            .get(row_idx)
            .ok_or_else(|| format!("Missing Category value at row {}", row_idx))?
            .to_string();

        let frequency_str = df
            .column("Frequency")
            .map_err(|e| format!("Error accessing Frequency column: {}", e))?
            .str()
            .map_err(|_| "Frequency column must be string type".to_string())?
            .get(row_idx)
            .ok_or_else(|| format!("Missing Frequency value at row {}", row_idx))?;

        let frequency = Frequency::from_str(frequency_str);

        // Parse constraints (optional)
        let constraints = if df.schema().contains("Constraints") {
            match df.column("Constraints") {
                Ok(c) => {
                    match c.dtype() {
                        DataType::List(_) => {
                            let list_arr = c
                                .list()
                                .map_err(|_| "Failed to convert Constraints to list".to_string())?;

                            // Get the constraints for this row
                            let opt_constr_arr = list_arr.get(row_idx);
                            match opt_constr_arr {
                                Some(constr_arr) => {
                                    let mut constraints = Vec::new();
                                    for i in 0..constr_arr.len() {
                                        if let Some(constr_str) = constr_arr.get_str(i) {
                                            match parse_one_constraint(constr_str) {
                                                Ok(constraint) => constraints.push(constraint),
                                                Err(e) => {
                                                    return Err(format!(
                                                        "Error parsing constraint '{}': {}",
                                                        constr_str, e
                                                    ))
                                                }
                                            }
                                        }
                                    }
                                    constraints
                                }
                                None => Vec::new(),
                            }
                        }
                        _ => Vec::new(),
                    }
                }
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        // Parse windows (optional)
        let windows = if df.schema().contains("Windows") {
            match df.column("Windows") {
                Ok(w) => {
                    match w.dtype() {
                        DataType::List(_) => {
                            let list_arr = w
                                .list()
                                .map_err(|_| "Failed to convert Windows to list".to_string())?;

                            // Get the windows for this row
                            let opt_win_arr = list_arr.get(row_idx);
                            match opt_win_arr {
                                Some(win_arr) => {
                                    let mut windows = Vec::new();
                                    for i in 0..win_arr.len() {
                                        if let Some(win_str) = win_arr.get_str(i) {
                                            match parse_one_window(win_str) {
                                                Ok(window) => windows.push(window),
                                                Err(e) => {
                                                    return Err(format!(
                                                        "Error parsing window '{}': {}",
                                                        win_str, e
                                                    ))
                                                }
                                            }
                                        }
                                    }
                                    windows
                                }
                                None => Vec::new(),
                            }
                        }
                        _ => Vec::new(),
                    }
                }
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        entities.push(Entity {
            name: event_name,
            category,
            frequency,
            constraints,
            windows,
        });
    }

    Ok(entities)
}

/// Convert a schedule result to a Polars DataFrame
fn schedule_result_to_df(result: &ScheduleResult) -> PolarsResult<DataFrame> {
    // Create builders for each column
    let n = result.scheduled_events.len();

    let mut entity_builder = StringBuilder::new();
    let mut instance_builder = Int32Chunked::new_from_iter("Instance", Vec::with_capacity(n));
    let mut time_min_builder = Int32Chunked::new_from_iter("TimeMinutes", Vec::with_capacity(n));
    let mut time_hhmm_builder = StringBuilder::new();

    // Fill builders with data
    for event in &result.scheduled_events {
        entity_builder.append_value(&event.entity_name);
        instance_builder.append_value(event.instance as i32);
        time_min_builder.append_value(event.time_minutes);
        time_hhmm_builder.append_value(&format_minutes_to_hhmm(event.time_minutes));
    }

    // Create DataFrame
    DataFrame::new(vec![
        entity_builder.finish().into_series(),
        instance_builder.into_series(),
        time_min_builder.into_series(),
        time_hhmm_builder.finish().into_series(),
    ])
}

/// Register the module
#[pymodule]
fn _polars_scheduler(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule_dataframe, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
