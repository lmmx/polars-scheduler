use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use scheduler_core::{
    format_minutes_to_hhmm, parse_one_constraint, parse_one_window, solve_schedule, Entity,
    ScheduleStrategy, SchedulerConfig,
};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct ScheduleKwargs {
    #[serde(default)]
    pub strategy: String,

    #[serde(default)]
    pub day_start: String,

    #[serde(default)]
    pub day_end: String,

    #[serde(default)]
    pub windows: Option<Vec<String>>,

    #[serde(default)]
    pub penalty_weight: f64,

    #[serde(default)]
    pub window_tolerance: f64,

    #[serde(default)]
    pub debug: bool,
}

/// Computes output type for the expression
fn schedule_output_type(_input_fields: &[Field]) -> PolarsResult<Field> {
    // We'll return a struct array with scheduled times for each event/instance
    Ok(Field::new(
        "schedule".into(),
        DataType::Struct(vec![
            Field::new("entity_name".into(), DataType::String),
            Field::new("instance".into(), DataType::Int32),
            Field::new("time_minutes".into(), DataType::Int32),
            Field::new("time_hhmm".into(), DataType::String),
        ]),
    ))
}

/// Polars expression that schedules events based on their constraints
/// Input is a DataFrame with event definitions
#[polars_expr(output_type_func=schedule_output_type)]
pub fn schedule_events(inputs: &[Series], kwargs: ScheduleKwargs) -> PolarsResult<Series> {
    // Validate that our input has all the necessary columns
    let df = match inputs[0].struct_() {
        Ok(ca) => ca,
        Err(_) => polars_bail!(
            ComputeError: "Expected a struct column representing a DataFrame"
        ),
    };

    // Extract the required columns from the struct array
    let event_col = df.field_by_name("Event")?.cast(&DataType::String)?;
    let category_col = df.field_by_name("Category")?.cast(&DataType::String)?;
    let frequency_col = df.field_by_name("Frequency")?.cast(&DataType::String)?;

    // Extract optional columns with the correct type
    let constraints_col = df
        .field_by_name("Constraints")
        .and_then(|s| s.list().map(|lc| lc.clone()))
        .unwrap_or_else(|_| ListChunked::full_null("Constraints".into(), df.len()));

    let windows_col = df
        .field_by_name("Windows")
        .and_then(|s| s.list().map(|lc| lc.clone()))
        .unwrap_or_else(|_| ListChunked::full_null("Windows".into(), df.len()));

    // Pre-compute null masks for both list columns
    let constraints_null_mask = constraints_col.is_null();
    let windows_null_mask = windows_col.is_null();

    // Convert to Entity objects
    let mut entities: Vec<Entity> = Vec::with_capacity(df.len());

    for i in 0..df.len() {
        // Extract basic fields
        let event = event_col
            .get(i)
            .or_else(|_| Err(PolarsError::ComputeError("Missing event name".into())))?;

        let category = category_col
            .get(i)
            .or_else(|_| Err(PolarsError::ComputeError("Missing category".into())))?;

        let frequency_str = frequency_col
            .get(i)
            .or_else(|_| Err(PolarsError::ComputeError("Missing frequency".into())))?;

        let frequency = match scheduler_core::Frequency::from_frequency_str(
            &frequency_str.to_string().trim_matches('"'),
        ) {
            Ok(freq) => freq,
            Err(e) => polars_bail!(
                ComputeError: format!("Failed to parse frequency from '{}': {}", frequency_str, e)
            ),
        };

        // Parse constraints using a simpler approach
        let constraints = {
            let mut parsed_constraints = Vec::new();

            // Check if we have any constraints at this index using the pre-computed mask
            if !constraints_null_mask.get(i).unwrap_or(true) {
                if let Ok(av) = constraints_col.get_any_value(i) {
                    if let AnyValue::List(list_series) = av {
                        // Now we have a proper Series
                        if let Ok(string_chunked) = list_series.str() {
                            for j in 0..string_chunked.len() {
                                if let Some(constraint_str) = string_chunked.get(j) {
                                    match parse_one_constraint(constraint_str) {
                                        Ok(constraint) => parsed_constraints.push(constraint),
                                        Err(e) => polars_bail!(
                                            ComputeError: format!("Error parsing constraint '{}': {}", constraint_str, e)
                                        ),
                                    }
                                }
                            }
                        }
                    }
                }
            }

            parsed_constraints
        };

        // Parse windows using same approach
        let windows = {
            let mut parsed_windows = Vec::new();

            // Check if we have any windows at this index using the pre-computed mask
            if !windows_null_mask.get(i).unwrap_or(true) {
                if let Ok(av) = windows_col.get_any_value(i) {
                    if let AnyValue::List(list_series) = av {
                        // Now we have a proper Series
                        if let Ok(string_chunked) = list_series.str() {
                            for j in 0..string_chunked.len() {
                                if let Some(window_str) = string_chunked.get(j) {
                                    match parse_one_window(window_str) {
                                        Ok(window) => parsed_windows.push(window),
                                        Err(e) => polars_bail!(
                                            ComputeError: format!("Error parsing window '{}': {}", window_str, e)
                                        ),
                                    }
                                }
                            }
                        }
                    }
                }
            }

            parsed_windows
        };

        // Create Entity
        entities.push(Entity {
            name: event.to_string(),
            category: category.to_string(),
            frequency,
            constraints,
            windows,
        });
    }

    // Parse scheduler config from kwargs
    let strategy = match kwargs.strategy.to_lowercase().as_str() {
        "earliest" | "" => ScheduleStrategy::Earliest,
        "latest" => ScheduleStrategy::Latest,
        s => polars_bail!(
            ComputeError: format!("Invalid strategy: '{}'. Must be 'earliest' or 'latest'", s)
        ),
    };

    // Parse day start/end times
    let day_start = if kwargs.day_start.is_empty() {
        8 * 60 // Default 8:00
    } else {
        match scheduler_core::parse_hhmm_to_minutes(&kwargs.day_start) {
            Ok(minutes) => minutes,
            Err(e) => polars_bail!(
                ComputeError: format!("Invalid day_start: {}", e)
            ),
        }
    };

    let day_end = if kwargs.day_end.is_empty() {
        22 * 60 // Default 22:00
    } else {
        match scheduler_core::parse_hhmm_to_minutes(&kwargs.day_end) {
            Ok(minutes) => minutes,
            Err(e) => polars_bail!(
                ComputeError: format!("Invalid day_end: {}", e)
            ),
        }
    };

    // Parse global windows if provided
    let global_windows = if let Some(window_specs) = &kwargs.windows {
        let mut parsed_windows = Vec::new();
        for spec in window_specs {
            match parse_one_window(spec) {
                Ok(window) => parsed_windows.push(window),
                Err(e) => polars_bail!(
                    ComputeError: format!("Error parsing window '{}': {}", spec, e)
                ),
            }
        }
        parsed_windows
    } else {
        Vec::new()
    };

    let penalty_weight = kwargs.penalty_weight;
    let window_tolerance = kwargs.window_tolerance;

    // Create scheduler config
    let config = SchedulerConfig {
        day_start_minutes: day_start,
        day_end_minutes: day_end,
        strategy,
        global_windows,
        penalty_weight,
        window_tolerance,
    };

    // Solve the schedule
    let result = match solve_schedule(entities, config, kwargs.debug) {
        Ok(r) => r,
        Err(e) => polars_bail!(
            ComputeError: format!("Scheduler error: {}", e)
        ),
    };

    // Prepare result arrays
    let entity_names: Vec<_> = result
        .scheduled_events
        .iter()
        .map(|e| e.entity_name.trim_matches('"'))
        .collect();

    let instances: Vec<_> = result
        .scheduled_events
        .iter()
        .map(|e| e.instance as i32)
        .collect();

    let time_minutes: Vec<_> = result
        .scheduled_events
        .iter()
        .map(|e| e.time_minutes)
        .collect();

    let time_hhmm: Vec<_> = result
        .scheduled_events
        .iter()
        .map(|e| format_minutes_to_hhmm(e.time_minutes))
        .collect();

    // Create individual series with proper into() for string literals
    let entity_series = Series::new("entity_name".into(), entity_names);
    let instance_series = Series::new("instance".into(), instances);
    let time_minutes_series = Series::new("time_minutes".into(), time_minutes);
    let time_hhmm_series = Series::new("time_hhmm".into(), time_hhmm);

    // Create field series and determine output length
    let field_series = vec![
        entity_series,
        instance_series,
        time_minutes_series,
        time_hhmm_series,
    ];

    // Calculate result length (all fields should have the same length)
    let len = if field_series.is_empty() {
        0
    } else {
        field_series[0].len()
    };

    // Use the cleaner StructChunked::from_series approach from the official plugin guide
    StructChunked::from_series("schedule".into(), len, field_series.iter())
        .map(|ca| ca.into_series())
}
