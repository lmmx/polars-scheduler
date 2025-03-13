use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;
use scheduler_core::{
    Entity, SchedulerConfig, ScheduleStrategy,
    parse_one_constraint, parse_one_window, solve_schedule,
    format_minutes_to_hhmm
};

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
    pub debug: bool,
}

/// Computes output type for the expression
fn schedule_output_type(input_fields: &[Field]) -> PolarsResult<Field> {
    // We'll return a struct array with scheduled times for each event/instance
    Ok(Field::new("schedule".into(), DataType::Struct(vec![
        Field::new("entity_name".into(), DataType::String),
        Field::new("instance".into(), DataType::Int32),
        Field::new("time_minutes".into(), DataType::Int32),
        Field::new("time_hhmm".into(), DataType::String),
    ])))
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
    
    // Extract optional columns
    let constraints_col = df
        .field_by_name("Constraints")
        .and_then(|s| s.list().map(|lc| lc.clone()))
        .unwrap_or_else(|_| ListChunked::full_null("Constraints".into(), df.len()));
    
    let windows_col = df
        .field_by_name("Windows")
        .and_then(|s| s.list().map(|lc| lc.clone()))
        .unwrap_or_else(|_| ListChunked::full_null("Windows".into(), df.len()));
    
    // Convert to Entity objects
    let mut entities: Vec<Entity> = Vec::with_capacity(df.len());
    
    for i in 0..df.len() {
        // Extract basic fields
        let event = event_col.get(i)
            .or_else(|| PolarsError::ComputeError("Missing event name".into()))?;
        
        let category = category_col.get(i)
            .or_else(|| PolarsError::ComputeError("Missing category".into()))?;
        
        let frequency_str = frequency_col.get(i)
            .or_else(|| PolarsError::ComputeError("Missing frequency".into()))?;
        
        let frequency = scheduler_core::Frequency::from_frequency_str(frequency_str);
        
        // Parse constraints
        let constraints = if let Some(constraint_arr) = constraints_col.get(i) {
            let mut parsed_constraints = Vec::new();
            for j in 0..constraint_arr.len() {
                if let Some(constraint_str) = constraint_arr.get_str(j) {
                    match parse_one_constraint(constraint_str) {
                        Ok(constraint) => parsed_constraints.push(constraint),
                        Err(e) => polars_bail!(
                            ComputeError: format!("Error parsing constraint '{}': {}", constraint_str, e)
                        ),
                    }
                }
            }
            parsed_constraints
        } else {
            Vec::new()
        };
        
        // Parse windows
        let windows = if let Some(window_arr) = windows_col.get(i) {
            let mut parsed_windows = Vec::new();
            for j in 0..window_arr.len() {
                if let Some(window_str) = window_arr.get_str(j) {
                    match parse_one_window(window_str) {
                        Ok(window) => parsed_windows.push(window),
                        Err(e) => polars_bail!(
                            ComputeError: format!("Error parsing window '{}': {}", window_str, e)
                        ),
                    }
                }
            }
            parsed_windows
        } else {
            Vec::new()
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
    
    // Create scheduler config
    let config = SchedulerConfig {
        day_start_minutes: day_start,
        day_end_minutes: day_end,
        strategy,
        global_windows,
        penalty_weight: 0.3,
    };
    
    // Solve the schedule
    let result = match solve_schedule(entities, config, kwargs.debug) {
        Ok(r) => r,
        Err(e) => polars_bail!(
            ComputeError: format!("Scheduler error: {}", e)
        ),
    };
    
    // Prepare result arrays
    let entity_names: Vec<_> = result.scheduled_events.iter()
        .map(|e| e.entity_name.clone())
        .collect();
    
    let instances: Vec<_> = result.scheduled_events.iter()
        .map(|e| e.instance as i32)
        .collect();
    
    let time_minutes: Vec<_> = result.scheduled_events.iter()
        .map(|e| e.time_minutes)
        .collect();
    
    let time_hhmm: Vec<_> = result.scheduled_events.iter()
        .map(|e| format_minutes_to_hhmm(e.time_minutes))
        .collect();
    
    // Create result struct array
    let fields = vec![
        Series::new("entity_name".into(), entity_names),
        Series::new("instance".into(), instances),
        Series::new("time_minutes".into(), time_minutes),
        Series::new("time_hhmm".into(), time_hhmm),
    ];
    
    StructChunked::new("schedule", &fields)
        .map_err(|e| PolarsError::ComputeError(format!("Error creating result struct: {}", e).into()))?
        .into_series()
        .pipe(Ok)
}
