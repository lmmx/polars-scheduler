pub mod domain;
pub mod parse;
pub mod solver;

// Re-export commonly used items for easier access
pub use domain::{
    ConstraintExpr, ConstraintRef, ConstraintType, Entity, Frequency, ScheduleResult,
    ScheduleStrategy, ScheduledEvent, SchedulerConfig, WindowSpec,
};
pub use parse::{
    format_minutes_to_hhmm, parse_from_table, parse_hhmm_to_minutes, parse_one_constraint,
    parse_one_window,
};
pub use solver::solve_schedule;

/// Helper function to print a schedule in a readable format
pub fn format_schedule(result: &ScheduleResult) -> String {
    let mut output = String::new();

    // Format header
    output.push_str("--- SCHEDULE ---\n");
    output.push_str(&format!("Total penalty: {:.1}\n\n", result.total_penalty));

    // Format scheduled events
    output.push_str("TIME     | ENTITY              | INSTANCE\n");
    output.push_str("---------+---------------------+---------\n");

    for event in &result.scheduled_events {
        let time_str = format_minutes_to_hhmm(event.time_minutes);
        output.push_str(&format!(
            "{:8} | {:<20} | #{}\n",
            time_str, event.entity_name, event.instance
        ));
    }

    // Format window usage
    if !result.window_usage.is_empty() {
        output.push_str("\n--- WINDOW USAGE ---\n");
        output.push_str("ENTITY              | WINDOW             | USED BY\n");
        output.push_str("--------------------+--------------------+--------\n");

        for (entity, window, instances) in &result.window_usage {
            let instances_str = instances
                .iter()
                .map(|i| format!("#{}", i))
                .collect::<Vec<_>>()
                .join(", ");

            output.push_str(&format!(
                "{:<20} | {:<20} | {}\n",
                entity, window, instances_str
            ));
        }
    }

    output
}

/// Runs the scheduler with the specified configuration and returns the result
pub fn run_scheduler(
    entities: Vec<Entity>,
    config: SchedulerConfig,
    debug: bool,
) -> Result<ScheduleResult, String> {
    solve_schedule(entities, config, debug)
}

/// Create a default scheduler configuration
pub fn default_config() -> SchedulerConfig {
    SchedulerConfig::default()
}

/// Convert a list of constraints represented as strings into ConstraintExpr objects
pub fn parse_constraints(constraints: &[String]) -> Result<Vec<ConstraintExpr>, String> {
    constraints
        .iter()
        .map(|s| parse_one_constraint(s))
        .collect()
}

/// Convert a list of window specifications as strings into WindowSpec objects
pub fn parse_windows(windows: &[String]) -> Result<Vec<WindowSpec>, String> {
    windows.iter().map(|s| parse_one_window(s)).collect()
}

/// Run the scheduler withprovided entities and specified configuration strategy
pub fn run_schedule_with_stratey(
    entities: Vec<Entity>,
    strategy: ScheduleStrategy,
    debug: bool,
) -> Result<ScheduleResult, String> {
    let mut config = default_config();
    config.strategy = strategy;
    run_scheduler(entities, config, debug)
}
