pub mod domain;
pub mod parse;
pub mod solver;

// Re-export commonly used items for easier access
pub use domain::{
    Entity, SchedulerConfig, ScheduleStrategy, ScheduleResult, ScheduledEvent,
    WindowSpec, ConstraintExpr, ConstraintType, ConstraintRef, Frequency
};
pub use parse::{
    parse_from_table, parse_one_constraint, parse_one_window, 
    parse_hhmm_to_minutes, format_minutes_to_hhmm
};
pub use solver::solve_schedule;

/// Creates a sample table for testing and demonstration purposes
pub fn create_sample_table() -> Vec<Vec<String>> {
    vec![
        vec![
            "Entity".to_string(),
            "Category".to_string(),
            "Unit".to_string(),
            "Amount".to_string(),
            "Split".to_string(),
            "Frequency".to_string(),
            "Constraints".to_string(),
            "Windows".to_string(),
            "Note".to_string(),
        ],
        vec![
            "Antepsin".to_string(),
            "med".to_string(),
            "tablet".to_string(),
            "null".to_string(),
            "3".to_string(),
            "3x daily".to_string(),
            "[\"≥6h apart\", \"≥1h before food\", \"≥2h after food\"]".to_string(),
            "[]".to_string(), // no windows
            "in 1tsp water".to_string(),
        ],
        vec![
            "Gabapentin".to_string(),
            "med".to_string(),
            "ml".to_string(),
            "1.8".to_string(),
            "null".to_string(),
            "2x daily".to_string(),
            "[\"≥8h apart\"]".to_string(),
            "[]".to_string(),
            "null".to_string(),
        ],
        vec![
            "Pardale".to_string(),
            "med".to_string(),
            "tablet".to_string(),
            "null".to_string(),
            "2".to_string(),
            "2x daily".to_string(),
            "[\"≥8h apart\"]".to_string(),
            "[]".to_string(),
            "null".to_string(),
        ],
        vec![
            "Pro-Kolin".to_string(),
            "med".to_string(),
            "ml".to_string(),
            "3.0".to_string(),
            "null".to_string(),
            "2x daily".to_string(),
            "[]".to_string(),
            "[]".to_string(),
            "with food".to_string(),
        ],
        vec![
            "Chicken and rice".to_string(),
            "food".to_string(),
            "meal".to_string(),
            "null".to_string(),
            "null".to_string(),
            "2x daily".to_string(),
            "[]".to_string(),               // no 'apart' constraints
            "[\"08:00\", \"18:00-20:00\"]".to_string(), // has 1 anchor & 1 range
            "some note".to_string(),
        ],
    ]
}

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
        output.push_str(&format!("{:8} | {:<20} | #{}\n", 
            time_str, event.entity_name, event.instance));
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
                
            output.push_str(&format!("{:<20} | {:<20} | {}\n", 
                entity, window, instances_str));
        }
    }
    
    output
}

/// Runs the scheduler with the specified configuration and returns the result
pub fn run_scheduler(
    entities: Vec<Entity>,
    config: SchedulerConfig,
    debug: bool
) -> Result<ScheduleResult, String> {
    solve_schedule(entities, config, debug)
}

/// Create a default scheduler configuration
pub fn default_config() -> SchedulerConfig {
    SchedulerConfig::default()
}

/// Run the scheduler with sample data and default configuration
pub fn run_sample_schedule(strategy: ScheduleStrategy, debug: bool) -> Result<ScheduleResult, String> {
    let table = create_sample_table();
    let entities = parse_from_table(table)?;
    let mut config = default_config();
    config.strategy = strategy;
    run_scheduler(entities, config, debug)
}

/// Convert a list of constraints represented as strings into ConstraintExpr objects
pub fn parse_constraints(constraints: &[String]) -> Result<Vec<ConstraintExpr>, String> {
    constraints.iter()
        .map(|s| parse_one_constraint(s))
        .collect()
}

/// Convert a list of window specifications as strings into WindowSpec objects
pub fn parse_windows(windows: &[String]) -> Result<Vec<WindowSpec>, String> {
    windows.iter()
        .map(|s| parse_one_window(s))
        .collect()
}