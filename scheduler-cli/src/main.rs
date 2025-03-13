mod cli;

use crate::cli::parse_config_from_args;
use colored::Colorize;
use scheduler_core::{create_sample_table, format_schedule, parse_from_table, solve_schedule};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    // Parse command-line arguments
    let config = parse_config_from_args();
    println!(
        "{}",
        format!(
            "Using day window: {}..{} (in minutes)",
            config.day_start_minutes, config.day_end_minutes
        )
        .yellow()
    );
    println!("{}", format!("Strategy: {:?}", config.strategy).yellow());

    // Parse sample table data
    let table_data = create_sample_table();
    let entities = parse_from_table(table_data)?;

    println!("{}", "Entities loaded:".green());
    for e in &entities {
        println!(
            "  - {} ({}x daily) with {} constraints and {} windows",
            e.name,
            e.frequency.instances_per_day(),
            e.constraints.len(),
            e.windows.len()
        );
    }

    // Solve the schedule
    println!("{}", "\nSolving schedule...".green());
    let debug_enabled = std::env::args().any(|a| a == "--debug");

    let result = solve_schedule(entities, config, debug_enabled)
        .map_err(|e| format!("Solver error: {}", e))?;

    // Print results
    println!("\n{}", "Schedule result:".green());
    println!("{}", format_schedule(&result));

    let elapsed = start_time.elapsed();
    println!("{}", format!("Total runtime: {:.2?}", elapsed).yellow());

    Ok(())
}
