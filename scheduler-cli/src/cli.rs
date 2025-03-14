use scheduler_core::{parse_hhmm_to_minutes, ScheduleStrategy, SchedulerConfig, WindowSpec};
use std::env;

pub fn parse_config_from_args() -> SchedulerConfig {
    let args: Vec<String> = env::args().collect();
    let mut config = SchedulerConfig::default();

    // 1) Day window
    let parse_time_arg = |prefix: &str, minutes: &mut i32| {
        args.iter()
            .find_map(|arg| arg.strip_prefix(prefix))
            .and_then(|time_str| time_str.split_once(':'))
            .and_then(|(h_str, m_str)| {
                h_str
                    .parse::<i32>()
                    .ok()
                    .zip(m_str.parse::<i32>().ok())
                    .map(|(h, m)| *minutes = h * 60 + m)
            });
    };
    parse_time_arg("--start=", &mut config.day_start_minutes);
    parse_time_arg("--end=", &mut config.day_end_minutes);

    // 2) Strategy
    if args.iter().any(|a| a.eq_ignore_ascii_case("--latest")) {
        config.strategy = ScheduleStrategy::Latest;
    }

    // 3) Global windows: e.g. --windows=08:00,12:00-13:00,18:00
    if let Some(win_arg) = args.iter().find(|a| a.starts_with("--windows=")) {
        let raw = &win_arg["--windows=".len()..];
        config.global_windows = parse_windows_string(raw).unwrap_or_else(|e| {
            eprintln!("Warning: could not parse windows from '{}': {}", raw, e);
            Vec::new()
        });
    }

    // 4) Penalty weight
    if let Some(weight_arg) = args.iter().find(|a| a.starts_with("--penalty=")) {
        if let Ok(weight) = weight_arg["--penalty=".len()..].parse::<f64>() {
            config.penalty_weight = weight;
        }
    }

    // 5) Window tolerance
    if let Some(win_tol_arg) = args.iter().find(|a| a.starts_with("--tolerance=")) {
        if let Ok(win_tol) = weight_arg["--tolerance=".len()..].parse::<f64>() {
            config.window_tolerance = win_tol;
        }
    }

    config
}

// Parse a windows string like "08:00,12:00-13:00,18:00" into WindowSpec objects
fn parse_windows_string(input: &str) -> Result<Vec<WindowSpec>, String> {
    let parts: Vec<_> = input.split(',').map(|p| p.trim()).collect();
    let mut specs = Vec::new();

    for part in parts {
        if part.is_empty() {
            continue;
        }

        if let Some(idx) = part.find('-') {
            // Range
            let (start_str, end_str) = part.split_at(idx);
            let end_str = &end_str[1..];
            let start_min = parse_hhmm_to_minutes(start_str.trim())?;
            let end_min = parse_hhmm_to_minutes(end_str.trim())?;

            if end_min < start_min {
                return Err(format!("Invalid window range: {}", part));
            }

            specs.push(WindowSpec::Range(start_min, end_min));
        } else {
            // Anchor
            let anchor = parse_hhmm_to_minutes(part)?;
            specs.push(WindowSpec::Anchor(anchor));
        }
    }

    Ok(specs)
}
