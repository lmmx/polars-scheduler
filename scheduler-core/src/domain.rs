use good_lp::variable::Variable;
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintType {
    Before,
    After,
    Apart,
    ApartFrom,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintRef {
    WithinGroup,
    Unresolved(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintExpr {
    pub time_hours: u32,
    pub ctype: ConstraintType,
    pub cref: ConstraintRef,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frequency {
    /// “N× daily” (e.g. "9x daily" => TimesPerDay(9)).
    TimesPerDay(u32),
}

impl Frequency {
    /// Parse strings like "3x daily" into an enum.
    /// Anything else returns an error.
    pub fn from_frequency_str(s: &str) -> Result<Self, String> {
        let input = s.trim().to_lowercase();
        let rx_times_daily = Regex::new(r"^(\d+)\s*x\s*daily$").unwrap();
        // "(\d+)x daily" => TimesPerDay(N)
        if let Some(caps) = rx_times_daily.captures(&input) {
            let n: u32 = caps[1]
                .parse()
                .map_err(|_| format!("Invalid integer in '{}'", s))?;
            return Ok(Frequency::TimesPerDay(n));
        }
        // If none matched, return an error
        Err(format!("Unrecognized frequency string: '{}'", s))
    }

    /// Returns the integer value stored in `TimesPerDay(n)`.
    pub fn instances_per_day(&self) -> usize {
        match self {
            Frequency::TimesPerDay(n) => *n as usize,
        }
    }
}

/// Represents a desired scheduling "window," which can be:
///   - A single anchor time (in minutes from midnight), e.g. 480 for 08:00
///   - A start–end range in minutes (e.g. 720..780 for 12:00–13:00)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowSpec {
    Anchor(i32),
    Range(i32, i32),
}

/// An "entity" to be scheduled.
/// - `constraints` are the typical "Apart", "Before", etc. constraints
/// - `windows` is optional extra data: if nonempty, the solver may need
///   to place this entity in one of these windows, or near these anchors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    pub name: String,
    pub category: String,
    pub frequency: Frequency,
    pub constraints: Vec<ConstraintExpr>,

    /// A list of windows (anchors or ranges) associated with this entity.
    /// If empty, the entity has no special windows and may be placed by global logic.
    pub windows: Vec<WindowSpec>,
}

#[derive(Clone)]
pub struct ClockVar {
    pub entity_name: String,
    pub instance: usize,
    pub var: Variable,
}

pub fn c2str(c: &ClockVar) -> String {
    format!("({}_var{})", c.entity_name, c.instance)
}

// Results returned by the scheduler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEvent {
    pub entity_name: String,
    pub instance: usize,
    pub time_minutes: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleResult {
    pub scheduled_events: Vec<ScheduledEvent>,
    pub total_penalty: f64,
    pub window_usage: Vec<(String, String, Vec<usize>)>, // (entity, window, instances)
}

// Configuration for the scheduling algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScheduleStrategy {
    Earliest,
    Latest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    pub day_start_minutes: i32,
    pub day_end_minutes: i32,
    pub strategy: ScheduleStrategy,
    pub global_windows: Vec<WindowSpec>,
    pub penalty_weight: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            day_start_minutes: 8 * 60,
            day_end_minutes: 22 * 60,
            strategy: ScheduleStrategy::Earliest,
            global_windows: Vec::new(),
            penalty_weight: 0.3,
        }
    }
}

// Struct for window info reporting
#[derive(Debug, Clone)]
pub struct WindowInfo {
    pub entity_name: String,
    pub window_index: usize,
    pub time_desc: String,
}
