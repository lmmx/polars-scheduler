use good_lp::variable::Variable;
use serde::{Serialize, Deserialize};

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
    Daily,
    TwiceDaily,
    ThreeTimesDaily,
    EveryXHours(u32),
}

impl Frequency {
    pub fn from_str(s: &str) -> Self {
        let lower = s.to_lowercase();
        match () {
            _ if lower.contains("3x") => Self::ThreeTimesDaily,
            _ if lower.contains("2x") => Self::TwiceDaily,
            _ if lower.contains("1x") => Self::Daily,
            _ => Self::EveryXHours(8),
        }
    }

    pub fn instances_per_day(&self) -> usize {
        match self {
            Self::Daily => 1,
            Self::TwiceDaily => 2,
            Self::ThreeTimesDaily => 3,
            Self::EveryXHours(h) => 24 / (*h as usize),
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