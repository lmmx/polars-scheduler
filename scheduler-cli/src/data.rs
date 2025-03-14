/// Module containing sample data for demonstration purposes

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
