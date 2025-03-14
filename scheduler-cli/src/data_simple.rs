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
            "Chicken and rice".to_string(),
            "food".to_string(),
            "meal".to_string(),
            "null".to_string(),
            "null".to_string(),
            "1x daily".to_string(),
            "[]".to_string(),                // no 'apart' constraints
            "[\"12:00-13:00\"]".to_string(), // has 1 range
            "some note".to_string(),
        ],
    ]
}
