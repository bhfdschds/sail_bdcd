# ===============================================================================
# Example 2: Conflict Detection and Quality Assessment
# ===============================================================================
# This example demonstrates advanced analysis techniques for assessing data
# quality and detecting conflicts across multiple sources.
#
# What you'll learn:
# - How to detect conflicts between sources
# - How to analyze data quality metrics
# - How to generate comprehensive summaries
# - How to compare sources side-by-side
# - How to export conflict reports for review
# ===============================================================================

# Load required libraries
library(dplyr)
library(tidyr)
library(DBI)
library(yaml)

# Source required scripts
source("utility_code/db2_connection.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")

# ===============================================================================
# Setup: Create Long Format Table
# ===============================================================================

config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
conn <- create_db2_connection(config)

# Create long format table for ethnicity
# (Use NULL for all patients, or provide specific patient IDs)
ethnicity_long <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "ethnicity",
  patient_ids = NULL
)

print("Long format table created successfully")
print(paste("Total rows:", nrow(ethnicity_long)))
print(paste("Unique patients:", n_distinct(ethnicity_long$patient_id)))

# ===============================================================================
# Part 1: Comprehensive Summary Statistics
# ===============================================================================

print("\n===== PART 1: SUMMARY STATISTICS =====\n")

# Generate comprehensive summary
summary <- summarize_long_format_table(ethnicity_long, "ethnicity")

# --- Overall Statistics ---
print("Overall Statistics:")
print(summary$overall)
# Shows:
# - total_rows: Total number of rows in long format
# - unique_patients: Number of distinct patients
# - num_sources: Number of different sources

# --- Per-Source Statistics ---
print("\nPer-Source Statistics:")
print(summary$by_source)
# Shows for each source:
# - patient_count: Number of patients from this source
# - row_count: Number of rows from this source
# - source_quality: Quality rating (high/medium/low)
# - source_coverage: Expected coverage (0.0 to 1.0)

# --- Patient Coverage Across Sources ---
print("\nPatient Coverage Across Sources:")
print(summary$patient_coverage)
# Shows how many patients appear in:
# - Only one source
# - Two sources
# - Three or more sources

# ===============================================================================
# Part 2: Conflict Detection
# ===============================================================================

print("\n===== PART 2: CONFLICT DETECTION =====\n")

# Detect conflicts in ethnicity_code
conflicts <- check_conflicts(
  ethnicity_long,
  asset_name = "ethnicity",
  key_column = "ethnicity_code"
)

print(paste("Number of patients with conflicts:", nrow(conflicts)))

# View conflict details
if (nrow(conflicts) > 0) {
  print("\nFirst 10 conflicts:")
  print(head(conflicts, 10))

  # Analyze conflict patterns
  print("\nConflict summary:")
  print(paste("  - Total conflicting patients:", nrow(conflicts)))
  print(paste("  - Percentage of all patients:",
              round(100 * nrow(conflicts) / n_distinct(ethnicity_long$patient_id), 2),
              "%"))
} else {
  print("No conflicts detected - all patients have consistent values across sources")
}

# ===============================================================================
# Part 3: Side-by-Side Source Comparison
# ===============================================================================

print("\n===== PART 3: SIDE-BY-SIDE COMPARISON =====\n")

# Pivot to wide format for easy comparison
ethnicity_wide <- pivot_to_wide_by_source(
  ethnicity_long,
  value_columns = c("ethnicity_code", "ethnicity_category")
)

print("Wide format structure:")
print(str(ethnicity_wide))

# View first few rows
print("\nFirst 10 rows of wide format:")
print(head(ethnicity_wide, 10))

# This creates columns like:
# - source1_ethnicity_code, source1_ethnicity_category
# - source2_ethnicity_code, source2_ethnicity_category
# - etc.

# Find patients with different codes across sources
if (nrow(ethnicity_wide) > 0 && "source1_ethnicity_code" %in% names(ethnicity_wide)) {
  # Get column names for ethnicity_code from different sources
  code_columns <- grep("source.*_ethnicity_code", names(ethnicity_wide), value = TRUE)

  if (length(code_columns) >= 2) {
    # Compare first two sources
    different_codes <- ethnicity_wide %>%
      filter(!is.na(.data[[code_columns[1]]]) & !is.na(.data[[code_columns[2]]])) %>%
      filter(.data[[code_columns[1]]] != .data[[code_columns[2]]])

    print(paste("\nPatients with different codes between sources:",
                nrow(different_codes)))

    if (nrow(different_codes) > 0) {
      print("\nExamples of differing codes:")
      print(head(different_codes, 5))
    }
  }
}

# ===============================================================================
# Part 4: Quality Assessment by Source
# ===============================================================================

print("\n===== PART 4: QUALITY ASSESSMENT =====\n")

# Analyze quality metrics
quality_analysis <- ethnicity_long %>%
  group_by(source_table, source_priority, source_quality, source_coverage) %>%
  summarise(
    n_patients = n_distinct(patient_id),
    n_records = n(),
    avg_records_per_patient = n() / n_distinct(patient_id),
    .groups = "drop"
  ) %>%
  arrange(source_priority)

print("Quality Analysis by Source:")
print(quality_analysis)

# Check for missing values in key columns
missing_analysis <- ethnicity_long %>%
  group_by(source_table) %>%
  summarise(
    total_rows = n(),
    missing_ethnicity_code = sum(is.na(ethnicity_code)),
    missing_ethnicity_category = sum(is.na(ethnicity_category)),
    pct_missing_code = round(100 * sum(is.na(ethnicity_code)) / n(), 2),
    pct_missing_category = round(100 * sum(is.na(ethnicity_category)) / n(), 2)
  )

print("\nMissing Value Analysis:")
print(missing_analysis)

# ===============================================================================
# Part 5: Temporal Analysis
# ===============================================================================

print("\n===== PART 5: TEMPORAL ANALYSIS =====\n")

# Analyze when sources were last updated
temporal_summary <- ethnicity_long %>%
  group_by(source_table, source_last_updated) %>%
  summarise(
    n_patients = n_distinct(patient_id),
    n_records = n(),
    .groups = "drop"
  ) %>%
  arrange(source_last_updated)

print("Source Update Timeline:")
print(temporal_summary)

# Identify patients with only old data
cutoff_date <- as.Date("2023-01-01")

if ("source_last_updated" %in% names(ethnicity_long)) {
  old_data_patients <- ethnicity_long %>%
    group_by(patient_id) %>%
    filter(all(as.Date(source_last_updated) < cutoff_date)) %>%
    pull(patient_id) %>%
    unique()

  print(paste("\nPatients with data only from before", cutoff_date, ":",
              length(old_data_patients)))
}

# ===============================================================================
# Part 6: Priority-Based Resolution
# ===============================================================================

print("\n===== PART 6: PRIORITY-BASED RESOLUTION =====\n")

# Resolve conflicts using priority
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)

print(paste("Patients before resolution:", n_distinct(ethnicity_long$patient_id)))
print(paste("Patients after resolution:", nrow(ethnicity_resolved)))

# Analyze which sources were selected
source_selection <- ethnicity_resolved %>%
  group_by(source_table, source_priority) %>%
  summarise(
    n_patients_selected = n(),
    .groups = "drop"
  ) %>%
  arrange(source_priority)

print("\nSource Selection After Priority Resolution:")
print(source_selection)

# Calculate percentage of patients from each source
source_selection <- source_selection %>%
  mutate(
    percentage = round(100 * n_patients_selected / sum(n_patients_selected), 2)
  )

print("\nPercentage of patients from each source:")
print(source_selection)

# ===============================================================================
# Part 7: Export Conflict Reports
# ===============================================================================

print("\n===== PART 7: EXPORT REPORTS =====\n")

# Create output directory
dir.create("conflict_reports", showWarnings = FALSE)

# Export conflict summary
if (nrow(conflicts) > 0) {
  write.csv(
    conflicts,
    "conflict_reports/ethnicity_conflicts.csv",
    row.names = FALSE
  )
  print("Exported: conflict_reports/ethnicity_conflicts.csv")
}

# Export wide format for manual review
write.csv(
  ethnicity_wide,
  "conflict_reports/ethnicity_source_comparison.csv",
  row.names = FALSE
)
print("Exported: conflict_reports/ethnicity_source_comparison.csv")

# Export quality analysis
write.csv(
  quality_analysis,
  "conflict_reports/quality_analysis.csv",
  row.names = FALSE
)
print("Exported: conflict_reports/quality_analysis.csv")

# Export missing value analysis
write.csv(
  missing_analysis,
  "conflict_reports/missing_values_analysis.csv",
  row.names = FALSE
)
print("Exported: conflict_reports/missing_values_analysis.csv")

# ===============================================================================
# Part 8: Advanced Filtering
# ===============================================================================

print("\n===== PART 8: ADVANCED FILTERING =====\n")

# Filter to high-quality sources only
high_quality <- ethnicity_long %>%
  filter(source_quality == "high")

print(paste("Rows with high quality sources:", nrow(high_quality)))
print(paste("Unique patients:", n_distinct(high_quality$patient_id)))

# Resolve using only high-quality sources
ethnicity_high_quality_resolved <- get_highest_priority_per_patient(high_quality)

# Compare to full resolution
comparison <- data.frame(
  method = c("All sources", "High quality only"),
  n_patients = c(
    nrow(ethnicity_resolved),
    nrow(ethnicity_high_quality_resolved)
  )
)

print("\nComparison of resolution methods:")
print(comparison)

# ===============================================================================
# Part 9: Identifying Problematic Patients
# ===============================================================================

print("\n===== PART 9: PROBLEMATIC PATIENTS =====\n")

# Find patients appearing in multiple sources with different values
problematic_patients <- ethnicity_long %>%
  group_by(patient_id) %>%
  filter(n_distinct(ethnicity_code, na.rm = TRUE) > 1) %>%
  ungroup()

if (nrow(problematic_patients) > 0) {
  print(paste("Patients with conflicting ethnicity codes:",
              n_distinct(problematic_patients$patient_id)))

  # Get details for first few problematic patients
  example_conflicts <- problematic_patients %>%
    group_by(patient_id) %>%
    slice_head(n = 1) %>%
    ungroup() %>%
    head(5)

  print("\nExample problematic patients:")
  for (i in 1:nrow(example_conflicts)) {
    pid <- example_conflicts$patient_id[i]
    patient_details <- ethnicity_long %>%
      filter(patient_id == pid) %>%
      select(patient_id, source_table, source_priority, ethnicity_code, ethnicity_category)

    print(paste("\nPatient", pid, ":"))
    print(patient_details)
  }
}

# Find patients with data from only low-quality sources
low_quality_only <- ethnicity_long %>%
  group_by(patient_id) %>%
  filter(all(source_quality == "low")) %>%
  pull(patient_id) %>%
  unique()

print(paste("\nPatients with data only from low-quality sources:",
            length(low_quality_only)))

# ===============================================================================
# Clean Up
# ===============================================================================

DBI::dbDisconnect(conn)
print("\nDatabase connection closed.")

# ===============================================================================
# Summary
# ===============================================================================

print("\n===== SUMMARY =====\n")

# What we did:
# 1. Generated comprehensive summary statistics
# 2. Detected conflicts between sources
# 3. Created side-by-side source comparisons
# 4. Assessed data quality by source
# 5. Analyzed temporal patterns
# 6. Performed priority-based resolution
# 7. Exported conflict reports
# 8. Applied quality-based filtering
# 9. Identified problematic patients

# Key insights:
# - Long format enables transparent conflict detection
# - Source metadata (quality, coverage, priority) supports informed decisions
# - Multiple analysis approaches (summary, conflicts, wide format) provide
#   different perspectives on data quality
# - Conflicts can be resolved automatically (priority) or manually (export)

print("Analysis complete! Check the 'conflict_reports' directory for outputs.")

# ===============================================================================
# Next Steps
# ===============================================================================

# Try modifying this script to:
# 1. Analyze conflicts in other assets (date_of_birth, lsoa)
# 2. Implement custom conflict resolution rules
# 3. Compare different quality thresholds
# 4. Investigate specific patient cohorts

# See other examples:
# - 01_basic_usage.R - Basic workflow
# - 03_complete_pipeline.R - Running the full pipeline for all assets
