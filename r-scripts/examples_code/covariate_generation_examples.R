# Covariate Generation Examples
#
# This file demonstrates how to:
# 1. Filter long format data using lookup tables
# 2. Generate covariates based on events before an index date
# 3. Generate covariates for specific time windows
# 4. Create comprehensive temporal covariate sets
#
# Author: SAIL BDCD Team
# Date: 2025-01-24

library(dplyr)
library(DBI)

# Setup paths
source("../utility_code/db2_connection.R")
source("../pipeline_code/read_db2_config_multi_source.R")
source("../pipeline_code/create_long_format_assets.R")
source("../pipeline_code/generate_covariates.R")

# Connect to database
conn <- create_db2_connection()
config <- read_db2_config("../pipeline_code/db2_config_multi_source.yaml")

# ==============================================================================
# EXAMPLE 1: Filter Long Format Data by Lookup Table
# ==============================================================================

# Suppose we have a long format asset for patient diagnoses or clinical events
# and we want to filter for specific condition codes

# Step 1: Create long format data from database
long_data <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "sex"
)

print(paste("Total records in long format:", nrow(long_data)))
print(paste("Unique patients:", n_distinct(long_data$patient_id)))

# Step 2: Create a lookup table for specific codes of interest
# In a real scenario, this might be loaded from a CSV or database table
diabetes_codes <- data.frame(
  code = c("E10", "E11", "E13", "E14"),
  name = c("Type 1 Diabetes", "Type 2 Diabetes", "Other Diabetes", "Unspecified Diabetes"),
  description = c(
    "Insulin-dependent diabetes mellitus",
    "Non-insulin-dependent diabetes mellitus",
    "Other specified diabetes mellitus",
    "Unspecified diabetes mellitus"
  ),
  terminology = c("ICD10", "ICD10", "ICD10", "ICD10"),
  stringsAsFactors = FALSE
)

# Example for sex codes (using actual data available)
sex_codes <- data.frame(
  code = c("M", "F"),
  name = c("Male", "Female"),
  description = c("Male gender", "Female gender"),
  terminology = c("GNDR", "GNDR"),
  stringsAsFactors = FALSE
)

# Step 3: Filter long format data using the lookup table
filtered_data <- filter_long_by_lookup(
  long_data = long_data,
  lookup_table = sex_codes,
  code_col_in_data = "sex_code",
  event_date_col = "record_date",
  add_lookup_info = TRUE  # Adds name, description, terminology columns
)

print(paste("Filtered records:", nrow(filtered_data)))
print("Sample of filtered data:")
print(head(filtered_data))

# Step 4: Filter with date range
recent_filtered_data <- filter_long_by_lookup(
  long_data = long_data,
  lookup_table = sex_codes,
  code_col_in_data = "sex_code",
  event_date_col = "record_date",
  event_date_range = c(as.Date("2020-01-01"), as.Date("2023-12-31")),
  add_lookup_info = FALSE
)

print(paste("Filtered records (2020-2023):", nrow(recent_filtered_data)))


# ==============================================================================
# EXAMPLE 2: Generate Covariates Before Index Date
# ==============================================================================

# Common use case: For a cohort study, generate covariates based on
# events that occurred before the index date (e.g., study enrollment date,
# diagnosis date, or treatment start date)

# Step 1: Define index dates for your cohort
# In a real scenario, these would come from your cohort definition
unique_patients <- long_data %>%
  select(patient_id) %>%
  distinct() %>%
  slice_head(n = 100)  # Example: first 100 patients

index_dates <- unique_patients %>%
  mutate(
    # Example: index date is 2023-01-01 for all patients
    # In reality, this would be patient-specific
    index_date = as.Date("2023-01-01")
  )

print("Index dates:")
print(head(index_dates))

# Step 2: Generate default covariates (count and indicator)
covariates_default <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date"
)

print("Default covariates:")
print(head(covariates_default))

# Step 3: Generate custom covariates with aggregation functions
covariates_custom <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  aggregation_functions = list(
    n_events = ~ n(),
    has_event = ~ n() > 0,
    earliest_event = ~ min(record_date, na.rm = TRUE),
    latest_event = ~ max(record_date, na.rm = TRUE),
    n_sources = ~ n_distinct(source_table),
    days_since_first_event = ~ as.numeric(first(index_date) - min(record_date))
  )
)

print("Custom covariates:")
print(head(covariates_custom))

# Step 4: Generate covariates with minimum lookback period
# Only include events at least 30 days before index
covariates_lookback <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  min_days_before = 30,  # Exclude events in last 30 days before index
  aggregation_functions = list(
    n_events = ~ n(),
    has_event = ~ n() > 0
  )
)

print("Covariates with 30-day lookback exclusion:")
print(head(covariates_lookback))


# ==============================================================================
# EXAMPLE 3: Generate Covariates for Time Windows
# ==============================================================================

# Common use case: Examine temporal patterns by creating covariates
# for different time periods before the index date

# Step 1: Create covariates for last 30 days before index
covariates_30d <- generate_covariates_time_window(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  days_before_start = 30,
  days_before_end = 1,  # Use 1 to exclude index date itself
  window_label = "last_30d"
)

print("Covariates for last 30 days:")
print(head(covariates_30d))

# Step 2: Create covariates for 30-90 days before index
covariates_30to90d <- generate_covariates_time_window(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  days_before_start = 90,
  days_before_end = 30,
  window_label = "30to90d"
)

print("Covariates for 30-90 days before index:")
print(head(covariates_30to90d))

# Step 3: Create covariates for 1-2 years before index
covariates_1to2y <- generate_covariates_time_window(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  days_before_start = 730,  # 2 years
  days_before_end = 365,    # 1 year
  window_label = "1to2y"
)

print("Covariates for 1-2 years before index:")
print(head(covariates_1to2y))


# ==============================================================================
# EXAMPLE 4: Generate Multiple Time Windows at Once
# ==============================================================================

# Most efficient approach: Generate all time windows in one call

# Define time windows
time_windows <- list(
  list(start = 30, end = 1, label = "last_30d"),
  list(start = 90, end = 31, label = "30to90d"),
  list(start = 180, end = 91, label = "90to180d"),
  list(start = 365, end = 181, label = "180dto1y"),
  list(start = 730, end = 366, label = "1to2y")
)

# Generate all covariates at once
all_covariates <- generate_multiple_time_windows(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  time_windows = time_windows
)

print("All time window covariates:")
print(head(all_covariates))
print(paste("Number of covariate columns:", ncol(all_covariates) - 2))  # Subtract patient_id and index_date


# ==============================================================================
# EXAMPLE 5: Complete Workflow - Multiple Conditions
# ==============================================================================

# Real-world scenario: Create covariates for multiple conditions with
# different time windows

# Step 1: Define multiple lookup tables for different conditions
hypertension_codes <- data.frame(
  code = c("I10", "I11", "I12", "I13"),
  name = c("Essential HTN", "HTN heart disease", "HTN kidney disease", "HTN heart+kidney"),
  description = c(
    "Essential (primary) hypertension",
    "Hypertensive heart disease",
    "Hypertensive chronic kidney disease",
    "Hypertensive heart and chronic kidney disease"
  ),
  terminology = rep("ICD10", 4),
  stringsAsFactors = FALSE
)

# In this example, we'll use sex codes as a proxy
# In real usage, you would have diagnosis/procedure codes

# Step 2: Filter for each condition
# (In this example, we're reusing sex data for demonstration)
condition1_events <- filter_long_by_lookup(
  long_data = long_data,
  lookup_table = sex_codes,
  code_col_in_data = "sex_code",
  add_lookup_info = FALSE
)

# Step 3: Generate covariates for each condition
# Create meaningful time windows for analysis
windows_detailed <- list(
  list(start = 30, end = 1, label = "recent"),      # Last month
  list(start = 180, end = 31, label = "medium"),    # 1-6 months ago
  list(start = 365, end = 181, label = "distant")   # 6-12 months ago
)

condition1_covariates <- generate_multiple_time_windows(
  long_data = condition1_events,
  index_dates = index_dates,
  event_date_col = "record_date",
  time_windows = windows_detailed
)

# Rename columns to indicate condition
names(condition1_covariates) <- gsub("^(recent|medium|distant)_",
                                      "condition1_\\1_",
                                      names(condition1_covariates))

print("Condition-specific covariates:")
print(head(condition1_covariates))


# ==============================================================================
# EXAMPLE 6: Custom Aggregation Functions for Domain-Specific Covariates
# ==============================================================================

# Create sophisticated aggregations for clinical research

custom_clinical_covariates <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  aggregation_functions = list(
    # Count of events
    n_events = ~ n(),

    # Indicator for any event
    has_event = ~ n() > 0,

    # Number of unique sources
    n_sources = ~ n_distinct(source_table),

    # Days since first event
    days_since_first = ~ as.numeric(first(index_date) - min(record_date)),

    # Days since last event
    days_since_last = ~ as.numeric(first(index_date) - max(record_date)),

    # Average time between events
    avg_days_between_events = ~ if(n() > 1) {
      mean(diff(sort(as.numeric(record_date))))
    } else {
      NA_real_
    },

    # Highest priority source used
    highest_priority_source = ~ source_table[which.min(source_priority)][1],

    # Whether data comes from multiple sources (potential conflicts)
    has_multiple_sources = ~ n_distinct(source_table) > 1
  )
)

print("Custom clinical covariates:")
print(head(custom_clinical_covariates))
print("Summary of days since last event:")
print(summary(custom_clinical_covariates$days_since_last))


# ==============================================================================
# EXAMPLE 7: Handling Missing Data and Edge Cases
# ==============================================================================

# Example with patients who may not have events

# Include patients without events (fill_missing_patients = TRUE by default)
covariates_with_missing <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  fill_missing_patients = TRUE  # Default
)

print("Patients without events:")
print(covariates_with_missing %>% filter(n_events == 0) %>% head())

# Exclude patients without events
covariates_no_missing <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = index_dates,
  event_date_col = "record_date",
  fill_missing_patients = FALSE
)

print(paste("Patients with events:", nrow(covariates_with_missing)))
print(paste("Patients with events (no fill):", nrow(covariates_no_missing)))


# ==============================================================================
# EXAMPLE 8: Combining with Other Assets
# ==============================================================================

# Real-world scenario: Combine demographic info with temporal covariates

# Get demographic long format data
demographics <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "date_of_birth"
)

# Get the highest priority demographic info per patient
demographics_wide <- demographics %>%
  group_by(patient_id) %>%
  filter(source_priority == min(source_priority)) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(patient_id, date_of_birth)

# Combine with temporal covariates
final_dataset <- covariates_custom %>%
  left_join(demographics_wide, by = "patient_id")

print("Combined dataset with demographics and temporal covariates:")
print(head(final_dataset))


# ==============================================================================
# EXAMPLE 9: Export Results
# ==============================================================================

# Save covariates to CSV for analysis
output_path <- "../output/covariates_example.csv"
if (!dir.exists("../output")) {
  dir.create("../output", recursive = TRUE)
}

write.csv(all_covariates, output_path, row.names = FALSE)
print(paste("Covariates saved to:", output_path))

# Or save to database
# DBI::dbWriteTable(conn, "COHORT_COVARIATES", all_covariates, overwrite = TRUE)


# Clean up
DBI::dbDisconnect(conn)

print("Examples completed successfully!")
