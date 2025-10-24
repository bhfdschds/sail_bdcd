# Covariate Generation from Long Format Data

This guide explains how to generate temporal covariates from long format assets using lookup tables and time windows.

## Overview

The covariate generation pipeline provides three main capabilities:

1. **Filter long format data** using lookup tables for specific clinical codes
2. **Generate covariates** based on events before an index date
3. **Create temporal covariates** for specific time windows (e.g., "30-90 days before index")

## Table of Contents

- [Quick Start](#quick-start)
- [Core Functions](#core-functions)
- [Detailed Examples](#detailed-examples)
- [Advanced Usage](#advanced-usage)
- [Best Practices](#best-practices)

## Quick Start

```r
library(dplyr)
source("pipeline_code/generate_covariates.R")

# 1. Filter long format data using lookup table
filtered_data <- filter_long_by_lookup(
  long_data = your_long_format_data,
  lookup_table = your_codes,
  code_col_in_data = "diagnosis_code"
)

# 2. Generate covariates before index date
covariates <- generate_covariates_before_index(
  long_data = filtered_data,
  index_dates = your_index_dates,
  event_date_col = "admission_date"
)

# 3. Generate covariates for time windows
window_covariates <- generate_covariates_time_window(
  long_data = filtered_data,
  index_dates = your_index_dates,
  event_date_col = "admission_date",
  days_before_start = 90,
  days_before_end = 30,
  window_label = "30to90d"
)
```

## Core Functions

### Overview

The covariate generation pipeline provides 7 main functions:

1. **`filter_long_by_lookup()`** - Filter data by lookup table codes
2. **`generate_covariates_before_index()`** - General covariates before index date
3. **`generate_covariates_time_window()`** - Covariates for specific time windows
4. **`generate_multiple_time_windows()`** - Multiple time windows at once
5. **`generate_flag_covariates_by_name()`** - Binary flags based on lookup names
6. **`extract_value_by_name()`** - Extract min/max values by name
7. **`generate_covariates_by_name_window()`** - Covariates separated by name

### 1. `filter_long_by_lookup()`

Filters long format data to retain only rows matching codes in a lookup table.

**Parameters:**
- `long_data`: Data frame in long format (patient_id, source columns, data columns)
- `lookup_table`: Data frame with columns `code`, `name`, `description`, `terminology`
- `code_col_in_data`: Name of the code column in long_data (e.g., "icd10_code")
- `event_date_col`: Name of the event date column (default: "record_date")
- `event_date_range`: Optional vector of 2 dates: `c(start_date, end_date)`
- `terminology_filter`: Optional filter for specific terminologies
- `add_lookup_info`: If TRUE, adds name/description/terminology columns (default: TRUE)

**Returns:** Filtered data frame

**Example:**
```r
# Create lookup table
diabetes_codes <- data.frame(
  code = c("E10", "E11", "E13", "E14"),
  name = c("Type 1 DM", "Type 2 DM", "Other DM", "Unspecified DM"),
  description = c(
    "Type 1 diabetes mellitus",
    "Type 2 diabetes mellitus",
    "Other specified diabetes",
    "Unspecified diabetes"
  ),
  terminology = rep("ICD10", 4)
)

# Filter long format data
diabetes_events <- filter_long_by_lookup(
  long_data = hospital_admissions_long,
  lookup_table = diabetes_codes,
  code_col_in_data = "icd10_code",
  event_date_col = "admission_date",
  add_lookup_info = TRUE
)
```

### 2. `generate_covariates_before_index()`

Creates patient-level covariates from events occurring before an index date.

**Parameters:**
- `long_data`: Long format data with event-level information
- `index_dates`: Data frame with `patient_id` and `index_date` columns
- `event_date_col`: Name of event date column (default: "record_date")
- `patient_id_col`: Name of patient ID column (default: "patient_id")
- `aggregation_functions`: Named list of functions for aggregation (see below)
- `include_before_only`: If TRUE, excludes events on the index date (default: TRUE)
- `min_days_before`: Minimum days before index to include events (default: NULL)
- `fill_missing_patients`: If TRUE, includes patients with no events (default: TRUE)

**Returns:** Data frame with one row per patient

**Default Aggregation:**
```r
# Default creates:
# - n_events: Count of events
# - has_event: Boolean indicator
```

**Custom Aggregation Example:**
```r
covariates <- generate_covariates_before_index(
  long_data = diabetes_events,
  index_dates = cohort_index_dates,
  event_date_col = "admission_date",
  aggregation_functions = list(
    n_events = ~ n(),
    has_event = ~ n() > 0,
    earliest_event = ~ min(admission_date, na.rm = TRUE),
    latest_event = ~ max(admission_date, na.rm = TRUE),
    n_unique_codes = ~ n_distinct(icd10_code),
    n_sources = ~ n_distinct(source_table),
    days_since_first = ~ as.numeric(first(index_date) - min(admission_date))
  )
)
```

### 3. `generate_covariates_time_window()`

Creates covariates for events within a specific time window before index date.

**Parameters:**
- `long_data`: Long format event data
- `index_dates`: Data frame with `patient_id` and `index_date`
- `event_date_col`: Name of event date column
- `patient_id_col`: Name of patient ID column
- `days_before_start`: Start of time window (days before index)
- `days_before_end`: End of time window (days before index)
- `aggregation_functions`: Named list of aggregation functions
- `fill_missing_patients`: Include patients with no events in window
- `window_label`: Label prefix for covariate names (e.g., "30to90d")

**Time Window Logic:**
- `days_before_start`: Must be > `days_before_end`
- `days_before_end = 0`: Includes up to and including index date
- `days_before_end = 1`: Excludes index date
- Example: `start=90, end=30` means "30-90 days before index"

**Example:**
```r
# Events in last 30 days before index
last_30d <- generate_covariates_time_window(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  days_before_start = 30,
  days_before_end = 1,  # Exclude index date
  window_label = "last_30d"
)

# Result columns: patient_id, index_date, last_30d_n_events, last_30d_has_event
```

### 4. `generate_multiple_time_windows()`

Convenience function to generate covariates for multiple time windows at once.

**Parameters:**
- `long_data`: Long format event data
- `index_dates`: Data frame with patient_id and index_date
- `event_date_col`: Name of event date column
- `patient_id_col`: Name of patient ID column
- `time_windows`: List of window specifications (see example below)
- `aggregation_functions`: Named list of aggregation functions
- `fill_missing_patients`: Include patients with no events

**Example:**
```r
# Define multiple time windows
windows <- list(
  list(start = 30, end = 1, label = "last_30d"),
  list(start = 90, end = 31, label = "30to90d"),
  list(start = 180, end = 91, label = "90to180d"),
  list(start = 365, end = 181, label = "180dto1y"),
  list(start = 730, end = 366, label = "1to2y")
)

# Generate all covariates at once
all_covariates <- generate_multiple_time_windows(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  time_windows = windows
)

# Result includes columns for all windows:
# last_30d_n_events, last_30d_has_event
# 30to90d_n_events, 30to90d_has_event
# etc.
```

### 5. `generate_flag_covariates_by_name()`

Creates binary flags (0/1) for each unique 'name' from the lookup table. Optionally includes dates and days_between calculations.

**Parameters:**
- `long_data`: Long format data with 'name' column (must use `add_lookup_info=TRUE`)
- `index_dates`: Data frame with patient_id and index_date
- `event_date_col`: Name of event date column
- `patient_id_col`: Name of patient ID column
- `names_to_flag`: Specific names to create flags for (NULL = all names)
- `include_date`: "none", "earliest", "latest", or "both"
- `include_days_between`: TRUE for all names, or character vector for specific names
- `date_filter`: "before_index", "before_or_on_index", or "all"
- `time_window`: Optional list with 'start' and 'end' (days before index)

**Returns:**
- `{name}_flag`: Binary indicator (0/1)
- `{name}_earliest_date`: Earliest event date (if requested)
- `{name}_latest_date`: Latest event date (if requested)
- `{name}_days_between`: Days between latest event and index (if requested)

**Example:**
```r
# Filter with lookup info (REQUIRED for name-based functions)
diabetes_events <- filter_long_by_lookup(
  long_data = hospital_admissions,
  lookup_table = diabetes_codes,
  code_col_in_data = "icd10_code",
  add_lookup_info = TRUE  # Must be TRUE
)

# Create basic flags
flags <- generate_flag_covariates_by_name(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date"
)
# Result: Type_1_DM_flag, Type_2_DM_flag, etc.

# Include dates and days_between for specific names
flags_detailed <- generate_flag_covariates_by_name(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  include_date = "both",
  include_days_between = c("Type 1 DM", "Type 2 DM"),
  time_window = list(start = 365, end = 1)  # Last year only
)
# Result includes:
# Type_1_DM_flag, Type_1_DM_earliest_date, Type_1_DM_latest_date, Type_1_DM_days_between
```

### 6. `extract_value_by_name()`

Extracts min or max value for each lookup 'name' within a time window. Useful when patients have multiple events and you need the earliest/latest date or min/max measurement.

**Parameters:**
- `long_data`: Long format data with 'name' column
- `index_dates`: Data frame with patient_id and index_date
- `event_date_col`: Name of event date column
- `value_col`: Column to extract min/max from (NULL = use event_date_col)
- `patient_id_col`: Name of patient ID column
- `names_to_extract`: Specific names to extract (NULL = all names)
- `extract_function`: "min" or "max"
- `time_window`: Required list with 'start' and 'end' (days before index)
- `window_label`: Label for time window in column names

**Example:**
```r
# Get earliest admission date for each diabetes type in last year
earliest_admissions <- extract_value_by_name(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  value_col = "admission_date",  # Column to extract
  names_to_extract = c("Type 1 DM", "Type 2 DM"),
  extract_function = "min",
  time_window = list(start = 365, end = 1),
  window_label = "last_year"
)
# Result: last_year_Type_1_DM_min_admission_date, last_year_Type_2_DM_min_admission_date

# Get maximum HbA1c value for each test type in last 90 days
max_labs <- extract_value_by_name(
  long_data = lab_results,
  index_dates = index_dates,
  event_date_col = "test_date",
  value_col = "test_value",
  extract_function = "max",
  time_window = list(start = 90, end = 1),
  window_label = "last_90d"
)
```

### 7. `generate_covariates_by_name_window()`

Generates covariates separated by lookup 'name' within time windows. Creates separate columns for each name or aggregates across names.

**Parameters:**
- `long_data`: Long format data with 'name' column
- `index_dates`: Data frame with patient_id and index_date
- `event_date_col`: Name of event date column
- `patient_id_col`: Name of patient ID column
- `names_to_analyze`: Specific names to analyze (NULL = all)
- `time_window`: Optional list with 'start' and 'end'
- `window_label`: Label for time window
- `aggregation_functions`: Named list of aggregation functions
- `separate_by_name`: If TRUE, creates separate columns per name

**Example:**
```r
# Count events for each diabetes type separately
diabetes_by_type <- generate_covariates_by_name_window(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  names_to_analyze = c("Type 1 DM", "Type 2 DM"),
  time_window = list(start = 365, end = 1),
  window_label = "last_year",
  separate_by_name = TRUE
)
# Result: Type_1_DM_last_year_n_events, Type_2_DM_last_year_n_events

# Aggregate across all diabetes types
diabetes_combined <- generate_covariates_by_name_window(
  long_data = diabetes_events,
  index_dates = index_dates,
  event_date_col = "admission_date",
  time_window = list(start = 365, end = 1),
  window_label = "last_year",
  separate_by_name = FALSE
)
# Result: last_year_n_events (combines all diabetes types)
```

## Detailed Examples

### Example 1: Basic Workflow

```r
# Connect to database
conn <- create_db2_connection()
config <- read_db2_config("pipeline_code/db2_config_multi_source.yaml")

# Create long format asset
hospital_data <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "hospital_admissions"
)

# Define codes of interest
mi_codes <- data.frame(
  code = c("I21", "I22"),
  name = c("Acute MI", "Subsequent MI"),
  description = c("Acute myocardial infarction", "Subsequent myocardial infarction"),
  terminology = c("ICD10", "ICD10")
)

# Filter for MI events
mi_events <- filter_long_by_lookup(
  long_data = hospital_data,
  lookup_table = mi_codes,
  code_col_in_data = "icd10_code"
)

# Define cohort with index dates
cohort <- data.frame(
  patient_id = c(1001, 1002, 1003),
  index_date = as.Date(c("2023-01-01", "2023-03-15", "2023-06-20"))
)

# Generate covariates
mi_covariates <- generate_covariates_before_index(
  long_data = mi_events,
  index_dates = cohort,
  event_date_col = "admission_date"
)
```

### Example 2: Multiple Conditions with Time Windows

```r
# Define multiple condition codes
conditions <- list(
  diabetes = data.frame(
    code = c("E10", "E11", "E13", "E14"),
    name = c("Type 1 DM", "Type 2 DM", "Other DM", "Unspecified DM"),
    description = c("T1DM", "T2DM", "Other", "Unspecified"),
    terminology = rep("ICD10", 4)
  ),
  hypertension = data.frame(
    code = c("I10", "I11", "I12", "I13"),
    name = c("Essential HTN", "HTN heart", "HTN kidney", "HTN heart+kidney"),
    description = c("Essential HTN", "HTN heart disease", "HTN kidney", "HTN both"),
    terminology = rep("ICD10", 4)
  )
)

# Time windows for analysis
windows <- list(
  list(start = 30, end = 1, label = "recent"),
  list(start = 180, end = 31, label = "medium"),
  list(start = 365, end = 181, label = "distant")
)

# Generate covariates for each condition
covariate_list <- lapply(names(conditions), function(condition_name) {
  # Filter events
  events <- filter_long_by_lookup(
    long_data = hospital_data,
    lookup_table = conditions[[condition_name]],
    code_col_in_data = "icd10_code"
  )

  # Generate temporal covariates
  covariates <- generate_multiple_time_windows(
    long_data = events,
    index_dates = cohort,
    event_date_col = "admission_date",
    time_windows = windows
  )

  # Rename columns to include condition name
  names(covariates) <- gsub("^(recent|medium|distant)_",
                             paste0(condition_name, "_\\1_"),
                             names(covariates))

  return(covariates)
})

# Combine all covariates
final_covariates <- Reduce(function(x, y) {
  left_join(x, y, by = c("patient_id", "index_date"))
}, covariate_list)
```

### Example 3: Advanced Aggregations

```r
# Create sophisticated clinical covariates
clinical_covariates <- generate_covariates_before_index(
  long_data = diabetes_events,
  index_dates = cohort,
  event_date_col = "admission_date",
  aggregation_functions = list(
    # Basic counts
    n_admissions = ~ n(),
    has_admission = ~ n() > 0,

    # Temporal features
    days_since_first = ~ as.numeric(first(index_date) - min(admission_date)),
    days_since_last = ~ as.numeric(first(index_date) - max(admission_date)),
    days_between_admissions = ~ if(n() > 1) {
      mean(diff(sort(as.numeric(admission_date))))
    } else NA_real_,

    # Data quality features
    n_sources = ~ n_distinct(source_table),
    has_multiple_sources = ~ n_distinct(source_table) > 1,
    highest_priority_source = ~ source_table[which.min(source_priority)][1],

    # Clinical features
    n_unique_diagnoses = ~ n_distinct(icd10_code),
    most_common_diagnosis = ~ {
      codes <- table(icd10_code)
      names(codes)[which.max(codes)]
    }
  )
)
```

### Example 4: Handling Missing Data

```r
# Include all patients, even those without events
all_patients <- generate_covariates_before_index(
  long_data = mi_events,
  index_dates = cohort,
  event_date_col = "admission_date",
  fill_missing_patients = TRUE  # Default behavior
)

# Patients without events will have:
# - n_events = 0
# - has_event = FALSE

# Only include patients with events
patients_with_events <- generate_covariates_before_index(
  long_data = mi_events,
  index_dates = cohort,
  event_date_col = "admission_date",
  fill_missing_patients = FALSE
)
```

## Advanced Usage

### Custom Time Windows for Specific Research Questions

```r
# Perioperative risk assessment: events in different time windows
periop_windows <- list(
  list(start = 7, end = 1, label = "week_before"),
  list(start = 30, end = 8, label = "month_before"),
  list(start = 90, end = 31, label = "quarter_before")
)

periop_risk <- generate_multiple_time_windows(
  long_data = complication_events,
  index_dates = surgery_dates,
  event_date_col = "event_date",
  time_windows = periop_windows
)
```

### Minimum Lookback Period (Immortal Time Bias)

```r
# Exclude events in 30 days before index to avoid immortal time bias
covariates_no_immortal_time <- generate_covariates_before_index(
  long_data = medication_events,
  index_dates = cohort,
  event_date_col = "prescription_date",
  min_days_before = 30  # Events must be at least 30 days before index
)
```

### Combining with Wide Format Assets

```r
# Get demographic data (wide format)
demographics <- get_highest_priority_per_patient(demographics_long)

# Generate temporal covariates
temporal_covariates <- generate_covariates_before_index(
  long_data = event_data,
  index_dates = cohort,
  event_date_col = "event_date"
)

# Combine
final_dataset <- cohort %>%
  left_join(demographics, by = "patient_id") %>%
  left_join(temporal_covariates, by = c("patient_id", "index_date"))
```

## Best Practices

### 1. Lookup Table Structure

Always ensure your lookup tables have the required structure:

```r
lookup_table <- data.frame(
  code = c("code1", "code2"),           # Required
  name = c("Name 1", "Name 2"),          # Required
  description = c("Desc 1", "Desc 2"),   # Required
  terminology = c("ICD10", "ICD10"),     # Required
  stringsAsFactors = FALSE
)
```

### 2. Index Date Considerations

- Index dates should be **patient-specific** where possible
- Ensure index dates are **after** the events you're analyzing
- Use consistent date formats (use `as.Date()`)

```r
# Good
index_dates <- cohort %>%
  mutate(index_date = as.Date(diagnosis_date))

# Bad - same date for all patients may not be meaningful
index_dates <- cohort %>%
  mutate(index_date = as.Date("2023-01-01"))
```

### 3. Time Window Selection

Choose time windows based on your research question:

```r
# Acute conditions: shorter windows
acute_windows <- list(
  list(start = 7, end = 1, label = "week"),
  list(start = 30, end = 8, label = "month")
)

# Chronic conditions: longer windows
chronic_windows <- list(
  list(start = 180, end = 1, label = "6mo"),
  list(start = 365, end = 181, label = "6to12mo"),
  list(start = 730, end = 366, label = "1to2y")
)
```

### 4. Performance Tips

- **Filter before aggregating**: Use `filter_long_by_lookup()` first to reduce data size
- **Use appropriate patient subsets**: Don't process all patients if you only need a cohort
- **Batch processing**: For very large datasets, process in batches

```r
# Good - filter first
filtered <- filter_long_by_lookup(long_data, lookup_table, "code")
covariates <- generate_covariates_before_index(filtered, index_dates, "date")

# Less efficient - process all data then filter
```

### 5. Missing Data Handling

Be explicit about how you want to handle patients without events:

```r
# Include all patients (impute 0 for missing)
fill_missing_patients = TRUE

# Exclude patients without events
fill_missing_patients = FALSE
```

### 6. Date Column Names

Use consistent, descriptive date column names:

```r
# Good
event_date_col = "admission_date"
event_date_col = "prescription_date"
event_date_col = "procedure_date"

# Less clear
event_date_col = "date"
event_date_col = "dt"
```

## Common Patterns

### Pattern 1: Charlson Comorbidity Index Components

```r
# Define comorbidities
mi_codes <- data.frame(code = c("I21", "I22"), ...)
chf_codes <- data.frame(code = c("I50"), ...)
pvd_codes <- data.frame(code = c("I73.9"), ...)

# Generate binary indicators for each
mi_cov <- generate_covariates_before_index(
  filter_long_by_lookup(hosp_data, mi_codes, "icd10"),
  cohort, "admission_date",
  aggregation_functions = list(has_mi = ~ n() > 0)
)

# Combine all conditions
charlson_covariates <- mi_cov %>%
  left_join(chf_cov, by = c("patient_id", "index_date")) %>%
  left_join(pvd_cov, by = c("patient_id", "index_date"))
```

### Pattern 2: Healthcare Utilization Metrics

```r
utilization <- generate_covariates_before_index(
  long_data = all_encounters,
  index_dates = cohort,
  event_date_col = "encounter_date",
  aggregation_functions = list(
    n_ed_visits = ~ sum(encounter_type == "ED"),
    n_inpatient_days = ~ sum(length_of_stay, na.rm = TRUE),
    n_outpatient_visits = ~ sum(encounter_type == "Outpatient"),
    avg_time_between_visits = ~ if(n() > 1) {
      mean(diff(sort(as.numeric(encounter_date))))
    } else NA_real_
  )
)
```

### Pattern 3: Medication Adherence

```r
# Calculate medication possession ratio (simplified)
medication_adherence <- generate_covariates_before_index(
  long_data = prescription_events,
  index_dates = cohort,
  event_date_col = "fill_date",
  aggregation_functions = list(
    n_fills = ~ n(),
    days_supply_total = ~ sum(days_supply, na.rm = TRUE),
    adherence_ratio = ~ sum(days_supply, na.rm = TRUE) /
                       as.numeric(first(index_date) - min(fill_date))
  )
)
```

## Testing

Comprehensive tests are available in [test-generate-covariates.R](tests/testthat/test-generate-covariates.R).

Run tests:
```r
library(testthat)
test_file("tests/testthat/test-generate-covariates.R")
```

## See Also

- [Long Format Assets Documentation](README_LONG_FORMAT_EXAMPLES.md)
- [Multi-Source Configuration Guide](pipeline_code/db2_config_multi_source.yaml)
- [Example Code](examples_code/covariate_generation_examples.R)
