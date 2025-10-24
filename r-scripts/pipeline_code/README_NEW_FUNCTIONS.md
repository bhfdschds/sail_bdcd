# New Pipeline Functions - User Guide

## Quick Start

This guide explains how to use the new cohort, covariate, and outcome generation functions.

---

## 1. Generate Cohort

### Purpose
Create a study cohort by applying demographic restrictions (age, sex, ethnicity, LSOA).

### Basic Usage

```r
source("pipeline_code/generate_cohort.R")

# Combine demographics assets
demographics <- combine_demographics(
  dob_asset = dob_data,
  sex_asset = sex_data,
  ethnicity_asset = ethnicity_data,
  lsoa_asset = lsoa_data
)

# Generate cohort
cohort <- generate_cohort(
  demographics_asset = demographics,
  index_date = as.Date("2024-01-01"),
  min_age = 18,
  max_age = 100,
  require_known_sex = TRUE,
  require_known_ethnicity = FALSE,
  require_lsoa = TRUE
)
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `demographics_asset` | data.frame | Combined demographics data | *required* |
| `index_date` | Date/vector | Single date or per-patient dates | *required* |
| `min_age` | numeric | Minimum age at index date | NULL (no limit) |
| `max_age` | numeric | Maximum age at index date | NULL (no limit) |
| `require_known_sex` | logical | Exclude missing sex | TRUE |
| `require_known_ethnicity` | logical | Exclude missing ethnicity | FALSE |
| `require_lsoa` | logical | Exclude missing LSOA | FALSE |

### Output

Data frame with:
- `patient_id` - Patient identifier
- `index_date` - Index date for each patient
- `age_at_index` - Age in years at index date
- `date_of_birth` - Date of birth
- `sex_code` - Sex code
- `ethnicity_code` - Ethnicity code (if provided)
- `lsoa_code` - LSOA code (if provided)

---

## 2. Generate Covariates

### Purpose
Create time-windowed covariates from disease/treatment data (lookback from index date).

### Basic Usage

```r
source("pipeline_code/generate_covariates.R")

# Create lookup table
lookup <- data.frame(
  code = c("E10", "E11", "I10", "I11"),
  name = c("diabetes", "diabetes", "hypertension", "hypertension"),
  description = c("Type 1 DM", "Type 2 DM", "Essential HTN", "Secondary HTN"),
  terminology = "ICD10"
)

# Generate single covariate
diabetes <- generate_covariates(
  disease_treatment_asset = disease_data,
  cohort = cohort,
  lookup_table = lookup,
  covariate_name = "diabetes",
  days_before_start = NULL,  # Any time before
  days_before_end = 0,       # Up to day before index
  selection_method = "min"   # First occurrence
)

# Generate multiple covariates
covariates <- generate_multiple_covariates(
  disease_treatment_asset = disease_data,
  cohort = cohort,
  lookup_table = lookup,
  covariate_names = c("diabetes", "hypertension")
)
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `disease_treatment_asset` | data.frame | Long format disease/treatment data | *required* |
| `cohort` | data.frame | Cohort with patient_id and index_date | *required* |
| `lookup_table` | data.frame | Code lookup (code, name, description, terminology) | *required* |
| `covariate_name` | character | Name to filter in lookup table | *required* |
| `days_before_start` | numeric | Start of lookback window (NULL = unlimited) | NULL |
| `days_before_end` | numeric | End of lookback window (0 = day before index) | 0 |
| `selection_method` | character | "min" (earliest) or "max" (latest) | "min" |
| `calculate_days_to_index` | logical | Calculate days between covariate and index | TRUE |
| `show_quality_report` | logical | Show quality assessment | TRUE |

### Temporal Windows

```
         days_before_start          days_before_end
              <-------|--------------------|----> Time
                      |                    | Index Date

Examples:
  NULL, 0     = Any time before index
  365, 0      = Within 365 days before index
  365, 30     = Between 365 and 30 days before index
```

### Output

Data frame with:
- `patient_id` - Patient identifier
- `index_date` - Index date
- `covariate_flag` - TRUE if covariate present
- `covariate_date` - Date of covariate event
- `days_to_index` - Days between covariate and index (negative)

---

## 3. Generate Outcomes

### Purpose
Create time-windowed outcomes from disease/treatment data (follow-up after index date).

### Basic Usage

```r
source("pipeline_code/generate_outcomes.R")

# Create lookup table
lookup <- data.frame(
  code = c("I21", "I22", "I63", "I64"),
  name = c("heart_attack", "heart_attack", "stroke", "stroke"),
  description = c("Acute MI", "Subsequent MI", "Cerebral infarction", "Stroke NOS"),
  terminology = "ICD10"
)

# Generate single outcome
heart_attack <- generate_outcomes(
  disease_treatment_asset = disease_data,
  cohort = cohort,
  lookup_table = lookup,
  outcome_name = "heart_attack",
  days_after_start = 0,      # From index date
  days_after_end = 365,      # Within 1 year
  selection_method = "min"   # First occurrence
)

# Generate multiple outcomes
outcomes <- generate_multiple_outcomes(
  disease_treatment_asset = disease_data,
  cohort = cohort,
  lookup_table = lookup,
  outcome_names = c("heart_attack", "stroke")
)
```

### Parameters

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `disease_treatment_asset` | data.frame | Long format disease/treatment data | *required* |
| `cohort` | data.frame | Cohort with patient_id and index_date | *required* |
| `lookup_table` | data.frame | Code lookup (code, name, description, terminology) | *required* |
| `outcome_name` | character | Name to filter in lookup table | *required* |
| `days_after_start` | numeric | Start of follow-up window (0 = index date) | 0 |
| `days_after_end` | numeric | End of follow-up window (NULL = unlimited) | NULL |
| `selection_method` | character | "min" (earliest) or "max" (latest) | "min" |
| `calculate_days_from_index` | logical | Calculate days between index and outcome | TRUE |
| `show_quality_report` | logical | Show quality assessment | TRUE |

### Temporal Windows

```
                days_after_start       days_after_end
      <-----------|--------------------|--------> Time
                  | Index Date         |

Examples:
  0, NULL     = Any time after index
  0, 365      = Within 365 days after index
  30, 365     = Between 30 and 365 days after index
```

### Output

Data frame with:
- `patient_id` - Patient identifier
- `index_date` - Index date
- `outcome_flag` - TRUE if outcome present
- `outcome_date` - Date of outcome event
- `days_from_index` - Days between index and outcome (positive)

---

## 4. Complete Pipeline Example

### Full Workflow

```r
# Source all functions
source("pipeline_code/create_long_format_assets.R")
source("pipeline_code/generate_cohort.R")
source("pipeline_code/generate_covariates.R")
source("pipeline_code/generate_outcomes.R")
source("utility_code/db2_connection.R")

# OR use the complete pipeline
source("examples_code/complete_pipeline_example.R")

# Run everything
results <- run_complete_pipeline(
  patient_ids = 1001:2000,
  index_date = as.Date("2024-01-01"),
  min_age = 18,
  max_age = 100,
  follow_up_days = 365
)

# Access results
cohort <- results$cohort
covariates <- results$covariates
outcomes <- results$outcomes
final_data <- results$final_data
```

---

## 5. Common Use Cases

### Use Case 1: Baseline Comorbidities

```r
# Get all comorbidities any time before index
comorbidities <- generate_multiple_covariates(
  disease_treatment_asset = hospital_data,
  cohort = cohort,
  lookup_table = comorbidity_lookup,
  covariate_names = c("diabetes", "hypertension", "copd", "chd"),
  days_before_start = NULL,  # Any time before
  days_before_end = 0
)
```

### Use Case 2: Recent Medications

```r
# Get medications within last 90 days
recent_meds <- generate_covariates(
  disease_treatment_asset = medicines_data,
  cohort = cohort,
  lookup_table = medication_lookup,
  covariate_name = "statin",
  days_before_start = 90,    # Last 90 days
  days_before_end = 0,
  selection_method = "max"   # Most recent
)
```

### Use Case 3: 30-Day Outcomes

```r
# Get outcomes within 30 days
short_term_outcomes <- generate_outcomes(
  disease_treatment_asset = hospital_data,
  cohort = cohort,
  lookup_table = outcome_lookup,
  outcome_name = "readmission",
  days_after_start = 0,
  days_after_end = 30
)
```

### Use Case 4: Time-to-Event Analysis

```r
# Get all events after index (for survival analysis)
survival_outcomes <- generate_outcomes(
  disease_treatment_asset = death_data,
  cohort = cohort,
  lookup_table = death_lookup,
  outcome_name = "death",
  days_after_start = 0,
  days_after_end = NULL,     # Unlimited follow-up
  calculate_days_from_index = TRUE  # For time-to-event
)

# Use days_from_index for survival::Surv()
library(survival)
surv_object <- Surv(
  time = survival_outcomes$days_from_index,
  event = survival_outcomes$outcome_flag
)
```

---

## 6. Data Requirements

### Demographics Asset
```r
data.frame(
  patient_id = ...,
  date_of_birth = ...,
  sex_code = ...,
  ethnicity_code = ...,  # Optional
  lsoa_code = ...        # Optional
)
```

### Disease/Treatment Asset
```r
data.frame(
  patient_id = ...,
  event_date = ...,
  code = ...,
  terminology = ...      # Optional
)
```

### Lookup Table
```r
data.frame(
  code = ...,           # Diagnostic/treatment code
  name = ...,           # Covariate/outcome name
  description = ...,    # Human-readable description
  terminology = ...     # ICD10, Read, SNOMED, etc.
)
```

---

## 7. Quality Checks

All functions include built-in quality reporting:

```r
generate_covariates(..., show_quality_report = TRUE)

# Shows:
# 1. Cohort Coverage
#    Patients with 'diabetes': 234 / 1000 (23.4%)
#
# 2. Timeframe Coverage
#    Date range: 2020-01-01 to 2023-12-31
#
# 3. Top Codes
#    code  name      n_events  n_patients
#    E11   diabetes  456       234
#    E10   diabetes  67        45
```

---

## 8. Tips and Best Practices

### ✅ Do:
- Use clear, descriptive covariate/outcome names
- Create comprehensive lookup tables with all relevant codes
- Always check quality reports before proceeding
- Export intermediate results for debugging
- Use `selection_method = "min"` for first diagnosis
- Use `selection_method = "max"` for most recent medication

### ❌ Don't:
- Mix different terminologies without mapping codes
- Use overlapping temporal windows without justification
- Ignore patients without covariates (they're included with flag = FALSE)
- Forget to specify index_date
- Use very wide temporal windows without clinical rationale

---

## 9. Troubleshooting

### Problem: No patients with covariate

**Check:**
1. Are codes correct in lookup table?
2. Is terminology matching (ICD10 vs Read)?
3. Are temporal windows too restrictive?
4. Is disease_asset filtered correctly?

### Problem: Too many missing covariates

**Check:**
1. Data quality of source tables
2. Temporal windows (are they appropriate?)
3. Code lists (are they comprehensive?)

### Problem: Unexpected patient counts

**Check:**
1. Print quality reports (`show_quality_report = TRUE`)
2. Check intermediate outputs
3. Verify cohort restrictions
4. Review temporal window logic

---

## 10. Testing

Run tests to verify installation:

```r
# Run all tests
testthat::test_dir("tests/testthat")

# Run specific tests
testthat::test_file("tests/testthat/test_generate_cohort.R")
testthat::test_file("tests/testthat/test_generate_covariates.R")
testthat::test_file("tests/testthat/test_generate_outcomes.R")
```

---

## Support

For issues or questions:
1. Check the [REFACTORING_SUMMARY.md](../../REFACTORING_SUMMARY.md) for overview
2. Review the [complete_pipeline_example.R](../examples_code/complete_pipeline_example.R)
3. Run the tests to verify your setup
4. Check function documentation in source files

---

*Last Updated: 2025-10-24*
