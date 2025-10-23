# Data Preprocessing Pipeline Documentation

## Overview

This document describes the data preprocessing module that integrates with the SAIL asset creation pipeline. The preprocessing module provides a flexible, configurable framework for cleaning, validating, and enriching data before it flows into the asset creation process.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Preprocessing Types](#preprocessing-types)
4. [Configuration Structure](#configuration-structure)
5. [Integration Points](#integration-points)
6. [Usage Examples](#usage-examples)
7. [Function Reference](#function-reference)

---

## Architecture Overview

### Module Components

The preprocessing system consists of three main components:

```
┌─────────────────────────────────────────────────────────────┐
│                    PREPROCESSING SYSTEM                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  data_preprocessing.R                                 │  │
│  │  - Core preprocessing functions                       │  │
│  │  - Code matching logic                               │  │
│  │  - Validation rules                                  │  │
│  │  - Covariate/outcome flagging                        │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  preprocessing_integration.R                          │  │
│  │  - Pipeline integration functions                     │  │
│  │  - Configuration reader                              │  │
│  │  - Cohort data management                            │  │
│  │  - Workflow orchestration                            │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  preprocessing_config.yaml                            │  │
│  │  - Preprocessing step definitions                     │  │
│  │  - Dataset mapping configuration                     │  │
│  │  - Lookup table references                           │  │
│  │  - Cohort baseline settings                          │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Modularity**: Each preprocessing step is independent and can be applied in any order
2. **Configurability**: All preprocessing logic is defined in YAML configuration files
3. **Extensibility**: New preprocessing types can be added without modifying existing code
4. **Reusability**: Preprocessing configurations can be shared across multiple datasets
5. **Integration**: Seamlessly integrates with existing asset creation pipeline

---

## Data Flow

### Complete Pipeline Flow with Preprocessing

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. CONFIGURATION LOADING                                         │
│    • Load database config (db2_config_multi_source.yaml)        │
│    • Load preprocessing config (preprocessing_config.yaml)       │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 2. DATABASE CONNECTION                                           │
│    • Create DB2 connection                                       │
│    • Validate connection                                         │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 3. COHORT PREPARATION (if needed)                               │
│    • Load or create cohort with baseline dates                  │
│    • Required for covariate/outcome flagging                    │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 4. ASSET DATA RETRIEVAL                                         │
│    For each asset/source:                                       │
│    • Build SQL query with column mapping                        │
│    • Execute query and retrieve raw data                        │
│    • Add source metadata                                        │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 5. PREPROCESSING (NEW!)                                         │
│    For each dataset with preprocessing configured:              │
│                                                                  │
│    a) Code Matching                                             │
│       • Match codes against lookup tables                       │
│       • Enrich data with additional attributes                  │
│       • Filter based on code matches                            │
│                                                                  │
│    b) Value Validation                                          │
│       • Check numeric ranges                                    │
│       • Validate against allowed values                         │
│       • Flag or filter invalid records                          │
│                                                                  │
│    c) Covariate Flagging                                        │
│       • Identify events before baseline date                    │
│       • Create patient-level covariate flags                    │
│       • Link to disease/condition lookup tables                 │
│                                                                  │
│    d) Outcome Flagging                                          │
│       • Identify events after baseline date                     │
│       • Record date of first outcome occurrence                 │
│       • Create patient-level outcome flags                      │
│                                                                  │
│    e) Data Transformation                                       │
│       • Type conversion (date, numeric, etc.)                   │
│       • String cleaning and standardization                     │
│       • Categorical remapping                                   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 6. LONG FORMAT COMBINATION                                      │
│    • Combine all sources: bind_rows()                           │
│    • Sort by patient_id and source_priority                     │
│    • Standardize column names                                   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 7. CONFLICT DETECTION & ANALYSIS                                │
│    • Summarize long format table                                │
│    • Check for conflicts across sources                         │
│    • Generate quality reports                                   │
└────────────────────────┬────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────────┐
│ 8. EXPORT                                                       │
│    • Export to CSV or RDS format                                │
│    • Save to specified output directory                         │
└─────────────────────────────────────────────────────────────────┘
```

### Preprocessing Step Flow Detail

```
Input Data
    ↓
┌───────────────────────────────────┐
│ Get Preprocessing Config          │
│ - Look up dataset name            │
│ - Load preprocessing steps        │
│ - Check if enabled                │
└───────────┬───────────────────────┘
            ↓
┌───────────────────────────────────┐
│ For Each Preprocessing Step:     │
│                                   │
│  1. Validate configuration        │
│  2. Load lookup data if needed    │
│  3. Apply transformation          │
│  4. Handle errors gracefully      │
│  5. Log results                   │
└───────────┬───────────────────────┘
            ↓
Preprocessed Data
```

---

## Preprocessing Types

### 1. Code Matching (`type: "code_match"`)

**Purpose**: Match codes in data against reference lookup tables to enrich or filter data.

**Use Cases**:
- Matching lab test codes to get test names and normal ranges
- Matching diagnosis codes (ICD-10) to get disease descriptions
- Matching medication codes (BNF) to get drug names and classifications
- Filtering data to include only relevant codes

**Configuration Parameters**:

```yaml
code_column: "test_code"              # Column in data with codes
lookup_source: "CHOLESTEROL_CODES"    # Lookup table name
source_type: "database"               # database, csv, or inline
schema: "SAIL"                        # Database schema
match_column: "code"                  # Column in lookup table
output_columns:                       # Columns to add from lookup
  - "test_name"
  - "test_category"
join_type: "left"                     # left, inner, or semi
```

**Example**:

```r
# Before code matching:
# patient_id | test_code | result_value
# 001        | CHOL001   | 5.2
# 002        | CHOL002   | 4.8

# After code matching:
# patient_id | test_code | result_value | test_name    | units
# 001        | CHOL001   | 5.2          | Total Chol.  | mmol/L
# 002        | CHOL002   | 4.8          | LDL Chol.    | mmol/L
```

---

### 2. Value Validation (`type: "value_validation"`)

**Purpose**: Validate that values fall within expected ranges and handle invalid data.

**Use Cases**:
- Range checking lab results (e.g., cholesterol 1-15 mmol/L)
- Date range validation
- Checking against allowed categorical values
- Quality control flagging

**Configuration Parameters**:

```yaml
column: "result_value"
min_value: 1.0                        # Minimum allowed value
max_value: 15.0                       # Maximum allowed value
allowed_values: ["M", "F", "U"]       # For categorical validation
action: "flag"                        # flag, filter, or transform
flag_column: "cholesterol_valid"      # Name of flag column to create
```

**Actions**:
- `flag`: Add a boolean column indicating validity (keeps all records)
- `filter`: Remove invalid records from dataset
- `transform`: Replace invalid values with specified transform_value (e.g., NA)

**Example**:

```r
# Before validation (action: "flag"):
# patient_id | cholesterol | cholesterol_valid
# 001        | 5.2         | TRUE
# 002        | 45.0        | FALSE  (out of range)
# 003        | NA          | TRUE   (NA allowed)
```

---

### 3. Covariate Flagging (`type: "covariate_flag"`)

**Purpose**: Flag baseline covariates - events that occur BEFORE the patient's baseline/index date.

**Use Cases**:
- Identifying pre-existing conditions (e.g., diabetes at baseline)
- Flagging historical medication use
- Baseline risk factors
- Study inclusion/exclusion criteria

**Configuration Parameters**:

```yaml
covariate_name: "heart_disease"
code_column: "diagnosis_code"
event_date_column: "diagnosis_date"
flag_column: "has_heart_disease"

# Option 1: Provide codes directly
event_codes: ["I21", "I22", "I25"]

# Option 2: Load from lookup table
code_lookup_table: "HEART_DISEASE_ICD10"
code_filter: "active = 1"

# Option 3: Load from CSV file
code_file: "/path/to/codes.csv"

aggregate_to_patient: true            # Create patient-level flags
```

**Requirements**:
- Requires cohort data with `patient_id` and `baseline_date` columns
- Baseline date can be fixed, loaded from database, or calculated from events

**Example**:

```r
# Cohort data:
# patient_id | baseline_date
# 001        | 2020-01-01
# 002        | 2020-01-01

# Event data:
# patient_id | diagnosis_code | diagnosis_date
# 001        | I21           | 2019-06-15  (BEFORE baseline)
# 001        | I50           | 2020-03-10  (AFTER baseline)
# 002        | I21           | 2020-06-20  (AFTER baseline)

# Result (aggregate_to_patient: true):
# patient_id | has_heart_disease
# 001        | TRUE   (had I21 before baseline)
# 002        | FALSE  (no heart disease codes before baseline)
```

---

### 4. Outcome Flagging (`type: "outcome_flag"`)

**Purpose**: Flag outcomes - events that occur AFTER the patient's baseline/index date.

**Use Cases**:
- Identifying disease outcomes (e.g., stroke after baseline)
- Time-to-event analysis
- Follow-up events
- Treatment outcomes

**Configuration Parameters**:

```yaml
outcome_name: "stroke"
code_column: "diagnosis_code"
event_date_column: "admission_date"
flag_column: "has_stroke_outcome"
date_column: "stroke_date"            # Column for first event date

event_codes: ["I60", "I61", "I63", "I64"]

aggregate_to_patient: true
include_date: true                    # Include date of first outcome
```

**Example**:

```r
# Cohort data:
# patient_id | baseline_date
# 001        | 2020-01-01
# 002        | 2020-01-01

# Event data:
# patient_id | diagnosis_code | admission_date
# 001        | I63           | 2020-06-15  (AFTER baseline - OUTCOME!)
# 001        | I50           | 2019-03-10  (BEFORE baseline - not outcome)
# 002        | K50           | 2020-06-20  (not stroke code)

# Result (aggregate_to_patient: true, include_date: true):
# patient_id | has_stroke_outcome | stroke_date
# 001        | TRUE               | 2020-06-15
# 002        | FALSE              | NA
```

---

### 5. Data Transformation (`type: "data_transformation"`)

**Purpose**: Apply data type conversions and standardization.

**Use Cases**:
- Converting string dates to Date objects
- Converting character values to numeric
- String cleaning (trimming, case conversion)
- Categorical value remapping

**Configuration Parameters**:

```yaml
transform_type: "date_conversion"     # See types below
column: "result_date"
date_format: "%Y-%m-%d"               # For date conversion
mapping:                              # For categorical mapping
  "1": "Male"
  "2": "Female"
```

**Transform Types**:
- `date_conversion`: Convert to Date type
- `numeric_conversion`: Convert to numeric
- `string_cleaning`: Trim whitespace and uppercase
- `categorical_mapping`: Remap values using lookup dictionary

---

## Configuration Structure

### Main Configuration File Structure

```yaml
# preprocessing_config.yaml

# 1. Preprocessing Definitions
preprocessing:
  [config_name]:                      # Reusable preprocessing config
    [step_name]:
      type: "[preprocessing_type]"
      [parameters...]

# 2. Cohort Configuration
cohort:
  baseline_strategy: "fixed"          # fixed, database, or event
  fixed_date: "2020-01-01"           # For fixed strategy
  # OR
  baseline_table: "STUDY_COHORT"     # For database strategy
  baseline_schema: "SAIL"
  patient_id_column: "patient_id"
  baseline_date_column: "index_date"

# 3. Dataset Preprocessing Mapping
dataset_preprocessing_map:
  [TABLE_NAME]:
    preprocessing: "[config_name]"
    enabled: true

# 4. Lookup Tables Configuration
lookup_tables:
  [LOOKUP_NAME]:
    table_name: "REFERENCE_TABLE"
    schema: "SAIL"
    description: "Description"

# 5. Global Settings
settings:
  continue_on_error: true
  add_preprocessing_metadata: true
  log_level: "INFO"
```

### Example: Complete Cholesterol Preprocessing

```yaml
preprocessing:
  cholesterol_data:

    # Step 1: Match test codes
    match_codes:
      type: "code_match"
      code_column: "test_code"
      lookup_source: "CHOLESTEROL_CODES"
      source_type: "database"
      match_column: "code"
      output_columns: ["test_name", "units", "normal_min", "normal_max"]
      join_type: "inner"

    # Step 2: Validate values
    validate_values:
      type: "value_validation"
      column: "result_value"
      min_value: 1.0
      max_value: 15.0
      action: "flag"
      flag_column: "value_valid"

    # Step 3: Convert dates
    convert_dates:
      type: "data_transformation"
      transform_type: "date_conversion"
      column: "result_date"
      date_format: "%Y-%m-%d"

dataset_preprocessing_map:
  GP_CHOLESTEROL_TESTS:
    preprocessing: "cholesterol_data"
    enabled: true
```

---

## Integration Points

### Integration with Existing Pipeline

The preprocessing module integrates at **Step 5** of the existing pipeline, between data retrieval and long format combination.

#### Option 1: Enhanced Get Asset Data

```r
# Modified function that includes preprocessing
data <- get_asset_data_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "ethnicity",
  source_name = "gp_ethnicity",
  conn = conn,
  apply_preprocessing = TRUE,
  cohort_data = cohort_data
)
```

#### Option 2: Enhanced Long Format Asset Creation

```r
# Modified long format creation with preprocessing
long_data <- create_long_format_asset_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "ethnicity",
  conn = conn,
  apply_preprocessing = TRUE,
  cohort_data = cohort_data
)
```

#### Option 3: Complete Pipeline Workflow

```r
# End-to-end pipeline with preprocessing
results <- run_preprocessing_pipeline(
  db_config_file = "db2_config_multi_source.yaml",
  preprocessing_config_file = "preprocessing_config.yaml",
  asset_names = c("ethnicity", "lsoa", "date_of_birth"),
  output_dir = "/mnt/user-data/outputs/"
)
```

### Backward Compatibility

The preprocessing module is **fully backward compatible**:

- If no preprocessing config is provided, the pipeline works exactly as before
- If preprocessing is configured but `apply_preprocessing = FALSE`, it's skipped
- Existing scripts continue to work without modification
- New scripts can opt-in to preprocessing features

---

## Usage Examples

### Example 1: Simple Code Matching and Validation

```r
# Load libraries
library(dplyr)
source("pipeline_code/data_preprocessing.R")
source("pipeline_code/preprocessing_integration.R")
source("utility_code/db2_connection.R")

# Load configurations
db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

# Create connection
conn <- create_db2_connection(db_config)

# Get cholesterol data with preprocessing
cholesterol_data <- get_asset_data_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "cholesterol",
  source_name = "gp_cholesterol",
  conn = conn,
  apply_preprocessing = TRUE
)

# View results
summary(cholesterol_data)
table(cholesterol_data$cholesterol_valid)

DBI::dbDisconnect(conn)
```

### Example 2: Covariate and Outcome Flagging

```r
# Load configurations
db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

# Create connection
conn <- create_db2_connection(db_config)

# Prepare cohort with baseline dates
cohort_data <- prepare_cohort_data(preprocessing_config, conn)

# Process diagnosis data with covariate/outcome flagging
diagnosis_data <- get_asset_data_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "diagnoses",
  source_name = "hospital_diagnoses",
  conn = conn,
  apply_preprocessing = TRUE,
  cohort_data = cohort_data  # Required for covariate/outcome flagging
)

# Check covariate flags
table(diagnosis_data$has_heart_disease)
table(diagnosis_data$has_diabetes)

# Check outcome flags
table(diagnosis_data$has_stroke_outcome)

DBI::dbDisconnect(conn)
```

### Example 3: Complete Pipeline with Multiple Assets

```r
# Run complete preprocessing pipeline
results <- run_preprocessing_pipeline(
  db_config_file = "pipeline_code/db2_config_multi_source.yaml",
  preprocessing_config_file = "pipeline_code/preprocessing_config.yaml",
  asset_names = c("cholesterol", "diagnoses", "prescriptions", "lab_results"),
  output_dir = "/mnt/user-data/outputs/preprocessed/"
)

# Access individual results
cholesterol <- results$cholesterol
diagnoses <- results$diagnoses

# Summary statistics
lapply(results, function(x) {
  list(
    n_rows = nrow(x),
    n_patients = length(unique(x$patient_id)),
    n_columns = ncol(x)
  )
})
```

### Example 4: Adding Preprocessing to Existing Long Format Pipeline

```r
# Original pipeline code (create_long_format_assets.R)
# Modified to include preprocessing

library(yaml)
source("pipeline_code/data_preprocessing.R")
source("pipeline_code/preprocessing_integration.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")
source("utility_code/db2_connection.R")

# Load both configs
db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

# Create connection
conn <- create_db2_connection(db_config)

# Prepare cohort
cohort_data <- prepare_cohort_data(preprocessing_config, conn)

# Create long format assets with preprocessing
assets <- c("ethnicity", "lsoa", "date_of_birth")

for (asset_name in assets) {
  message(sprintf("\n=== Processing asset: %s ===", asset_name))

  long_data <- create_long_format_asset_with_preprocessing(
    config = db_config,
    preprocessing_config = preprocessing_config,
    asset_name = asset_name,
    conn = conn,
    apply_preprocessing = TRUE,
    cohort_data = cohort_data
  )

  # Generate summary
  summary_data <- summarize_long_format_table(long_data, asset_name)
  print(summary_data)

  # Check conflicts
  conflicts <- check_conflicts(long_data, asset_name)

  # Export
  export_asset_table(long_data, asset_name, format = "rds")
}

DBI::dbDisconnect(conn)
```

---

## Function Reference

### Core Preprocessing Functions (`data_preprocessing.R`)

| Function | Purpose |
|----------|---------|
| `apply_preprocessing()` | Main entry point - applies all preprocessing steps |
| `apply_code_matching()` | Match codes against lookup tables |
| `apply_value_validation()` | Validate values against ranges and allowed values |
| `apply_covariate_flagging()` | Flag events before baseline date |
| `apply_outcome_flagging()` | Flag events after baseline date |
| `apply_data_transformation()` | Apply type conversions and transformations |
| `get_lookup_table()` | Load lookup table from database, CSV, or inline |
| `get_event_codes()` | Extract event codes from config or lookup |

### Integration Functions (`preprocessing_integration.R`)

| Function | Purpose |
|----------|---------|
| `read_preprocessing_config()` | Load preprocessing configuration from YAML |
| `get_dataset_preprocessing_config()` | Get preprocessing config for specific dataset |
| `apply_preprocessing_to_asset_data()` | Wrapper for applying preprocessing to asset data |
| `prepare_cohort_data()` | Create or load cohort with baseline dates |
| `get_asset_data_with_preprocessing()` | Enhanced asset data retrieval with preprocessing |
| `create_long_format_asset_with_preprocessing()` | Enhanced long format creation with preprocessing |
| `run_preprocessing_pipeline()` | Complete end-to-end pipeline workflow |

### Function Call Hierarchy

```
run_preprocessing_pipeline()
├── read_preprocessing_config()
├── prepare_cohort_data()
└── create_long_format_asset_with_preprocessing()
    ├── get_asset_data()  [existing function]
    └── apply_preprocessing_to_asset_data()
        ├── get_dataset_preprocessing_config()
        └── apply_preprocessing()
            ├── apply_code_matching()
            │   └── get_lookup_table()
            ├── apply_value_validation()
            ├── apply_covariate_flagging()
            │   └── get_event_codes()
            ├── apply_outcome_flagging()
            │   └── get_event_codes()
            └── apply_data_transformation()
```

---

## Best Practices

### Configuration Management

1. **Modular Configs**: Create reusable preprocessing configurations for common patterns
2. **Version Control**: Keep preprocessing configs under version control
3. **Documentation**: Document the purpose and rationale for each preprocessing step
4. **Testing**: Test preprocessing on sample data before applying to full datasets

### Performance Optimization

1. **Lookup Tables**: Cache frequently-used lookup tables in memory
2. **Filtering**: Apply code matching with `join_type: "inner"` early to reduce data volume
3. **Validation**: Use `action: "filter"` to remove invalid records early
4. **Cohort Size**: When possible, filter to specific patient cohort before preprocessing

### Error Handling

1. **Validation**: Always validate preprocessing configuration before running pipeline
2. **Logging**: Enable detailed logging for troubleshooting
3. **Graceful Degradation**: Use `continue_on_error: true` to process remaining data if one step fails
4. **Audit Trail**: Keep track of which preprocessing steps were applied and their results

### Data Quality

1. **Validation Flags**: Use `action: "flag"` to retain all data with validity indicators
2. **Metadata**: Enable `add_preprocessing_metadata: true` to track preprocessing provenance
3. **Quality Reports**: Generate summary statistics before and after preprocessing
4. **Conflict Detection**: Check for conflicts after preprocessing, especially for code matching

---

## Troubleshooting

### Common Issues

**Issue**: "Lookup table not found"
- **Solution**: Check `lookup_tables` configuration in YAML
- Verify table exists in database with correct schema
- Ensure database connection has proper permissions

**Issue**: "Cohort data required for covariate flagging"
- **Solution**: Ensure `cohort` section is configured in YAML
- Call `prepare_cohort_data()` before applying preprocessing
- Verify cohort data has `patient_id` and `baseline_date` columns

**Issue**: "No preprocessing configured for table"
- **Solution**: Check `dataset_preprocessing_map` in YAML
- Ensure table name matches exactly (case-sensitive)
- Verify `enabled: true` is set

**Issue**: "Code column not found in data"
- **Solution**: Verify column names in database match config
- Check for case sensitivity (DB2 uses uppercase by default)
- Ensure column mapping in db2_config is correct

---

## Extending the System

### Adding New Preprocessing Types

To add a new preprocessing type:

1. **Create preprocessing function** in `data_preprocessing.R`:

```r
apply_my_new_preprocessing <- function(data, config) {
  # Your preprocessing logic here
  return(processed_data)
}
```

2. **Add case to switch statement** in `apply_preprocessing()`:

```r
switch(step_type,
       "code_match" = apply_code_matching(...),
       "my_new_type" = apply_my_new_preprocessing(preprocessed_data, step_config),
       ...
)
```

3. **Document in YAML** with example configuration:

```yaml
preprocessing:
  example_config:
    my_preprocessing_step:
      type: "my_new_type"
      parameter1: value1
      parameter2: value2
```

---

## File Locations

| File | Location | Purpose |
|------|----------|---------|
| `data_preprocessing.R` | `r-scripts/pipeline_code/` | Core preprocessing functions |
| `preprocessing_integration.R` | `r-scripts/pipeline_code/` | Pipeline integration functions |
| `preprocessing_config_example.yaml` | `r-scripts/pipeline_code/` | Example configuration |
| `PREPROCESSING_DOCUMENTATION.md` | `r-scripts/pipeline_code/` | This documentation |
| `create_long_format_assets.R` | `r-scripts/pipeline_code/` | Existing pipeline (integration point) |
| `read_db2_config_multi_source.R` | `r-scripts/pipeline_code/` | Existing config reader |
| `db2_connection.R` | `r-scripts/utility_code/` | Database connection |

---

## Support and Contact

For questions or issues with the preprocessing module:

1. Review this documentation
2. Check example configurations in `preprocessing_config_example.yaml`
3. Review function documentation in source code
4. Contact the development team

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024 | Initial release of preprocessing module |

---

## License

This preprocessing module is part of the SAIL asset creation pipeline and follows the same license terms as the main project.
