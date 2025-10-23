# Data Preprocessing Module - Quick Start

## Overview

This module provides a comprehensive data preprocessing framework for the SAIL asset creation pipeline, enabling:

- **Code matching** from lookup tables (e.g., cholesterol codes, disease codes)
- **Value validation** with range checking
- **Covariate flagging** (events before baseline date)
- **Outcome flagging** (events after baseline date)
- **Data transformation** (type conversion, cleaning)

All preprocessing is configured via YAML files and seamlessly integrates with the existing pipeline.

## Quick Start

### 1. Basic Usage

```r
# Load libraries
source("pipeline_code/data_preprocessing.R")
source("pipeline_code/preprocessing_integration.R")

# Load configurations
db_config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
preprocessing_config <- read_preprocessing_config("pipeline_code/preprocessing_config.yaml")

# Create connection
conn <- create_db2_connection(db_config)

# Get preprocessed data
data <- get_asset_data_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "cholesterol",
  source_name = "gp_cholesterol",
  conn = conn,
  apply_preprocessing = TRUE
)
```

### 2. With Covariate/Outcome Flagging

```r
# Prepare cohort with baseline dates
cohort_data <- prepare_cohort_data(preprocessing_config, conn)

# Get data with covariate/outcome flags
diagnosis_data <- get_asset_data_with_preprocessing(
  config = db_config,
  preprocessing_config = preprocessing_config,
  asset_name = "diagnoses",
  conn = conn,
  cohort_data = cohort_data  # Required for flagging
)

# Check results
table(diagnosis_data$has_heart_disease)
table(diagnosis_data$has_stroke_outcome)
```

### 3. Complete Pipeline

```r
# Run end-to-end preprocessing for multiple assets
results <- run_preprocessing_pipeline(
  db_config_file = "pipeline_code/db2_config_multi_source.yaml",
  preprocessing_config_file = "pipeline_code/preprocessing_config.yaml",
  asset_names = c("cholesterol", "diagnoses", "prescriptions"),
  output_dir = "/mnt/user-data/outputs/"
)
```

## Files Created

| File | Purpose |
|------|---------|
| `data_preprocessing.R` | Core preprocessing functions |
| `preprocessing_integration.R` | Pipeline integration functions |
| `preprocessing_config_example.yaml` | Example YAML configuration |
| `PREPROCESSING_DOCUMENTATION.md` | Complete documentation |
| `example_preprocessing_usage.R` | Usage examples |
| `PREPROCESSING_README.md` | This quick start guide |

## Configuration Example

```yaml
preprocessing:
  cholesterol_data:
    match_codes:
      type: "code_match"
      code_column: "test_code"
      lookup_source: "CHOLESTEROL_CODES"
      output_columns: ["test_name", "units"]

    validate_values:
      type: "value_validation"
      column: "result_value"
      min_value: 1.0
      max_value: 15.0
      action: "flag"

dataset_preprocessing_map:
  GP_CHOLESTEROL_TESTS:
    preprocessing: "cholesterol_data"
    enabled: true
```

## Key Features

### 1. Code Matching
Match codes against lookup tables to enrich or filter data.

```yaml
type: "code_match"
code_column: "diagnosis_code"
lookup_source: "ICD10_CODES"
```

### 2. Value Validation
Validate numeric ranges and allowed values.

```yaml
type: "value_validation"
column: "cholesterol"
min_value: 1.0
max_value: 15.0
action: "flag"  # or "filter" or "transform"
```

### 3. Covariate Flagging
Flag events BEFORE baseline (pre-existing conditions).

```yaml
type: "covariate_flag"
covariate_name: "heart_disease"
event_codes: ["I21", "I22", "I25"]
aggregate_to_patient: true
```

### 4. Outcome Flagging
Flag events AFTER baseline (follow-up outcomes).

```yaml
type: "outcome_flag"
outcome_name: "stroke"
event_codes: ["I60", "I61", "I63"]
include_date: true
```

## Integration with Existing Pipeline

The preprocessing module integrates seamlessly:

```
Database Query → Preprocessing → Long Format Combination → Analysis
                       ↑
                 (NEW MODULE)
```

### Backward Compatibility

- If no preprocessing config provided, pipeline works as before
- Existing scripts require no modification
- New scripts opt-in to preprocessing features

## Examples

See `example_preprocessing_usage.R` for 7 detailed examples:

1. Simple code matching and validation
2. Covariate flagging (pre-existing conditions)
3. Outcome flagging (follow-up events)
4. Complete pipeline with multiple assets
5. Long format integration
6. Manual preprocessing
7. Custom configuration

Run examples:

```r
source("pipeline_code/example_preprocessing_usage.R")
example_1_simple_preprocessing()
```

## Documentation

For complete documentation, see:
- `PREPROCESSING_DOCUMENTATION.md` - Comprehensive guide
- `preprocessing_config_example.yaml` - Configuration examples
- `example_preprocessing_usage.R` - Code examples

## Architecture

```
┌──────────────────────────────────────────────────┐
│          User-Facing Functions                   │
├──────────────────────────────────────────────────┤
│ • run_preprocessing_pipeline()                   │
│ • get_asset_data_with_preprocessing()            │
│ • create_long_format_asset_with_preprocessing()  │
└────────────────┬─────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────┐
│          Integration Layer                       │
├──────────────────────────────────────────────────┤
│ • apply_preprocessing_to_asset_data()            │
│ • prepare_cohort_data()                          │
│ • read_preprocessing_config()                    │
└────────────────┬─────────────────────────────────┘
                 ↓
┌──────────────────────────────────────────────────┐
│          Core Preprocessing                      │
├──────────────────────────────────────────────────┤
│ • apply_preprocessing()                          │
│ • apply_code_matching()                          │
│ • apply_value_validation()                       │
│ • apply_covariate_flagging()                     │
│ • apply_outcome_flagging()                       │
│ • apply_data_transformation()                    │
└──────────────────────────────────────────────────┘
```

## Common Use Cases

### Use Case 1: Cholesterol Test Processing
1. Match test codes to get test names and units
2. Validate values are within physiological range (1-15 mmol/L)
3. Flag abnormal results

### Use Case 2: Disease Cohort Creation
1. Match diagnosis codes against disease lookup table
2. Flag patients with disease before baseline (covariate)
3. Flag patients with disease after baseline (outcome)
4. Record date of first occurrence

### Use Case 3: Medication Analysis
1. Match medication codes to get drug names
2. Flag patients on specific medications before baseline
3. Calculate medication exposure duration

### Use Case 4: Multi-Source Data Quality
1. Apply consistent validation rules across all sources
2. Code matching for standardization
3. Flag data quality issues for investigation

## Support

For questions or issues:
1. Review `PREPROCESSING_DOCUMENTATION.md`
2. Check `preprocessing_config_example.yaml`
3. Run examples in `example_preprocessing_usage.R`
4. Contact development team

## Version

Version 1.0 - Initial release

## License

Part of the SAIL asset creation pipeline project.
