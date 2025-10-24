# Database Storage for Intermediate Tables

This document explains how to use database tables instead of RDS/CSV files for storing intermediate analysis results.

## Overview

The codebase has been updated to save and read intermediate analysis tables directly to/from the database instead of RDS or CSV files. This eliminates the file system dependency and the error:

```
Warning message:
In gzfile(file, mode) :
  cannot open compressed file '/mnt/user-data/outputs/demographics_combined.rds',
  probable reason 'No such file or directory'
```

## Benefits

- **No file system dependencies**: All data stored in database
- **Better data governance**: Centralized storage with access controls
- **Improved performance**: Direct database access without file I/O
- **Version control**: Database can track changes to tables
- **Concurrent access**: Multiple users can access same data safely

## Storage Location

All intermediate tables are saved to the **DB2INST1** schema (user workspace), separate from the source data in the **SAIL** schema:

- **SAIL schema**: Source data (read-only)
- **DB2INST1 schema**: Analysis workspace (read-write)

## Table Naming Convention

Tables are saved with uppercase names and descriptive suffixes:

| Data Type | Table Name | Original File |
|-----------|------------|---------------|
| Demographics (combined) | `DEMOGRAPHICS_COMBINED` | `demographics_combined.rds` |
| Date of birth (long format) | `DATE_OF_BIRTH_LONG_FORMAT` | `date_of_birth_long_format.rds` |
| Sex (long format) | `SEX_LONG_FORMAT` | `sex_long_format.rds` |
| Ethnicity (long format) | `ETHNICITY_LONG_FORMAT` | `ethnicity_long_format.rds` |
| LSOA (long format) | `LSOA_LONG_FORMAT` | `lsoa_long_format.rds` |
| Hospital admissions | `HOSPITAL_ADMISSIONS_LONG_FORMAT` | `hospital_admissions_long_format.rds` |
| Primary care | `PRIMARY_CARE_LONG_FORMAT` | `primary_care_long_format.rds` |
| Medicines | `PRIMARY_CARE_MEDICINES_LONG_FORMAT` | `primary_care_medicines_long_format.rds` |
| Deaths | `DEATHS_LONG_FORMAT` | `deaths_long_format.rds` |
| Study cohort | `STUDY_COHORT` | `study_cohort.rds` |
| Cohort with covariates | `COHORT_WITH_COVARIATES` | `cohort_with_covariates.rds` |
| Cohort with outcomes | `COHORT_WITH_OUTCOMES` | `cohort_with_outcomes.rds` |
| Final analysis dataset | `FINAL_ANALYSIS_DATASET` | `final_analysis_dataset.rds` |

## Utility Functions

A new utility file provides database table operations:

### `db_table_utils.R`

Located at: `r-scripts/utility_code/db_table_utils.R`

Source it in your scripts:

```r
source("scripts/utility_code/db_table_utils.R")
```

### Available Functions

#### 1. Save Data to Database

```r
save_to_db(conn, data, table_name, schema = "DB2INST1", overwrite = TRUE)
```

**Example:**
```r
conn <- create_db2_connection()
save_to_db(conn, demographics, "DEMOGRAPHICS_COMBINED")
# ✓ Saved to database: DB2INST1.DEMOGRAPHICS_COMBINED (1000 rows, 10 columns)
```

#### 2. Read Data from Database

```r
read_from_db(conn, table_name, schema = "DB2INST1")
```

**Example:**
```r
demographics <- read_from_db(conn, "DEMOGRAPHICS_COMBINED")
# ✓ Read from database: DB2INST1.DEMOGRAPHICS_COMBINED (1000 rows, 10 columns)
```

#### 3. Check if Table Exists

```r
db_table_exists(conn, table_name, schema = "DB2INST1")
```

**Example:**
```r
if (db_table_exists(conn, "DEMOGRAPHICS_COMBINED")) {
  data <- read_from_db(conn, "DEMOGRAPHICS_COMBINED")
}
```

#### 4. List All Tables

```r
list_workspace_tables(conn, schema = "DB2INST1")
```

**Example:**
```r
tables <- list_workspace_tables(conn)
# Found 12 tables in schema DB2INST1
# [1] "DEMOGRAPHICS_COMBINED" "STUDY_COHORT" ...
```

#### 5. Delete Table

```r
delete_db_table(conn, table_name, schema = "DB2INST1")
```

**Example:**
```r
delete_db_table(conn, "OLD_ANALYSIS_TABLE")
# ✓ Deleted table: DB2INST1.OLD_ANALYSIS_TABLE
```

#### 6. Save Multiple Tables

```r
save_multiple_to_db(conn, table_list, schema = "DB2INST1", overwrite = TRUE)
```

**Example:**
```r
tables <- list(
  demographics = demographics_df,
  cohort = cohort_df
)
saved <- save_multiple_to_db(conn, tables)
# ✓ Saved 2 tables to database
```

## Updated Pipeline Functions

All pipeline functions now support database storage by default:

### 1. Create Long Format Assets

```r
# Default: Save to database
asset_tables <- create_asset_pipeline(
  config_path = "db2_config_multi_source.yaml",
  format = "database"  # Default
)

# Legacy: Save to CSV files
asset_tables <- create_asset_pipeline(
  config_path = "db2_config_multi_source.yaml",
  format = "csv"
)
```

### 2. Complete Pipeline Example

```r
source("scripts/examples_code/complete_pipeline_example.R")

# Run the complete pipeline (saves all intermediate tables to database)
results <- run_complete_pipeline(
  config_path = "scripts/pipeline_code/db2_config_multi_source.yaml",
  patient_ids = NULL,
  index_date = as.Date("2024-01-01"),
  min_age = 18,
  max_age = 100,
  follow_up_days = 365
)
```

### 3. Individual Pipeline Steps

```r
# Generate cohort with database storage
conn <- create_db2_connection()
cohort <- example_generate_cohort(conn, use_database = TRUE)

# Generate covariates
covariates <- example_generate_covariates(conn, use_database = TRUE)

# Generate outcomes
outcomes <- example_generate_outcomes(conn, use_database = TRUE)

DBI::dbDisconnect(conn)
```

### 4. Quick Start Example

```r
source("scripts/examples_code/quick_start.R")

# Set USE_DATABASE = TRUE (default) to save to database
# Set USE_DATABASE = FALSE to use legacy CSV files
```

## Migration from Files to Database

If you have existing RDS/CSV files and want to migrate to database storage:

```r
library(DBI)
source("scripts/utility_code/db2_connection.R")
source("scripts/utility_code/db_table_utils.R")

# Connect to database
conn <- create_db2_connection()

# Define file-to-table mapping
files <- list(
  demographics_combined = "/mnt/user-data/outputs/demographics_combined.rds",
  study_cohort = "/mnt/user-data/outputs/study_cohort.rds",
  cohort_with_covariates = "/mnt/user-data/outputs/cohort_with_covariates.rds",
  cohort_with_outcomes = "/mnt/user-data/outputs/cohort_with_outcomes.rds",
  final_analysis_dataset = "/mnt/user-data/outputs/final_analysis_dataset.rds"
)

# Migrate each file to database
for (name in names(files)) {
  if (file.exists(files[[name]])) {
    cat(sprintf("Migrating %s...\n", name))
    data <- readRDS(files[[name]])
    save_to_db(conn, data, toupper(name))
  }
}

# Disconnect
DBI::dbDisconnect(conn)
```

## Backward Compatibility

All functions maintain backward compatibility with file-based storage:

```r
# Database storage (default)
example_generate_cohort(conn, use_database = TRUE)

# File-based storage (legacy)
example_generate_cohort(conn = NULL, use_database = FALSE)
```

## Direct SQL Access

You can also access the tables directly using SQL:

```r
conn <- create_db2_connection()

# Query a table
demographics <- DBI::dbGetQuery(
  conn,
  "SELECT * FROM DB2INST1.DEMOGRAPHICS_COMBINED WHERE age_at_index >= 65"
)

# Join tables
analysis_data <- DBI::dbGetQuery(
  conn,
  "SELECT
     c.*,
     cov.diabetes_covariate_flag,
     o.heart_attack_outcome_flag
   FROM DB2INST1.STUDY_COHORT c
   LEFT JOIN DB2INST1.COHORT_WITH_COVARIATES cov ON c.patient_id = cov.patient_id
   LEFT JOIN DB2INST1.COHORT_WITH_OUTCOMES o ON c.patient_id = o.patient_id"
)
```

## Troubleshooting

### Issue: Table not found

```r
Error: Table DB2INST1.DEMOGRAPHICS_COMBINED does not exist
```

**Solution:** Check if the table exists and create it if needed:

```r
if (!db_table_exists(conn, "DEMOGRAPHICS_COMBINED")) {
  # Run the pipeline to create the table
  demographics <- curate_demographics(config_path, patient_ids)
}
```

### Issue: Permission denied

```r
Error: Failed to save table DB2INST1.MY_TABLE: permission denied
```

**Solution:** Ensure you have write permissions in the DB2INST1 schema. Contact your database administrator if needed.

### Issue: Table already exists

```r
Error: Table DB2INST1.MY_TABLE already exists and overwrite=FALSE
```

**Solution:** Either set `overwrite=TRUE` or delete the existing table:

```r
# Option 1: Overwrite
save_to_db(conn, data, "MY_TABLE", overwrite = TRUE)

# Option 2: Delete and recreate
delete_db_table(conn, "MY_TABLE")
save_to_db(conn, data, "MY_TABLE")
```

## Best Practices

1. **Always close connections**: Use `DBI::dbDisconnect(conn)` when done
2. **Use descriptive table names**: Follow the uppercase naming convention
3. **Check table existence**: Use `db_table_exists()` before reading
4. **Clean up old tables**: Delete intermediate tables you no longer need
5. **Use transactions**: For complex operations, wrap in database transactions
6. **Document your tables**: Keep a record of what each table contains

## Example Workflow

Here's a complete example workflow using database storage:

```r
library(DBI)
library(dplyr)
source("scripts/utility_code/db2_connection.R")
source("scripts/utility_code/db_table_utils.R")
source("scripts/examples_code/complete_pipeline_example.R")

# Connect to database
config <- read_db_config("scripts/pipeline_code/db2_config_multi_source.yaml")
conn <- create_db2_connection(config)

# Run complete pipeline (all intermediate tables saved to database)
results <- run_complete_pipeline(
  config_path = "scripts/pipeline_code/db2_config_multi_source.yaml",
  index_date = as.Date("2024-01-01"),
  min_age = 18,
  max_age = 100,
  follow_up_days = 365
)

# Later: Retrieve the data for analysis
final_data <- read_from_db(conn, "FINAL_ANALYSIS_DATASET")

# Perform analysis
summary_stats <- final_data %>%
  group_by(sex_code) %>%
  summarise(
    n = n(),
    mean_age = mean(age_at_index),
    heart_attack_rate = mean(heart_attack_outcome_flag)
  )

print(summary_stats)

# Clean up
DBI::dbDisconnect(conn)
```

## Summary

The database storage approach provides a more robust and scalable solution for managing intermediate analysis results. All existing scripts have been updated to use database storage by default while maintaining backward compatibility with file-based storage.

For questions or issues, please refer to the function documentation or contact the development team.
