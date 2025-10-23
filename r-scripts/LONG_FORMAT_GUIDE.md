# Curated Long Format Code - User Guide

## Table of Contents
- [Overview](#overview)
- [What is Long Format?](#what-is-long-format)
- [Key Concepts](#key-concepts)
- [Getting Started](#getting-started)
- [Core Functions](#core-functions)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Data Quality & Conflict Detection](#data-quality--conflict-detection)
- [Export Options](#export-options)
- [Troubleshooting](#troubleshooting)

---

## Overview

The Curated Long Format system is a healthcare data curation pipeline that aggregates demographic data from multiple IBM DB2 source tables into standardized, analysis-ready datasets. It implements a priority-based variable resolution system that transparently handles conflicts when the same variable appears in multiple sources.

**Key Features:**
- Multi-source data aggregation with full source attribution
- Priority-based conflict resolution
- Automatic column standardization and mapping
- Comprehensive data quality checks
- Conflict detection and reporting
- Flexible export options (CSV, RDS)

**Location:** `/r-scripts/pipeline_code/create_long_format_assets.R`

---

## What is Long Format?

In the long format structure, each row represents data from **one source** for **one patient**. This contrasts with wide format where each patient has only one row.

### Example Comparison

**Long Format:**
```
patient_id | source_table  | source_priority | date_of_birth
---------------------------------------------------------
P001       | hospital_dob  | 1               | 1985-03-15
P001       | gp_dob        | 2               | 1985-03-15
P001       | registry_dob  | 3               | 1985-03-16
P002       | hospital_dob  | 1               | 1990-07-22
P002       | gp_dob        | 2               | 1990-07-22
```

**Wide Format (after resolution):**
```
patient_id | date_of_birth | source_table
--------------------------------------------
P001       | 1985-03-15    | hospital_dob
P002       | 1990-07-22    | hospital_dob
```

**Benefits:**
- Preserves all source data for transparency
- Enables conflict detection and analysis
- Supports data quality assessment by source
- Allows flexible priority-based resolution

---

## Key Concepts

### 1. Assets
An **asset** is a specific demographic variable you want to curate:
- `date_of_birth` - Patient birth dates
- `sex` - Patient biological sex
- `ethnicity` - Patient ethnicity codes and categories
- `lsoa` - Lower Layer Super Output Area (geographical location)

### 2. Sources
Each asset can come from multiple **sources** (database tables):
- Example: `date_of_birth` might come from hospital records, GP records, and registries

### 3. Priority
Each source has a **priority ranking** (1 = highest, 2 = next, etc.):
- Used to select the preferred value when conflicts exist
- Configured in YAML based on data quality and reliability
- Example: Self-reported ethnicity (priority 1) > Hospital admission ethnicity (priority 2)

### 4. Source Metadata
Each row includes metadata about its source:
- `source_table` - Name of the source table
- `source_priority` - Priority ranking (1, 2, 3...)
- `source_quality` - Quality rating (high, medium, low)
- `source_coverage` - Coverage percentage (0.0 to 1.0)
- `source_last_updated` - Date the source was last updated

### 5. Conflict
A **conflict** occurs when a patient has different values for the same variable across multiple sources.

---

## Getting Started

### Prerequisites
```r
# Required packages
library(dplyr)
library(tidyr)
library(DBI)
library(odbc)
library(yaml)

# Source required scripts
source("utility_code/db2_connection.R")
source("pipeline_code/read_db2_config_multi_source.R")
source("pipeline_code/create_long_format_assets.R")
```

### Quick Start (3 lines)
```r
# 1. Run the complete pipeline
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  patient_ids = NULL,  # All patients
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa"),
  output_dir = "/mnt/user-data/outputs"
)

# 2. Access your curated data
ethnicity_data <- results$asset_tables$ethnicity

# 3. Review conflicts
View(results$conflicts$ethnicity)
```

---

## Core Functions

### 1. `create_asset_pipeline()` - Complete Pipeline
**Purpose:** Run the entire end-to-end pipeline for multiple assets.

**Parameters:**
- `config_path` - Path to YAML configuration file
- `patient_ids` - Vector of patient IDs to include (NULL = all patients)
- `assets` - Character vector of asset names to process
- `output_dir` - Directory for exported files (NULL = no export)
- `export_format` - "csv" or "rds" (default: "csv")

**Returns:** List with:
- `asset_tables` - Named list of long format tables
- `summaries` - Summary statistics for each asset
- `conflicts` - Conflict analysis for each asset
- `exported_files` - Paths to exported files (if output_dir provided)

**Example:**
```r
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  patient_ids = c(1001:5000),
  assets = c("ethnicity", "date_of_birth"),
  output_dir = "/mnt/user-data/curated",
  export_format = "csv"
)
```

---

### 2. `create_long_format_asset()` - Single Asset Table
**Purpose:** Create long format table for a single asset from all sources.

**Parameters:**
- `conn` - Database connection object
- `config` - Parsed configuration list
- `asset_name` - Name of asset to create
- `patient_ids` - Vector of patient IDs (NULL = all)

**Returns:** Data frame with columns:
- `patient_id`
- `source_table`
- `source_priority`
- `source_quality`
- `source_coverage`
- `source_last_updated`
- [Asset-specific columns]

**Example:**
```r
config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
conn <- create_db2_connection(config)

ethnicity_long <- create_long_format_asset(
  conn,
  config,
  asset_name = "ethnicity",
  patient_ids = NULL
)

DBI::dbDisconnect(conn)
```

---

### 3. `create_all_asset_tables()` - Batch Processing
**Purpose:** Create long format tables for multiple assets.

**Parameters:**
- `conn` - Database connection object
- `config` - Parsed configuration list
- `asset_names` - Character vector of asset names
- `patient_ids` - Vector of patient IDs (NULL = all)

**Returns:** Named list of data frames

**Example:**
```r
all_tables <- create_all_asset_tables(
  conn,
  config,
  asset_names = c("date_of_birth", "sex", "ethnicity"),
  patient_ids = c(1:1000)
)

# Access individual tables
dob_table <- all_tables$date_of_birth
sex_table <- all_tables$sex
```

---

### 4. `get_highest_priority_per_patient()` - Resolve Conflicts
**Purpose:** Convert long format to one row per patient using priority.

**Parameters:**
- `long_table` - Long format data frame
- `group_columns` - Columns to keep (default: all non-metadata columns)

**Returns:** Data frame with one row per patient

**Example:**
```r
# Long format: multiple rows per patient
ethnicity_long <- create_long_format_asset(conn, config, "ethnicity")

# Resolved: one row per patient (highest priority source)
ethnicity_resolved <- get_highest_priority_per_patient(ethnicity_long)

# Now each patient appears only once
```

---

### 5. `pivot_to_wide_by_source()` - Side-by-Side Comparison
**Purpose:** Pivot long format to compare sources side-by-side.

**Parameters:**
- `long_table` - Long format data frame
- `value_columns` - Columns to pivot (default: all non-metadata)

**Returns:** Wide format with columns like `source1_value`, `source2_value`

**Example:**
```r
# Compare ethnicity values across sources
ethnicity_comparison <- pivot_to_wide_by_source(
  ethnicity_long,
  value_columns = c("ethnicity_code", "ethnicity_category")
)

# View patients with conflicting values
View(ethnicity_comparison)
```

---

### 6. `check_conflicts()` - Detect Conflicts
**Purpose:** Identify patients with conflicting values across sources.

**Parameters:**
- `long_table` - Long format data frame
- `asset_name` - Name of the asset (for reporting)
- `key_column` - Column to check for conflicts

**Returns:** Data frame with conflict summary

**Example:**
```r
conflicts <- check_conflicts(
  ethnicity_long,
  asset_name = "ethnicity",
  key_column = "ethnicity_code"
)

# Shows patients with different ethnicity codes across sources
print(conflicts)
```

---

### 7. `summarize_long_format_table()` - Summary Statistics
**Purpose:** Generate comprehensive summary statistics.

**Parameters:**
- `long_table` - Long format data frame
- `asset_name` - Name of the asset

**Returns:** List with:
- `overall` - Total rows, unique patients, source count
- `by_source` - Per-source patient counts, quality, coverage
- `patient_coverage` - Patients appearing in each source

**Example:**
```r
summary <- summarize_long_format_table(ethnicity_long, "ethnicity")

print(summary$overall)
print(summary$by_source)
print(summary$patient_coverage)
```

---

### 8. `export_asset_table()` - Export Single Table
**Purpose:** Export a single table to CSV or RDS.

**Parameters:**
- `asset_table` - Data frame to export
- `asset_name` - Name for the output file
- `output_dir` - Directory for output
- `format` - "csv" or "rds"

**Returns:** Path to exported file

**Example:**
```r
export_asset_table(
  ethnicity_long,
  asset_name = "ethnicity",
  output_dir = "/mnt/user-data/outputs",
  format = "csv"
)
# Creates: /mnt/user-data/outputs/ethnicity_long_format.csv
```

---

### 9. `export_all_asset_tables()` - Batch Export
**Purpose:** Export multiple tables at once.

**Parameters:**
- `asset_tables` - Named list of data frames
- `output_dir` - Directory for outputs
- `format` - "csv" or "rds"

**Returns:** Named vector of output file paths

**Example:**
```r
file_paths <- export_all_asset_tables(
  all_tables,
  output_dir = "/mnt/user-data/outputs",
  format = "rds"
)

print(file_paths)
```

---

## Configuration

### YAML Structure
Configuration is stored in `db2_config_multi_source.yaml`:

```yaml
database:
  dsn: "DB2_CONNECTION"
  schema: "SAIL_SCHEMA"
  uid: "${DB2_USER}"
  pwd: "${DB2_PASSWORD}"

assets:
  ethnicity:
    description: "Patient ethnicity codes and categories"
    default_source: "self_reported"
    sources:
      self_reported:
        table_name: "ETHNICITY_SELF_REPORTED"
        priority: 1
        quality: "high"
        coverage: 0.65
        last_updated: "2024-01-15"
        primary_key: ["PATIENT_ID"]
        columns:
          ethnicity_code:
            db_column: "ETHNIC_CODE"
            data_type: "character"
          ethnicity_category:
            db_column: "ETHNIC_CATEGORY"
            data_type: "character"
```

### Key Configuration Elements

**Database Section:**
- Connection details for IBM DB2
- Environment variables (${VAR}) are automatically expanded

**Asset Section:**
- `description` - Human-readable description
- `default_source` - Recommended primary source
- `sources` - Dictionary of source configurations

**Source Configuration:**
- `table_name` - DB2 table name (case-sensitive)
- `priority` - Ranking (1 = highest)
- `quality` - Rating: "high", "medium", "low"
- `coverage` - Proportion of patients (0.0 to 1.0)
- `last_updated` - Date string
- `primary_key` - Column(s) identifying patients
- `columns` - Column mappings (R name → DB column)

### Modifying Configuration

To add a new source:
```yaml
assets:
  ethnicity:
    sources:
      new_source_name:
        table_name: "NEW_TABLE"
        priority: 5  # Lower priority
        quality: "medium"
        coverage: 0.40
        last_updated: "2024-02-01"
        primary_key: ["PATIENT_ID"]
        columns:
          ethnicity_code:
            db_column: "ETHNICITY"
            data_type: "character"
```

---

## Usage Examples

### Example 1: Basic Asset Creation
```r
# Load configuration
config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")

# Connect to database
conn <- create_db2_connection(config)

# Create long format table for date of birth
dob_long <- create_long_format_asset(
  conn,
  config,
  asset_name = "date_of_birth",
  patient_ids = NULL  # All patients
)

# View the data
head(dob_long)

# Disconnect
DBI::dbDisconnect(conn)
```

---

### Example 2: Working with Specific Patients
```r
# Define patient cohort
my_patients <- c(100001:105000)

# Create ethnicity data for cohort
config <- read_db_config("pipeline_code/db2_config_multi_source.yaml")
conn <- create_db2_connection(config)

ethnicity_long <- create_long_format_asset(
  conn,
  config,
  asset_name = "ethnicity",
  patient_ids = my_patients
)

DBI::dbDisconnect(conn)

# Resolve to one row per patient
ethnicity_final <- get_highest_priority_per_patient(ethnicity_long)

# Export
write.csv(ethnicity_final, "cohort_ethnicity.csv", row.names = FALSE)
```

---

### Example 3: Conflict Analysis
```r
# Create long format table
ethnicity_long <- create_long_format_asset(conn, config, "ethnicity")

# Check for conflicts
conflicts <- check_conflicts(
  ethnicity_long,
  asset_name = "ethnicity",
  key_column = "ethnicity_code"
)

# View patients with conflicting ethnicity codes
print(conflicts)

# Get wide format for manual review
wide_comparison <- pivot_to_wide_by_source(
  ethnicity_long,
  value_columns = c("ethnicity_code", "ethnicity_category")
)

# Export conflicts for review
write.csv(wide_comparison, "ethnicity_conflicts.csv", row.names = FALSE)
```

---

### Example 4: Quality Assessment
```r
# Create long format table
lsoa_long <- create_long_format_asset(conn, config, "lsoa")

# Get comprehensive summary
summary <- summarize_long_format_table(lsoa_long, "lsoa")

# Review overall statistics
print(summary$overall)
# Shows: total_rows, unique_patients, num_sources

# Review by source
print(summary$by_source)
# Shows: patient_count, row_count, quality, coverage for each source

# Check patient coverage
print(summary$patient_coverage)
# Shows: which patients appear in which sources
```

---

### Example 5: Complete Pipeline with Export
```r
# Define parameters
my_patients <- readRDS("my_cohort_ids.rds")
output_directory <- "/mnt/user-data/curated_demographics"

# Run complete pipeline
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  patient_ids = my_patients,
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa"),
  output_dir = output_directory,
  export_format = "csv"
)

# Review results
lapply(results$summaries, print)  # Print all summaries
lapply(results$conflicts, print)  # Print all conflicts

# Access curated data
dob_data <- results$asset_tables$date_of_birth
sex_data <- results$asset_tables$sex
ethnicity_data <- results$asset_tables$ethnicity
lsoa_data <- results$asset_tables$lsoa

# Files are automatically exported to output_directory
print(results$exported_files)
```

---

### Example 6: Creating Resolved Dataset
```r
# Run pipeline
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  patient_ids = NULL,
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa")
)

# Resolve each asset to one row per patient
dob_resolved <- get_highest_priority_per_patient(results$asset_tables$date_of_birth)
sex_resolved <- get_highest_priority_per_patient(results$asset_tables$sex)
ethnicity_resolved <- get_highest_priority_per_patient(results$asset_tables$ethnicity)
lsoa_resolved <- get_highest_priority_per_patient(results$asset_tables$lsoa)

# Combine into single demographic dataset
demographics <- dob_resolved %>%
  select(patient_id, date_of_birth, source_table_dob = source_table) %>%
  left_join(
    sex_resolved %>% select(patient_id, sex, source_table_sex = source_table),
    by = "patient_id"
  ) %>%
  left_join(
    ethnicity_resolved %>% select(patient_id, ethnicity_code, ethnicity_category, source_table_ethnicity = source_table),
    by = "patient_id"
  ) %>%
  left_join(
    lsoa_resolved %>% select(patient_id, lsoa_code, lsoa_name, source_table_lsoa = source_table),
    by = "patient_id"
  )

# Export final dataset
write.csv(demographics, "demographics_master.csv", row.names = FALSE)
```

---

## Data Quality & Conflict Detection

### Understanding Conflicts

Conflicts occur when a patient has different values across sources. Example:

```r
# Patient 12345 in ethnicity data
patient_id | source_table    | source_priority | ethnicity_code
----------------------------------------------------------------
12345      | self_reported   | 1               | "A"
12345      | admission       | 2               | "B"
12345      | gp              | 4               | "A"
```

This patient has conflicting ethnicity codes: "A" (self-reported, GP) vs "B" (admission).

### Detecting Conflicts

```r
# Automatic conflict detection
conflicts <- check_conflicts(
  ethnicity_long,
  asset_name = "ethnicity",
  key_column = "ethnicity_code"
)

# Output shows:
# - patient_id
# - conflicting values
# - sources for each value
# - priority resolution (which value "wins")
```

### Investigating Conflicts

```r
# Get patients with conflicts
conflict_patients <- conflicts$patient_id

# Create wide format for manual review
conflict_detail <- ethnicity_long %>%
  filter(patient_id %in% conflict_patients) %>%
  pivot_to_wide_by_source(value_columns = c("ethnicity_code", "ethnicity_category"))

# View side-by-side comparison
View(conflict_detail)

# Export for clinical review
write.csv(conflict_detail, "ethnicity_conflicts_for_review.csv", row.names = FALSE)
```

### Quality Metrics

Each source has quality metadata:

```r
# View quality by source
summary <- summarize_long_format_table(ethnicity_long, "ethnicity")
print(summary$by_source)

# Output includes:
# - source_quality: high, medium, low
# - source_coverage: proportion of patients
# - patient_count: number of patients in source
```

### Filtering by Quality

```r
# Keep only high-quality sources
high_quality_only <- ethnicity_long %>%
  filter(source_quality == "high")

# Then resolve conflicts
ethnicity_high_qual <- get_highest_priority_per_patient(high_quality_only)
```

---

## Export Options

### CSV Export
**Best for:** Human review, Excel, external tools

```r
export_asset_table(
  ethnicity_long,
  asset_name = "ethnicity",
  output_dir = "/outputs",
  format = "csv"
)
# Creates: /outputs/ethnicity_long_format.csv
```

### RDS Export
**Best for:** R workflows, preserves data types, faster loading

```r
export_asset_table(
  ethnicity_long,
  asset_name = "ethnicity",
  output_dir = "/outputs",
  format = "rds"
)
# Creates: /outputs/ethnicity_long_format.rds

# Later, load with:
ethnicity_long <- readRDS("/outputs/ethnicity_long_format.rds")
```

### Batch Export

```r
# Export all assets at once
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa"),
  output_dir = "/outputs",
  export_format = "csv"
)

# Files created:
# - date_of_birth_long_format.csv
# - sex_long_format.csv
# - ethnicity_long_format.csv
# - lsoa_long_format.csv
```

---

## Troubleshooting

### Database Connection Issues

**Problem:** `Error: nanodbc/nanodbc.cpp:1021: 00000`

**Solution:**
```r
# Check environment variables
Sys.getenv("DB2_USER")
Sys.getenv("DB2_PASSWORD")

# Verify DSN configuration
odbcListDrivers()

# Test connection manually
conn <- DBI::dbConnect(
  odbc::odbc(),
  dsn = "DB2_CONNECTION",
  uid = Sys.getenv("DB2_USER"),
  pwd = Sys.getenv("DB2_PASSWORD")
)
```

---

### Column Name Mismatches

**Problem:** `Error: Column 'ETHNIC_CODE' not found`

**Solution:**
- DB2 returns uppercase column names
- Check exact spelling in database
- Update YAML configuration:

```yaml
columns:
  ethnicity_code:
    db_column: "ETHNIC_CODE"  # Must match DB2 exactly (case-sensitive)
```

---

### Missing Patients

**Problem:** Some patients missing from output

**Possible Causes:**
1. Patient IDs not in source tables
2. NULL values in primary key
3. WHERE clause filtering

**Investigation:**
```r
# Check how many patients in each source
summary <- summarize_long_format_table(ethnicity_long, "ethnicity")
print(summary$by_source)

# Check patient coverage
print(summary$patient_coverage)

# Query source directly
patient_count <- DBI::dbGetQuery(
  conn,
  "SELECT COUNT(DISTINCT PATIENT_ID) FROM SAIL_SCHEMA.ETHNICITY_SELF_REPORTED"
)
```

---

### High Memory Usage

**Problem:** Large datasets consume too much memory

**Solution 1:** Process in batches
```r
# Process 10,000 patients at a time
all_patients <- 1:100000
batch_size <- 10000

results_list <- list()
for(i in seq(1, length(all_patients), batch_size)) {
  batch <- all_patients[i:min(i+batch_size-1, length(all_patients))]

  results_list[[length(results_list) + 1]] <- create_asset_pipeline(
    config_path = "pipeline_code/db2_config_multi_source.yaml",
    patient_ids = batch,
    assets = "ethnicity"
  )
}

# Combine results
ethnicity_all <- bind_rows(lapply(results_list, function(x) x$asset_tables$ethnicity))
```

**Solution 2:** Select specific sources
```r
# Modify config to use only high-priority sources
# Comment out low-priority sources in YAML
```

---

### Unexpected Conflicts

**Problem:** More conflicts than expected

**Investigation:**
```r
# Check conflict details
conflicts <- check_conflicts(ethnicity_long, "ethnicity", "ethnicity_code")

# View actual values
wide_view <- pivot_to_wide_by_source(
  ethnicity_long,
  value_columns = "ethnicity_code"
)

# Count conflicts by source pair
conflict_matrix <- ethnicity_long %>%
  filter(patient_id %in% conflicts$patient_id) %>%
  group_by(patient_id, source_table, ethnicity_code) %>%
  summarise(n = n()) %>%
  pivot_wider(names_from = source_table, values_from = ethnicity_code)

View(conflict_matrix)
```

---

## Advanced Topics

### Custom Priority Resolution

Instead of using priority, implement custom logic:

```r
# Get long format
ethnicity_long <- create_long_format_asset(conn, config, "ethnicity")

# Custom resolution: most common value
ethnicity_custom <- ethnicity_long %>%
  group_by(patient_id, ethnicity_code) %>%
  summarise(n_sources = n()) %>%
  slice_max(n_sources, n = 1, with_ties = FALSE) %>%
  ungroup()
```

### Combining Assets

```r
# Create master demographic table
results <- create_asset_pipeline(
  config_path = "pipeline_code/db2_config_multi_source.yaml",
  assets = c("date_of_birth", "sex", "ethnicity", "lsoa")
)

# Resolve each
dob <- get_highest_priority_per_patient(results$asset_tables$date_of_birth)
sex <- get_highest_priority_per_patient(results$asset_tables$sex)
ethnicity <- get_highest_priority_per_patient(results$asset_tables$ethnicity)
lsoa <- get_highest_priority_per_patient(results$asset_tables$lsoa)

# Join into single table
demographics <- dob %>%
  select(patient_id, date_of_birth) %>%
  left_join(sex %>% select(patient_id, sex), by = "patient_id") %>%
  left_join(ethnicity %>% select(patient_id, ethnicity_code, ethnicity_category), by = "patient_id") %>%
  left_join(lsoa %>% select(patient_id, lsoa_code), by = "patient_id")
```

### Temporal Analysis

```r
# Analyze when sources were last updated
temporal_summary <- ethnicity_long %>%
  group_by(source_table, source_last_updated) %>%
  summarise(
    n_patients = n_distinct(patient_id),
    n_rows = n()
  ) %>%
  arrange(source_last_updated)

# Find patients with only old data
old_data_patients <- ethnicity_long %>%
  group_by(patient_id) %>%
  filter(all(as.Date(source_last_updated) < "2020-01-01")) %>%
  pull(patient_id) %>%
  unique()
```

---

## Additional Resources

**Files:**
- Main code: `r-scripts/pipeline_code/create_long_format_assets.R`
- Configuration: `r-scripts/pipeline_code/db2_config_multi_source.yaml`
- Tests: `r-scripts/tests/testthat/test-create-long-format-assets.R`
- Examples: `r-scripts/examples/` directory

**Related Functions:**
- `read_db_config()` - Load YAML configuration
- `create_db2_connection()` - Create database connection

**Testing:**
```r
# Run tests
testthat::test_file("tests/testthat/test-create-long-format-assets.R")
```

---

## Summary

The Curated Long Format system provides:

1. **Transparent multi-source aggregation** - All source data preserved
2. **Priority-based resolution** - Configurable conflict handling
3. **Quality assessment** - Built-in metrics and reporting
4. **Flexible workflows** - From quick queries to production pipelines
5. **Reproducible curation** - YAML-based configuration

For examples and sample code, see the `r-scripts/examples/` directory.

For questions or issues, consult the test files or configuration YAML for reference implementations.
