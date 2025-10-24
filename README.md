# SAIL BDCD: Data Curation Pipeline

A comprehensive R-based data pipeline for curating and analyzing health data from SAIL Databank.

## Overview

This pipeline provides standardized functions for creating analysis-ready datasets from health data sources, with support for:
- Multi-source data integration with priority-based resolution
- Database storage for intermediate results (replaces file-based storage)
- Quality assessment and validation
- Flexible cohort generation with multiple restriction criteria
- Covariate and outcome generation with temporal filtering

## Key Features

### Database Storage (New!)
All intermediate tables are now saved to the database instead of RDS/CSV files:
- **Storage location**: DB2INST1 schema (user workspace)
- **Benefits**: No file system dependencies, better data governance, improved performance
- **Backward compatible**: Legacy file-based storage still supported
- **Documentation**: See [r-scripts/docs/DATABASE_STORAGE.md](r-scripts/docs/DATABASE_STORAGE.md)

### Multi-Source Integration
- Handles conflicting values across multiple data sources
- Priority-based resolution with configurable source rankings
- Conflict detection and reporting

## Pipeline Components

### 1. Curate Long Format Assets

Creates standardized long-format tables with source tracking and priority resolution.

#### Demographics Assets
- **Date of Birth** → `DATE_OF_BIRTH_LONG_FORMAT` (database table)
- **Sex** → `SEX_LONG_FORMAT`
- **Ethnicity** → `ETHNICITY_LONG_FORMAT`
- **LSOA** → `LSOA_LONG_FORMAT`

#### Disease and Treatment Assets
- **Hospital admissions** → `HOSPITAL_ADMISSIONS_LONG_FORMAT`
- **Primary care events** → `PRIMARY_CARE_LONG_FORMAT`
- **Primary care medicines** → `PRIMARY_CARE_MEDICINES_LONG_FORMAT`
- **Deaths** → `DEATHS_LONG_FORMAT`

**Quality assessments**:
- Time coverage analysis
- Patient counts (total and distinct)
- Variable completeness percentages
- Source distribution statistics

### 2. Generate Cohort

Creates study cohorts with configurable restrictions from demographics data.

**Input**: Combined demographics asset (from database: `DEMOGRAPHICS_COMBINED`)

**Restrictions**:
- Age at index date (min/max thresholds)
- Known sex requirement
- Known ethnicity requirement
- LSOA at index date requirement

**Output**: Study cohort → `STUDY_COHORT` (database table)

### 3. Generate Covariates

Identifies patient covariates (baseline characteristics) from disease/treatment assets.

**Inputs**:
- Study cohort (from database: `STUDY_COHORT`)
- Disease/treatment assets (from database)
- Lookup table mapping codes to covariate names

**Lookup table structure**:
```r
data.frame(
  code = c("E10", "E11", "I10"),
  name = c("diabetes", "diabetes", "hypertension"),
  description = c("Type 1 diabetes", "Type 2 diabetes", "Essential hypertension"),
  terminology = c("ICD10", "ICD10", "ICD10")
)
```

**Quality assessments**:
- Cohort counts and percentages by covariate name
- Timeframe coverage by covariate name
- Code distribution with counts and percentages

**Temporal filtering**:
- Filter events before index date
- Optional date range (e.g., last 365 days before index)
- Selection method: min (first occurrence) or max (most recent)

**Outputs**:
- Covariate flags (0/1)
- Covariate dates
- Optional: days between covariate date and index date

**Result**: Cohort with covariates → `COHORT_WITH_COVARIATES` (database table)

### 4. Generate Outcomes

Identifies patient outcomes from disease/treatment assets.

**Inputs**:
- Study cohort (from database: `STUDY_COHORT`)
- Disease/treatment assets (from database)
- Lookup table mapping codes to outcome names

**Lookup table structure**:
```r
data.frame(
  code = c("I21", "I22", "I63"),
  name = c("heart_attack", "heart_attack", "stroke"),
  description = c("Acute MI", "Subsequent MI", "Cerebral infarction"),
  terminology = c("ICD10", "ICD10", "ICD10")
)
```

**Quality assessments**:
- Cohort counts and percentages by outcome name
- Timeframe coverage by outcome name
- Code distribution with counts and percentages

**Temporal filtering**:
- Filter events after index date
- Optional follow-up window (e.g., 365 days after index)
- Selection method: min (first occurrence) or max (most recent)

**Outputs**:
- Outcome flags (0/1)
- Outcome dates
- Optional: days between index date and outcome date

**Result**: Cohort with outcomes → `COHORT_WITH_OUTCOMES` (database table)

### 5. Final Dataset

Combines cohort, covariates, and outcomes into analysis-ready dataset.

**Output**: Final analysis dataset → `FINAL_ANALYSIS_DATASET` (database table)

## Quick Start

```r
# Source required functions
source("scripts/utility_code/db2_connection.R")
source("scripts/utility_code/db_table_utils.R")
source("scripts/examples_code/complete_pipeline_example.R")

# Connect to database
conn <- create_db2_connection()

# Run complete pipeline (saves all intermediate tables to database)
results <- run_complete_pipeline(
  config_path = "scripts/pipeline_code/db2_config_multi_source.yaml",
  patient_ids = NULL,
  index_date = as.Date("2024-01-01"),
  min_age = 18,
  max_age = 100,
  follow_up_days = 365
)

# Retrieve final dataset from database
final_data <- read_from_db(conn, "FINAL_ANALYSIS_DATASET")

# Close connection
DBI::dbDisconnect(conn)
```

## Documentation

- **[Database Storage Guide](r-scripts/docs/DATABASE_STORAGE.md)**: Comprehensive guide to using database storage
- **[Function Quick Reference](documentation/FUNCTION_QUICK_REFERENCE.md)**: Quick reference for all functions
- **[Pipeline Flow Documentation](documentation/PIPELINE_FLOW_DOCUMENTATION.md)**: Detailed pipeline workflow
- **[Examples Index](r-scripts/examples_code/INDEX.md)**: Example scripts and use cases

## Requirements

- R >= 4.0
- DBI
- odbc
- dplyr
- yaml
- glue

## Directory Structure

```
sail_bdcd/
├── r-scripts/
│   ├── utility_code/          # Database connection and table utilities
│   ├── pipeline_code/         # Core pipeline functions
│   ├── examples_code/         # Example scripts and workflows
│   ├── tests/                 # Unit tests
│   └── docs/                  # Documentation
└── documentation/             # Additional documentation
```

## Database Schema Organization

- **SAIL schema**: Source data (read-only)
- **DB2INST1 schema**: Analysis workspace (read-write)
  - All intermediate and final tables stored here
  - User has full control over workspace tables

## Support

For issues, questions, or contributions, please refer to the documentation or contact the development team.  
