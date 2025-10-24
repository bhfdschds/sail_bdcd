# SAIL BDCD Pipeline Architecture

This document describes the end-to-end data flow, architecture, and design decisions of the SAIL BDCD healthcare data curation pipeline.

## Table of Contents

- [Overview](#overview)
- [Pipeline Stages](#pipeline-stages)
- [Data Flow](#data-flow)
- [Architecture Components](#architecture-components)
- [Design Decisions](#design-decisions)
- [Configuration System](#configuration-system)
- [Output Formats](#output-formats)
- [Performance Considerations](#performance-considerations)

## Overview

The SAIL BDCD pipeline transforms raw, multi-source healthcare data into analysis-ready datasets through a series of well-defined stages. The pipeline is designed to handle:

- **Multiple data sources** with potentially conflicting information
- **Large-scale population data** from IBM DB2 databases
- **Complex temporal relationships** for cohort studies
- **Strict data quality requirements** for healthcare research

### Pipeline Objectives

1. **Integrate** data from multiple sources into a unified format
2. **Resolve** conflicts using transparent, priority-based rules
3. **Transform** data into researcher-friendly formats (long and wide)
4. **Generate** temporal covariates and outcomes for analysis
5. **Validate** data quality and provide comprehensive audit trails

## Pipeline Stages

The pipeline consists of five main stages:

```
┌─────────────────────────────────────────────────────────────────┐
│                    Stage 1: Configuration                       │
│              Load YAML config and define assets                 │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                  Stage 2: Data Ingestion                        │
│        Query multiple DB2 sources and combine results           │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Stage 3: Long Format Transformation                │
│    Standardize to long format with source tracking              │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│             Stage 4: Conflict Resolution                        │
│   Detect conflicts & select highest priority values             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│           Stage 5: Covariate/Outcome Generation                 │
│      Create temporal features for cohort analysis               │
└─────────────────────────────────────────────────────────────────┘
```

### Stage 1: Configuration

**File**: `r-scripts/pipeline_code/db2_config_multi_source.yaml`

**Purpose**: Define database connections, assets, and source priorities

**Key Components**:
- Database connection parameters (host, port, schema)
- Asset definitions (e.g., date_of_birth, sex, ethnicity)
- Source table specifications with priorities
- Column mappings (database → R variable names)
- Project-specific source preferences

**Example Configuration**:
```yaml
database:
  host: "db"
  port: 50000
  database_name: "DEVDB"
  schema: "SAIL"

assets:
  sex:
    description: "Patient sex/gender"
    sources:
      - name: "PATIENT_ALF_CLEANSED"
        table_name: "SAIL.PATIENT_ALF_CLEANSED"
        priority: 1
        quality_rating: "high"
        columns:
          patient_id: "ALF_PE"
          sex_code: "GNDR_CD"
```

**Key Functions**:
- `read_db2_config()` - Parses YAML and validates structure

### Stage 2: Data Ingestion

**File**: `r-scripts/pipeline_code/read_db2_config_multi_source.R`

**Purpose**: Execute queries against multiple DB2 sources and retrieve raw data

**Process**:
1. Establish DB2 connection using ODBC
2. For each source table in the asset:
   - Build SQL query with appropriate column selections
   - Filter by patient IDs if specified
   - Execute query and retrieve results
3. Combine results from all sources
4. Add metadata (source name, priority, retrieval timestamp)

**Key Functions**:
- `create_db2_connection()` - Establish database connection
- `get_asset_from_source()` - Query a single source table
- `get_multi_source_asset()` - Query all sources for an asset

**Output**: Multiple data frames (one per source) containing raw data

### Stage 3: Long Format Transformation

**File**: `r-scripts/pipeline_code/create_long_format_assets.R`

**Purpose**: Standardize data into long format with full source tracking

**Process**:
1. Combine data from all sources into single data frame
2. Add source tracking columns:
   - `source_table` - Name of originating table
   - `source_priority` - Priority ranking of source
   - `source_quality` - Quality rating
3. Standardize column names across sources
4. Preserve all rows (even duplicates) for audit trail
5. Add data retrieval metadata

**Key Functions**:
- `create_long_format_asset()` - Main orchestration function
- `summarize_long_format_table()` - Generate summary statistics

**Long Format Structure**:
```
patient_id | source_table | source_priority | sex_code | sex_description
-----------+--------------+-----------------+----------+-----------------
1001       | gp_registry  | 1               | M        | Male
1001       | hospital     | 2               | 1        | Male
1002       | gp_registry  | 1               | F        | Female
1002       | hospital     | 2               | 2        | Female
```

**Advantages**:
- Full audit trail of all sources
- Easy conflict detection
- Source comparison capabilities
- Complete data lineage

### Stage 4: Conflict Resolution

**Files**:
- `r-scripts/pipeline_code/create_long_format_assets.R`
- Functions: `check_conflicts()`, `get_highest_priority_per_patient()`

**Purpose**: Detect conflicts and resolve to single value per patient

**Process**:

#### 4a. Conflict Detection
```r
conflicts <- check_conflicts(
  long_data = sex_long,
  columns_to_check = c("sex_code", "sex_description")
)
```

**Conflict Definition**: Multiple sources provide different non-missing values for the same patient

**Output**:
- Count of conflicts per column
- List of patient IDs with conflicts
- Details of conflicting values and sources

#### 4b. Priority-Based Resolution
```r
sex_master <- get_highest_priority_per_patient(sex_long)
```

**Resolution Logic**:
1. Group data by patient_id
2. Sort by source_priority (1 = highest)
3. Select row with lowest priority number (highest priority)
4. If priority tie, select first source alphabetically
5. If high-priority source has missing value, fall back to next priority

**Output**: One row per patient with selected value and source provenance

**Resolution Example**:
```
Input (Long Format):
patient_id | source_table | priority | sex_code
-----------+--------------+----------+---------
1001       | gp_registry  | 1        | M
1001       | hospital     | 2        | 1

Output (Priority Selected):
patient_id | sex_code | source_table | source_priority
-----------+----------+--------------+----------------
1001       | M        | gp_registry  | 1
```

### Stage 5: Covariate/Outcome Generation

**Files**:
- `r-scripts/pipeline_code/generate_covariates.R`
- `r-scripts/pipeline_code/generate_outcomes.R`

**Purpose**: Create temporal features for cohort studies

**Process**:

#### 5a. Filter by Lookup Table
```r
diabetes_events <- filter_long_by_lookup(
  long_data = hospital_admissions,
  lookup_table = diabetes_codes,
  code_col_in_data = "icd10_code"
)
```

**Lookup Table Structure**:
```
code | name        | description                   | terminology
-----+-------------+-------------------------------+------------
E10  | Type 1 DM   | Type 1 diabetes mellitus      | ICD10
E11  | Type 2 DM   | Type 2 diabetes mellitus      | ICD10
```

#### 5b. Generate Temporal Covariates
```r
covariates <- generate_covariates_before_index(
  long_data = diabetes_events,
  index_dates = cohort_index_dates,
  event_date_col = "admission_date",
  min_days_before = 30  # Avoid immortal time bias
)
```

**Temporal Logic**:
- Define index date (e.g., surgery date, diagnosis date)
- Count events occurring before index date
- Optionally define time windows (e.g., "30-90 days before")
- Calculate aggregations (count, earliest, latest, etc.)

**Time Window Example**:
```r
# Events in last 30 days before index
generate_covariates_time_window(
  long_data = events,
  index_dates = cohort,
  days_before_start = 30,  # 30 days before
  days_before_end = 1,     # Exclude index date
  window_label = "last_30d"
)
```

**Timeline Visualization**:
```
<-------- 90 days -------><-- 30 days --><-Index->
         [earlier]          [recent]      Date
                                           |
Event1   Event2             Event3        Event4
 ↓        ↓                  ↓             ↓
90d      60d                20d           0d (index)
```

**Output Variables**:
- `{window}_n_events` - Count of events in window
- `{window}_has_event` - Binary indicator
- `{window}_earliest_date` - First event date
- `{window}_latest_date` - Most recent event date
- `{window}_days_between` - Days from latest event to index

#### 5c. Generate Outcomes
```r
outcomes <- generate_outcomes_after_index(
  long_data = complication_events,
  index_dates = surgery_dates,
  event_date_col = "complication_date",
  max_days_after = 30
)
```

**Outcome Logic**: Mirror of covariate generation, but for events AFTER index date

**Use Cases**:
- 30-day post-surgical complications
- Time to readmission
- Mortality outcomes
- Medication adherence post-discharge

## Data Flow

### Complete Pipeline Execution

```r
# 1. Initialize
conn <- create_db2_connection()
config <- read_db2_config("r-scripts/pipeline_code/db2_config_multi_source.yaml")

# 2. Define cohort
patient_ids <- c(1001:5000)
cohort <- data.frame(
  patient_id = patient_ids,
  index_date = as.Date("2023-01-01")  # Or patient-specific dates
)

# 3. Create long format demographics
sex_long <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "sex",
  patient_ids = patient_ids
)

# 4. Check for conflicts
conflicts <- check_conflicts(sex_long, c("sex_code"))
print(paste("Found", conflicts$n_conflicts, "conflicts"))

# 5. Resolve to one row per patient
sex_master <- get_highest_priority_per_patient(sex_long)

# 6. Create clinical events long format
hospital_long <- create_long_format_asset(
  conn = conn,
  config = config,
  asset_name = "hospital_admissions",
  patient_ids = patient_ids
)

# 7. Filter for condition of interest
diabetes_codes <- data.frame(
  code = c("E10", "E11"),
  name = c("Type 1 DM", "Type 2 DM"),
  description = c("T1DM", "T2DM"),
  terminology = c("ICD10", "ICD10")
)

diabetes_events <- filter_long_by_lookup(
  long_data = hospital_long,
  lookup_table = diabetes_codes,
  code_col_in_data = "icd10_code"
)

# 8. Generate temporal covariates
time_windows <- list(
  list(start = 30, end = 1, label = "recent"),
  list(start = 90, end = 31, label = "medium"),
  list(start = 365, end = 91, label = "distant")
)

diabetes_covariates <- generate_multiple_time_windows(
  long_data = diabetes_events,
  index_dates = cohort,
  event_date_col = "admission_date",
  time_windows = time_windows
)

# 9. Combine demographics and covariates
final_dataset <- cohort %>%
  left_join(sex_master, by = "patient_id") %>%
  left_join(diabetes_covariates, by = c("patient_id", "index_date"))

# 10. Export
write.csv(final_dataset, "/mnt/user-data/outputs/cohort_with_covariates.csv")

# 11. Cleanup
DBI::dbDisconnect(conn)
```

## Architecture Components

### Database Layer

**Technology**: IBM DB2 v11.5.8

**Connection Management**:
- ODBC driver (IBM DB2 ODBC CLI v11.5.8)
- Connection pooling for performance
- Automatic reconnection handling
- Secure credential management via environment variables

**Query Optimization**:
- Parameterized queries to prevent SQL injection
- Efficient use of indexes (patient_id, date columns)
- Batched retrieval for large datasets
- Selective column retrieval (only needed fields)

### Processing Layer

**Technology**: R 4.x with tidyverse

**Key Packages**:
- `DBI` + `odbc` - Database connectivity
- `dplyr` - Data manipulation
- `tidyr` - Data reshaping (long ↔ wide)
- `yaml` - Configuration parsing
- `glue` - SQL query construction

**Memory Management**:
- Process data in patient batches for large cohorts
- Release database results after copying to R
- Use data.table for very large datasets
- Chunk processing for covariate generation

### Configuration Layer

**Technology**: YAML

**Structure**:
```yaml
database:
  # Connection parameters

assets:
  asset_name:
    description: "..."
    sources:
      - name: "source1"
        priority: 1
        table_name: "SCHEMA.TABLE"
        columns:
          r_variable: "DB_COLUMN"

project_preferences:
  project_name:
    asset_name:
      preferred_sources: ["source1", "source2"]
```

**Benefits**:
- Human-readable configuration
- Version controlled
- No code changes needed for new sources
- Project-specific customization

## Design Decisions

### Why Long Format?

**Decision**: Use long format as the primary internal representation

**Rationale**:
1. **Full Audit Trail**: All source values preserved
2. **Conflict Detection**: Easy to identify discrepancies
3. **Source Comparison**: Side-by-side analysis possible
4. **Flexibility**: Can derive wide or priority-selected formats
5. **Data Lineage**: Complete provenance tracking

**Trade-off**: Larger file sizes, requires processing to get analysis-ready format

### Why Priority-Based Resolution?

**Decision**: Use configurable priority rankings rather than algorithmic fusion

**Rationale**:
1. **Transparency**: Clear rules for selection
2. **Domain Knowledge**: Incorporate expert knowledge of source quality
3. **Reproducibility**: Same inputs → same outputs
4. **Simplicity**: Easy to understand and audit

**Alternative Considered**: Statistical fusion (majority vote, Bayesian updating)
**Rejected Because**: Requires assumptions about error rates, harder to audit

### Why YAML Configuration?

**Decision**: Use YAML files rather than R code or database tables

**Rationale**:
1. **Version Control**: Git-trackable configuration changes
2. **No Coding Required**: Domain experts can modify
3. **Human Readable**: Clear structure and relationships
4. **Portable**: Same config works across environments

**Alternative Considered**: Database-driven configuration
**Rejected Because**: Harder to version control, requires DB access to change

### Why Separate Covariate/Outcome Functions?

**Decision**: Distinct functions for covariates (before index) and outcomes (after index)

**Rationale**:
1. **Conceptual Clarity**: Covariates vs outcomes are different
2. **Parameter Differences**: Different defaults and validations
3. **Error Prevention**: Harder to accidentally use wrong time direction

**Implementation**: Minimal code duplication (shared helper functions)

## Configuration System

### Asset Definition

Each asset has:
- **Name**: Unique identifier (e.g., "sex", "date_of_birth")
- **Description**: Human-readable explanation
- **Sources**: List of tables providing this data
- **Columns**: Mapping of R variables to database columns

### Source Specification

Each source has:
- **Priority**: Integer (1 = highest priority)
- **Quality Rating**: Categorical (high/medium/low)
- **Table Name**: Fully qualified table name
- **Primary Key**: Patient identifier column
- **Column Mappings**: R variable names → database column names
- **Metadata**: Coverage estimates, update frequency

### Project Preferences

Projects can override default priorities:
```yaml
project_preferences:
  diabetes_study:
    date_of_birth:
      preferred_sources: ["diabetes_registry", "gp_registry"]
```

## Output Formats

### 1. Long Format
**Use**: Audit trail, conflict detection, source comparison

**Structure**:
- One row per source per patient
- All sources included
- Full source metadata

**File**: `{asset}_long_format.csv`

### 2. Wide Format
**Use**: Manual review, side-by-side comparison

**Structure**:
- One row per patient
- Separate columns for each source
- Easy visual conflict detection

**File**: `{asset}_wide_format.csv`

**Example**:
```
patient_id | gp_registry_sex | hospital_sex | priority_sex
-----------+-----------------+--------------+-------------
1001       | M               | 1            | M
```

### 3. Priority-Selected Format
**Use**: Analysis-ready dataset

**Structure**:
- One row per patient
- Single selected value per variable
- Source provenance included

**File**: `{asset}_priority.csv` or `{asset}_master.csv`

### 4. Conflict Reports
**Use**: Data quality assessment

**Structure**:
- Summary statistics (% conflicts)
- Patient-level conflict details
- Source disagreement patterns

**File**: `{asset}_conflicts.csv`

## Performance Considerations

### Database Query Optimization

1. **Selective Retrieval**: Only query needed columns
2. **Patient Filtering**: Use WHERE clauses to limit rows
3. **Index Usage**: Ensure patient_id and date columns indexed
4. **Batch Processing**: Process 10,000-50,000 patients per batch

### Memory Management

1. **Release Connections**: Always disconnect when done
2. **Chunk Processing**: For millions of patients, process in chunks
3. **Efficient Data Types**: Use appropriate R data types
4. **Garbage Collection**: Explicit `gc()` for long-running jobs

### Parallel Processing

**Future Enhancement**: Process multiple assets in parallel

**Current**: Sequential processing to avoid database connection limits

### Caching

**Future Enhancement**: Cache intermediate results (long format tables)

**Current**: Re-query on each run for data freshness

## Best Practices

### 1. Always Test with Small Cohorts First
```r
# Test with 100 patients before running on millions
patient_ids <- c(1001:1100)
```

### 2. Check Summaries Before Exporting
```r
summary <- summarize_long_format_table(long_data)
print(summary)
```

### 3. Monitor for Conflicts
```r
conflicts <- check_conflicts(long_data, columns_to_check)
if (conflicts$n_conflicts > 0) {
  # Investigate or document
}
```

### 4. Document Source Priorities
Keep a separate document explaining why priorities are set as they are

### 5. Use Timestamped Outputs
```r
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
filename <- paste0("cohort_", timestamp, ".csv")
```

### 6. Validate Outputs
```r
# Check expected row counts
stopifnot(nrow(master) == length(unique(patient_ids)))

# Check for missing critical variables
stopifnot(sum(is.na(master$sex_code)) < 0.05 * nrow(master))
```

## Error Handling

### Connection Failures
- Retry logic with exponential backoff
- Clear error messages with troubleshooting steps
- Graceful degradation (skip unavailable sources)

### Data Quality Issues
- Warning messages for high conflict rates
- Validation checks on expected ranges
- Option to export quality reports

### Missing Data
- Configurable strategies (omit, impute, flag)
- Transparent reporting of missingness
- Option to include patients with partial data

## Security & Privacy

### Database Credentials
- Stored in environment variables (`.env` file)
- Never committed to version control
- Docker secrets for production

### Data Access
- Patient IDs never exposed in logs
- Aggregated statistics only in error messages
- Output files have restricted permissions

### Audit Trail
- Log all data retrievals with timestamps
- Track which sources contributed to final dataset
- Version control of configuration changes

## Future Enhancements

See [FUTURE_DEVELOPMENTS.md](FUTURE_DEVELOPMENTS.md) for detailed roadmap.

**High Priority**:
- Web-based configuration interface
- Real-time conflict resolution dashboard
- Automated data quality monitoring
- Performance optimization for very large cohorts

---

**Related Documentation**:
- [Getting Started Guide](GETTING_STARTED.md)
- [Covariate Generation](docs/COVARIATE_GENERATION.md)
- [Long Format Guide](docs/LONG_FORMAT_GUIDE.md)
- [Test Suite](TEST_SUMMARY.md)
