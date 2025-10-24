# Long Format Asset Tables - Worked Examples

This directory contains comprehensive worked examples for creating and analyzing long format asset tables using the `sex` asset as the primary example.

## Overview

Long format tables contain data from **ALL** source tables, with each row representing one source's data for a patient. This format enables:

- Comparison of values across multiple data sources
- Conflict detection and resolution
- Data quality assessment
- Source priority-based selection
- Full audit trail of all available data

## Quick Start

**New to this? Start here:**

1. Read [example_00_master_guide.R](example_00_master_guide.R) for a complete overview
2. Run the `quick_start_workflow()` function to see the entire process

```r
source("examples_code/example_00_master_guide.R")
results <- quick_start_workflow()
```

## Example Files

### [example_00_master_guide.R](example_00_master_guide.R) ⭐ START HERE
**Complete overview and quick start guide**

- Quick 5-step workflow
- Common use cases with ready-to-use functions
- Troubleshooting guide
- Best practices
- Data dictionary

### [example_01_create_long_format_asset.R](example_01_create_long_format_asset.R)
**Core function: `create_long_format_asset()`**

Examples:
1. Basic usage - create sex asset table from all sources
2. Filter by specific patient IDs
3. Include only specific sources
4. Control patient ID column standardization
5. Understanding the long format structure

### [example_02_summarize_long_format_table.R](example_02_summarize_long_format_table.R)
**Function: `summarize_long_format_table()`**

Examples:
1. Basic summary statistics
2. Interpreting source summary
3. Understanding coverage statistics
4. Comparing multiple assets
5. Data quality assessment

### [example_03_check_conflicts.R](example_03_check_conflicts.R)
**Function: `check_conflicts()`**

Examples:
1. Basic conflict detection
2. Understanding conflict details
3. Analyzing conflict patterns
4. Investigating specific conflicts
5. Comparing conflicts across columns
6. Exporting conflict reports

### [example_04_get_highest_priority.R](example_04_get_highest_priority.R)
**Function: `get_highest_priority_per_patient()`**

Examples:
1. Basic priority selection (one row per patient)
2. Comparing before and after
3. Creating master patient tables
4. Handling priority ties
5. Tracking data provenance
6. Quality checks after selection

### [example_05_pivot_to_wide.R](example_05_pivot_to_wide.R)
**Function: `pivot_to_wide_by_source()`**

Examples:
1. Basic wide format conversion
2. Understanding wide format structure
3. Identifying conflicts in wide format
4. Pivoting multiple columns
5. Creating comparison reports
6. Combining wide format with priority selection
7. Wide format for specific sources

### [example_06_export_functions.R](example_06_export_functions.R)
**Functions: `export_asset_table()`, `export_all_asset_tables()`**

Examples:
1. Export to CSV
2. Export to RDS format
3. Export in both formats with size comparison
4. Export all assets at once
5. Export different versions of same asset
6. Custom output directories
7. Reading exported files back
8. Export with metadata

### [example_07_create_all_asset_tables.R](example_07_create_all_asset_tables.R)
**Function: `create_all_asset_tables()`**

Examples:
1. Create all asset tables at once
2. Create specific assets only
3. Process all tables after creation
4. Check conflicts across all assets
5. Create master demographics table
6. Export all asset tables
7. Analyze coverage across all assets
8. Comprehensive quality report

### [example_08_complete_pipeline.R](example_08_complete_pipeline.R)
**Function: `create_asset_pipeline()`**

Examples:
1. Basic pipeline with defaults
2. Pipeline for all patients
3. Pipeline for specific assets
4. Exploring pipeline results
5. Post-pipeline analysis
6. Timestamped output directories
7. Pipeline error handling
8. Complete workflow with all post-processing

## Common Use Cases

### Use Case 1: Get sex data for specific patients
```r
source("examples_code/example_00_master_guide.R")
sex_data <- get_sex_for_patients(c(1001, 1002, 1003))
```

### Use Case 2: Find all conflicts
```r
source("examples_code/example_00_master_guide.R")
conflicts <- find_sex_conflicts()
```

### Use Case 3: Create master demographics
```r
source("examples_code/example_00_master_guide.R")
demographics <- create_master_demographics(patient_ids = c(1001:2000))
```

### Use Case 4: Export cohort data
```r
source("examples_code/example_00_master_guide.R")
cohort_data <- export_cohort_data(patient_ids = c(1001:1500))
```

### Use Case 5: Complete pipeline
```r
source("examples_code/example_08_complete_pipeline.R")
results <- example_complete_workflow()
```

## File Naming Convention

- `example_00_*` - Master guide and overview
- `example_01_*` - Individual function examples
- `example_08_*` - Complete pipeline examples

Numbers indicate recommended reading order.

## What Gets Created

Running the examples will create files in `/mnt/user-data/outputs/`:

**Long format files:**
- `{asset}_long_format.csv` - All sources, all rows

**Priority-selected files:**
- `{asset}_priority.csv` - One row per patient (highest priority)
- `{asset}_master.csv` - Master table for each asset

**Comparison files:**
- `{asset}_wide_format.csv` - Side-by-side source comparison
- `{asset}_wide_comparison.csv` - Wide format with multiple value columns

**Conflict files:**
- `{asset}_conflicts.csv` - Summary of conflicts
- `{asset}_conflicts_details.csv` - Full details of conflicting records
- `{asset}_conflicts_summary.csv` - Statistical summary

**Combined files:**
- `master_demographics.csv` - All assets joined together
- `comprehensive_demographics.csv` - Full demographics with source tracking

**Reports:**
- `SUMMARY_REPORT.txt` - Text summary of pipeline results

## Key Concepts

### Long Format
Each row = one source's data for one patient

| patient_id | source_table | sex_code | sex_description |
|------------|--------------|----------|-----------------|
| 1001       | gp_registry  | M        | Male            |
| 1001       | hospital_demographics | 1 | Male       |
| 1002       | gp_registry  | F        | Female          |

### Wide Format
Each row = one patient, each source gets columns

| patient_id | gp_registry_sex_code | hospital_demographics_sex_code |
|------------|---------------------|--------------------------------|
| 1001       | M                   | 1                              |
| 1002       | F                   | 2                              |

### Priority Selection
One row per patient, using highest priority source

| patient_id | sex_code | source_table |
|------------|----------|--------------|
| 1001       | M        | gp_registry  |
| 1002       | F        | gp_registry  |

## Troubleshooting

**Problem: Database connection fails**
```r
source("examples_code/example_00_master_guide.R")
troubleshooting_guide()
```

**Problem: Getting errors in examples**
- Check that file paths are correct (use absolute paths)
- Ensure database configuration is set up
- Try with small patient_ids subset first
- Check that output directory exists

**Problem: No data returned**
- Verify patient IDs exist in database
- Check table names in configuration
- Set `options(debug_queries = TRUE)` to see SQL

## Best Practices

```r
source("examples_code/example_00_master_guide.R")
best_practices_guide()
```

Key recommendations:
1. Always test with small patient subset first
2. Check summaries before exporting
3. Always check for conflicts
4. Keep long format for audit trail
5. Use wide format for manual review
6. Document source priorities
7. Use timestamped exports
8. Always disconnect from database

## Learning Path

**Beginner:**
1. Start with [example_00_master_guide.R](example_00_master_guide.R)
2. Run `quick_start_workflow()`
3. Try [example_01_create_long_format_asset.R](example_01_create_long_format_asset.R)

**Intermediate:**
4. Explore [example_02_summarize_long_format_table.R](example_02_summarize_long_format_table.R)
5. Learn conflict detection in [example_03_check_conflicts.R](example_03_check_conflicts.R)
6. Master priority selection in [example_04_get_highest_priority.R](example_04_get_highest_priority.R)

**Advanced:**
7. Use [example_07_create_all_asset_tables.R](example_07_create_all_asset_tables.R) for multiple assets
8. Run complete pipeline with [example_08_complete_pipeline.R](example_08_complete_pipeline.R)
9. Customize for your specific needs

## Additional Resources

- Main script: [pipeline_code/create_long_format_assets.R](../pipeline_code/create_long_format_assets.R)
- Configuration: [db2_config_multi_source.yaml](../db2_config_multi_source.yaml)
- Database connection: [utility_code/db2_connection.R](../utility_code/db2_connection.R)

## Getting Help

1. Read the relevant example file
2. Check the troubleshooting guide in [example_00_master_guide.R](example_00_master_guide.R)
3. Review function documentation in the main script
4. Check your configuration file

## Contributing

When adding new examples:
1. Follow the naming convention: `example_XX_description.R`
2. Include detailed comments
3. Show multiple use cases
4. Add practical examples
5. Update this README

---

**Questions?** Start with the master guide: `source("examples_code/example_00_master_guide.R")`
