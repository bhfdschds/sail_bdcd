# Long Format Code Examples

This folder contains practical examples demonstrating how to use the curated long format pipeline for healthcare demographic data curation.

## Overview

The long format system aggregates demographic data from multiple database sources into standardized, analysis-ready datasets. These examples show you how to use the system effectively, from basic operations to complete production pipelines.

## Prerequisites

Before running these examples, ensure you have:

1. **Required R packages installed:**
   ```r
   install.packages(c("dplyr", "tidyr", "DBI", "odbc", "yaml"))
   ```

2. **Database access configured:**
   - DB2 connection set up (DSN configured)
   - Environment variables set:
     - `DB2_USER` - Your database username
     - `DB2_PASSWORD` - Your database password

3. **Required scripts sourced:**
   ```r
   source("utility_code/db2_connection.R")
   source("pipeline_code/read_db2_config_multi_source.R")
   source("pipeline_code/create_long_format_assets.R")
   ```

4. **Configuration file:**
   - `pipeline_code/db2_config_multi_source.yaml` should be properly configured

## Examples

### Example 1: Basic Usage (`01_basic_usage.R`)

**Difficulty:** Beginner
**Run time:** ~2-5 minutes (depending on cohort size)

**What it covers:**
- Loading configuration files
- Connecting to the database
- Creating a long format table for a single asset
- Understanding the output structure
- Converting long format to resolved (one row per patient)
- Saving results

**When to use:**
- You're new to the long format system
- You need data for a single asset (e.g., just ethnicity)
- You want to understand the basic workflow

**How to run:**
```r
# From the r-scripts directory
source("examples/01_basic_usage.R")
```

**Output:**
- `ethnicity_long_format.csv` - Long format table with all sources
- `ethnicity_resolved.csv` - Resolved to one row per patient
- `ethnicity_long_format.rds` - R binary format (long)
- `ethnicity_resolved.rds` - R binary format (resolved)

---

### Example 2: Conflict Detection and Quality Assessment (`02_conflict_detection_and_quality.R`)

**Difficulty:** Intermediate
**Run time:** ~5-10 minutes

**What it covers:**
- Generating comprehensive summary statistics
- Detecting conflicts between sources
- Creating side-by-side source comparisons
- Assessing data quality by source
- Analyzing temporal patterns (when sources were updated)
- Filtering by quality level
- Exporting conflict reports for manual review

**When to use:**
- You need to assess data quality before analysis
- You want to understand conflicts between sources
- You're preparing a data quality report
- You need to identify problematic patients

**How to run:**
```r
# From the r-scripts directory
source("examples/02_conflict_detection_and_quality.R")
```

**Output:**
- `conflict_reports/` directory containing:
  - `ethnicity_conflicts.csv` - Patients with conflicting values
  - `ethnicity_source_comparison.csv` - Side-by-side comparison
  - `quality_analysis.csv` - Quality metrics by source
  - `missing_values_analysis.csv` - Missing data analysis

---

### Example 3: Complete Pipeline (`03_complete_pipeline.R`)

**Difficulty:** Advanced
**Run time:** ~10-20 minutes (all assets, all patients)

**What it covers:**
- Running the complete end-to-end pipeline
- Processing all demographic assets at once
- Creating a master demographics table
- Analyzing completeness across variables
- Identifying high-quality patients
- Exporting production-ready datasets
- Creating data dictionaries and summary reports

**When to use:**
- You need a complete demographic dataset
- You're setting up a production curation workflow
- You need all demographic variables (DOB, sex, ethnicity, LSOA)
- You want comprehensive quality reports

**How to run:**
```r
# From the r-scripts directory
source("examples/03_complete_pipeline.R")
```

**Output:**
- `/mnt/user-data/curated_demographics/run_YYYYMMDD_HHMMSS/` directory containing:
  - `demographics_master.csv` - All patients, all variables
  - `demographics_complete.csv` - Complete data only
  - `demographics_high_quality.csv` - High quality only
  - `completeness_summary.csv` - Completeness metrics
  - `conflict_summary.csv` - Conflict counts across assets
  - `[asset]_conflicts.csv` - Detailed conflict reports per asset
  - `data_dictionary.csv` - Column descriptions
  - `pipeline_report.txt` - Human-readable summary
  - `.rds` files for fast R loading

---

## Quick Start

**If you're new to the system, start here:**

1. **Read the main documentation:**
   - See `r-scripts/LONG_FORMAT_GUIDE.md` for comprehensive documentation

2. **Run Example 1 (Basic Usage):**
   ```r
   source("examples/01_basic_usage.R")
   ```
   This will help you understand the core concepts.

3. **Modify Example 1 for your needs:**
   - Change the asset (try `date_of_birth`, `sex`, or `lsoa`)
   - Filter to your patient cohort
   - Customize output locations

4. **Explore Example 2 when you need quality assessment:**
   ```r
   source("examples/02_conflict_detection_and_quality.R")
   ```

5. **Use Example 3 for production workflows:**
   ```r
   source("examples/03_complete_pipeline.R")
   ```

---

## Common Modifications

### Filter to Specific Patients

In any example, replace:
```r
patient_ids = NULL  # All patients
```

With:
```r
patient_ids = c(1001:5000)  # Specific range
# or
patient_ids = readRDS("my_cohort_ids.rds")  # Load from file
```

### Process Different Assets

Change the asset name:
```r
# Instead of "ethnicity"
create_long_format_asset(conn, config, asset_name = "date_of_birth")
# or
create_long_format_asset(conn, config, asset_name = "lsoa")
```

Available assets:
- `date_of_birth` - Patient birth dates
- `sex` - Biological sex
- `ethnicity` - Ethnicity codes and categories
- `lsoa` - Lower Layer Super Output Area (geography)

### Change Output Locations

Modify output directories:
```r
# Change this
output_dir = "/mnt/user-data/curated_demographics"

# To your preferred location
output_dir = "/path/to/your/output"
```

### Export as RDS Instead of CSV

For faster loading and smaller file sizes:
```r
export_format = "rds"  # Instead of "csv"
```

Load RDS files with:
```r
data <- readRDS("file_name.rds")
```

---

## Understanding the Output

### Long Format Structure

Long format tables have multiple rows per patient (one per source):

```
patient_id | source_table  | source_priority | ethnicity_code
------------------------------------------------------------
P001       | self_reported | 1               | "A"
P001       | admission     | 2               | "A"
P001       | gp            | 4               | "B"
P002       | self_reported | 1               | "C"
```

**Columns:**
- `patient_id` - Patient identifier
- `source_table` - Which source this row came from
- `source_priority` - Priority ranking (1 = highest)
- `source_quality` - Quality rating (high/medium/low)
- `source_coverage` - Expected coverage (0.0 to 1.0)
- `source_last_updated` - Date source was updated
- [Asset-specific columns] - The actual data

### Resolved Format Structure

Resolved tables have one row per patient (using priority):

```
patient_id | source_table  | source_priority | ethnicity_code
------------------------------------------------------------
P001       | self_reported | 1               | "A"
P002       | self_reported | 1               | "C"
```

Only the highest priority source is kept for each patient.

---

## Troubleshooting

### "Cannot connect to database"

**Solution:** Check your environment variables:
```r
Sys.getenv("DB2_USER")
Sys.getenv("DB2_PASSWORD")
```

Set them if missing:
```r
Sys.setenv(DB2_USER = "your_username")
Sys.setenv(DB2_PASSWORD = "your_password")
```

### "Column not found" errors

**Solution:** Check the YAML configuration matches actual database column names. DB2 returns uppercase columns, so the mapping must be exact.

### "Out of memory" errors

**Solution:** Process in smaller batches:
```r
# Instead of all patients
patient_ids = NULL

# Use batches
batch1 <- c(1:10000)
batch2 <- c(10001:20000)
# ... process separately and combine results
```

### "File not found" when sourcing

**Solution:** Make sure you're running from the `r-scripts` directory:
```r
# Check current directory
getwd()

# If not in r-scripts, set it
setwd("/home/user/sail_bdcd/r-scripts")
```

---

## Performance Tips

1. **Filter to specific patients when possible:**
   - Processing all patients can be slow
   - Use `patient_ids` parameter to limit scope

2. **Use RDS format for large datasets:**
   - Much faster to read/write than CSV
   - Preserves R data types
   - Smaller file sizes

3. **Process assets in parallel (if needed):**
   - Can create separate connections for different assets
   - Use R's parallel processing packages

4. **Cache results:**
   - Save intermediate results as RDS files
   - Reload instead of re-querying database

---

## Next Steps After Examples

Once you're comfortable with the examples:

1. **Integrate into your workflow:**
   - Create custom scripts based on the examples
   - Set up scheduled curation jobs
   - Build dashboards or reports

2. **Customize the configuration:**
   - Add new sources to `db2_config_multi_source.yaml`
   - Adjust priorities based on your assessment
   - Define new assets if needed

3. **Implement quality checks:**
   - Add validation rules for curated data
   - Set up automated conflict alerts
   - Track data quality over time

4. **Join with clinical data:**
   - Use `patient_id` to link demographics with other datasets
   - Create comprehensive patient cohorts
   - Support epidemiological studies

---

## Additional Resources

- **Main Documentation:** `r-scripts/LONG_FORMAT_GUIDE.md`
- **Function Reference:** `r-scripts/pipeline_code/create_long_format_assets.R`
- **Configuration Guide:** See YAML comments in `db2_config_multi_source.yaml`
- **Test Suite:** `r-scripts/tests/testthat/test-create-long-format-assets.R`

---

## Getting Help

If you encounter issues:

1. Check the main documentation (`LONG_FORMAT_GUIDE.md`)
2. Review the test files for reference implementations
3. Verify your configuration file is correct
4. Check database connectivity and permissions
5. Review error messages carefully (they often indicate the specific issue)

---

## Example Workflow

Here's a typical workflow using these examples:

```r
# 1. Start with basic usage to understand the system
source("examples/01_basic_usage.R")

# 2. Review conflicts and quality for your asset of interest
source("examples/02_conflict_detection_and_quality.R")

# 3. Based on quality assessment, adjust configuration if needed
# (Edit db2_config_multi_source.yaml - adjust priorities, add/remove sources)

# 4. Run complete pipeline for production curation
source("examples/03_complete_pipeline.R")

# 5. Use the curated demographics_master.csv in your analysis
demographics <- read.csv("curated_demographics/run_YYYYMMDD_HHMMSS/demographics_master.csv")

# 6. Join with your clinical data
my_cohort <- my_clinical_data %>%
  left_join(demographics, by = "patient_id")
```

---

**Happy curating!**

For questions or issues, consult the main documentation or review the detailed comments within each example script.
