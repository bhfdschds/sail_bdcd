# Long Format Code Examples - Complete Index

## 📚 Documentation

### [README_LONG_FORMAT_EXAMPLES.md](README_LONG_FORMAT_EXAMPLES.md)
Complete guide to all examples, use cases, and learning paths.

## 🎯 Example Files

### 🌟 [example_00_master_guide.R](example_00_master_guide.R) - **START HERE**
**17 KB | Complete Overview & Quick Start**

**Contents:**
- Overview of all available examples
- 5-step quick start workflow
- 5 common use case functions (ready to use)
- Troubleshooting guide
- Best practices guide
- Complete data dictionary

**Key Functions:**
- `quick_start_workflow()` - Complete workflow in 5 steps
- `get_sex_for_patients()` - Get sex data for specific patients
- `compare_sources_for_patient()` - Compare sources for one patient
- `find_sex_conflicts()` - Find all conflicting records
- `create_master_demographics()` - Create master demographics table
- `export_cohort_data()` - Export all data for a cohort
- `troubleshooting_guide()` - Display troubleshooting tips
- `best_practices_guide()` - Display best practices
- `data_dictionary()` - Display data dictionary

---

### 📖 [example_01_create_long_format_asset.R](example_01_create_long_format_asset.R)
**6.4 KB | Core Function Examples**

**Function:** `create_long_format_asset()`

**5 Examples:**
1. Basic usage - create sex table from all sources
2. Filter by specific patient IDs
3. Include only specific sources
4. Keep original patient ID column name
5. Understanding the long format structure

**Key Concepts:**
- What is long format?
- How rows represent source data
- Column structure and naming
- Metadata columns

---

### 📊 [example_02_summarize_long_format_table.R](example_02_summarize_long_format_table.R)
**11 KB | Summary Statistics**

**Function:** `summarize_long_format_table()`

**5 Examples:**
1. Basic summary of sex asset
2. Interpreting source summary
3. Understanding coverage statistics
4. Comparing summaries across multiple assets
5. Using summary for data quality assessment

**Key Concepts:**
- Source distribution
- Patient coverage
- Multi-source statistics
- Quality metrics

---

### 🔍 [example_03_check_conflicts.R](example_03_check_conflicts.R)
**12 KB | Conflict Detection**

**Function:** `check_conflicts()`

**6 Examples:**
1. Basic conflict detection
2. Understanding conflict details
3. Analyzing conflict patterns
4. Investigating specific conflicts
5. Comparing conflicts across different columns
6. Exporting conflict reports

**Key Concepts:**
- What are conflicts?
- Conflict patterns
- Source disagreements
- Conflict resolution strategies

---

### 🎯 [example_04_get_highest_priority.R](example_04_get_highest_priority.R)
**14 KB | Priority Selection**

**Function:** `get_highest_priority_per_patient()`

**6 Examples:**
1. Basic priority selection (one row per patient)
2. Comparing before and after selection
3. Creating master patient tables
4. Handling priority ties
5. Tracking data provenance
6. Quality checks after selection

**Key Concepts:**
- Priority-based selection
- One row per patient
- Source provenance
- Tie handling

---

### 🔄 [example_05_pivot_to_wide.R](example_05_pivot_to_wide.R)
**14 KB | Wide Format Conversion**

**Function:** `pivot_to_wide_by_source()`

**7 Examples:**
1. Basic wide format conversion
2. Understanding wide format structure
3. Identifying conflicts in wide format
4. Pivoting multiple value columns
5. Creating comparison reports
6. Combining wide format with priority selection
7. Wide format for specific sources

**Key Concepts:**
- Long vs wide format
- Side-by-side comparison
- Column naming in wide format
- Visual conflict detection

---

### 💾 [example_06_export_functions.R](example_06_export_functions.R)
**14 KB | Export & File Operations**

**Functions:** `export_asset_table()`, `export_all_asset_tables()`

**8 Examples:**
1. Export to CSV
2. Export to RDS format
3. Export in both formats (with size comparison)
4. Export all asset tables at once
5. Export different versions of same asset
6. Custom output directories
7. Reading exported files back
8. Export with metadata

**Key Concepts:**
- CSV vs RDS formats
- File size optimization
- Versioned outputs
- Metadata tracking

---

### 🔧 [example_07_create_all_asset_tables.R](example_07_create_all_asset_tables.R)
**15 KB | Multi-Asset Operations**

**Function:** `create_all_asset_tables()`

**8 Examples:**
1. Create all asset tables at once
2. Create specific assets only
3. Process all tables after creation
4. Check conflicts across all assets
5. Create master demographics table
6. Export all asset tables
7. Analyze coverage across all assets
8. Comprehensive quality report for all assets

**Key Concepts:**
- Batch processing
- Multi-asset workflows
- Cross-asset analysis
- Master table creation

---

### ⚙️ [example_08_complete_pipeline.R](example_08_complete_pipeline.R)
**17 KB | Complete Pipeline**

**Function:** `create_asset_pipeline()`

**8 Examples:**
1. Basic pipeline with defaults
2. Pipeline for all patients
3. Pipeline for specific assets
4. Exploring pipeline results
5. Post-pipeline analysis
6. Timestamped output directories
7. Pipeline error handling
8. Complete workflow with all post-processing

**Key Concepts:**
- End-to-end workflow
- Pipeline automation
- Result organization
- Production workflows

---

## 📋 Quick Reference

### By Task

**Just Getting Started?**
→ [example_00_master_guide.R](example_00_master_guide.R) - Run `quick_start_workflow()`

**Need sex data for specific patients?**
→ [example_00_master_guide.R](example_00_master_guide.R) - Run `get_sex_for_patients(c(1001, 1002))`

**Want to find conflicts?**
→ [example_03_check_conflicts.R](example_03_check_conflicts.R) - See example 1

**Need one value per patient?**
→ [example_04_get_highest_priority.R](example_04_get_highest_priority.R) - See example 1

**Want to compare sources side-by-side?**
→ [example_05_pivot_to_wide.R](example_05_pivot_to_wide.R) - See example 1

**Need to export results?**
→ [example_06_export_functions.R](example_06_export_functions.R) - See example 1

**Working with multiple assets?**
→ [example_07_create_all_asset_tables.R](example_07_create_all_asset_tables.R) - See example 5

**Want the complete workflow?**
→ [example_08_complete_pipeline.R](example_08_complete_pipeline.R) - See example 8

### By Function

| Function | Example File | File Size |
|----------|--------------|-----------|
| `create_long_format_asset()` | example_01 | 6.4 KB |
| `summarize_long_format_table()` | example_02 | 11 KB |
| `check_conflicts()` | example_03 | 12 KB |
| `get_highest_priority_per_patient()` | example_04 | 14 KB |
| `pivot_to_wide_by_source()` | example_05 | 14 KB |
| `export_asset_table()` | example_06 | 14 KB |
| `export_all_asset_tables()` | example_06 | 14 KB |
| `create_all_asset_tables()` | example_07 | 15 KB |
| `create_asset_pipeline()` | example_08 | 17 KB |

### Learning Path

**🎓 Beginner** (Start here!)
1. [example_00_master_guide.R](example_00_master_guide.R) - Overview
2. [example_01_create_long_format_asset.R](example_01_create_long_format_asset.R) - Basic creation
3. [example_02_summarize_long_format_table.R](example_02_summarize_long_format_table.R) - Understanding data

**📚 Intermediate**
4. [example_03_check_conflicts.R](example_03_check_conflicts.R) - Conflict detection
5. [example_04_get_highest_priority.R](example_04_get_highest_priority.R) - Priority selection
6. [example_06_export_functions.R](example_06_export_functions.R) - Exporting results

**🚀 Advanced**
7. [example_05_pivot_to_wide.R](example_05_pivot_to_wide.R) - Advanced transformations
8. [example_07_create_all_asset_tables.R](example_07_create_all_asset_tables.R) - Multi-asset workflows
9. [example_08_complete_pipeline.R](example_08_complete_pipeline.R) - Production pipelines

## 📊 Statistics

- **Total Examples:** 9 files
- **Total Code Examples:** 50+ individual examples
- **Total Size:** ~120 KB of documented code
- **Functions Covered:** 9 main functions
- **Use Cases:** 5 ready-to-use functions

## 🎯 Common Workflows

### Workflow 1: Quick Analysis
```r
source("examples_code/example_00_master_guide.R")
results <- quick_start_workflow()
```

### Workflow 2: Check for Conflicts
```r
source("examples_code/example_00_master_guide.R")
conflicts <- find_sex_conflicts()
```

### Workflow 3: Create Master Table
```r
source("examples_code/example_00_master_guide.R")
demographics <- create_master_demographics(patient_ids = c(1001:2000))
```

### Workflow 4: Complete Pipeline
```r
source("examples_code/example_08_complete_pipeline.R")
results <- example_complete_workflow()
```

## 📖 Additional Resources

- **Main Script:** `pipeline_code/create_long_format_assets.R`
- **Configuration:** `db2_config_multi_source.yaml`
- **Database Connection:** `utility_code/db2_connection.R`

## 🆘 Getting Help

1. **Start with:** [example_00_master_guide.R](example_00_master_guide.R)
2. **Troubleshooting:** Run `troubleshooting_guide()`
3. **Best Practices:** Run `best_practices_guide()`
4. **Data Dictionary:** Run `data_dictionary()`

---

**Last Updated:** 2025-10-24
**Total Examples:** 50+
**Coverage:** All functions in create_long_format_assets.R
