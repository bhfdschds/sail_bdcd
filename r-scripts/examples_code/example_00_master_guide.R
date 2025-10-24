# MASTER GUIDE: Long Format Asset Tables
# Complete worked example using the 'sex' asset
#
# This guide demonstrates the complete workflow from creating long format tables
# to analyzing conflicts and exporting results.

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)
library(tidyr)

# Source required files
source("/workspaces/sail_bdcd/r-scripts/utility_code/db2_connection.R")
source("/workspaces/sail_bdcd/r-scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# OVERVIEW OF AVAILABLE EXAMPLES
# ============================================================================

# The examples_code folder contains detailed examples for each function:
#
# example_01_create_long_format_asset.R
#   - Creating long format tables from multiple sources
#   - Filtering by patient IDs
#   - Selecting specific sources
#   - Understanding the long format structure
#
# example_02_summarize_long_format_table.R
#   - Generating summary statistics
#   - Analyzing source coverage
#   - Comparing multiple assets
#   - Data quality assessment
#
# example_03_check_conflicts.R
#   - Detecting conflicts across sources
#   - Analyzing conflict patterns
#   - Investigating specific conflicts
#   - Exporting conflict reports
#
# example_04_get_highest_priority.R
#   - Selecting highest priority value per patient
#   - Creating master patient tables
#   - Handling priority ties
#   - Tracking data provenance
#
# example_05_pivot_to_wide.R
#   - Converting long format to wide format
#   - Side-by-side source comparison
#   - Identifying conflicts in wide format
#   - Combining wide format with priority selection
#
# example_06_export_functions.R
#   - Exporting to CSV and RDS formats
#   - Exporting multiple assets
#   - Creating versioned outputs
#   - Reading exported files back
#
# example_07_create_all_asset_tables.R
#   - Creating all assets at once
#   - Processing multiple assets
#   - Creating master demographics tables
#   - Quality reports for all assets
#
# example_08_complete_pipeline.R
#   - Running the complete end-to-end pipeline
#   - Post-pipeline analysis
#   - Comprehensive workflow examples
#   - Error handling

# ============================================================================
# QUICK START: Complete Workflow in 5 Steps
# ============================================================================

quick_start_workflow <- function() {
  cat("\n")
  cat("==========================================================\n")
  cat("QUICK START: Complete Workflow for Sex Asset\n")
  cat("==========================================================\n\n")

  # STEP 1: Setup and Connect
  cat("STEP 1: Setup and Connect to Database\n")
  cat("-" * 60, "\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  cat("✓ Connected to database\n\n")

  # STEP 2: Create Long Format Table
  cat("STEP 2: Create Long Format Sex Table\n")
  cat("-" * 60, "\n")

  sex_long <- create_long_format_asset(
    conn = conn,
    config = config,
    asset_name = "sex",
    patient_ids = c(1001:1500)  # Subset for quick testing
  )

  cat(glue("✓ Created long format table: {nrow(sex_long)} rows\n\n"))

  # STEP 3: Analyze the Data
  cat("STEP 3: Analyze the Data\n")
  cat("-" * 60, "\n")

  # Generate summary
  cat("\n3a. Summary Statistics:\n")
  summary_results <- summarize_long_format_table(sex_long, "sex")

  # Check for conflicts
  cat("\n3b. Conflict Detection:\n")
  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  if (is.null(conflicts)) {
    cat("✓ No conflicts found\n\n")
  } else {
    cat(glue("⚠ Found {nrow(conflicts)} patients with conflicts\n\n"))
  }

  # STEP 4: Create Final Tables
  cat("STEP 4: Create Final Tables\n")
  cat("-" * 60, "\n\n")

  # Priority-selected version (one row per patient)
  cat("4a. Priority-selected table (one row per patient):\n")
  sex_priority <- get_highest_priority_per_patient(sex_long)
  cat(glue("   {nrow(sex_priority)} patients\n\n"))

  # Wide format for comparison
  cat("4b. Wide format comparison table:\n")
  sex_wide <- pivot_to_wide_by_source(sex_long, c("sex_code", "sex_description"))
  cat(glue("   {ncol(sex_wide)} columns\n\n"))

  # STEP 5: Export Results
  cat("STEP 5: Export Results\n")
  cat("-" * 60, "\n\n")

  # Export long format
  file1 <- export_asset_table(sex_long, "sex_long_format", format = "csv")
  cat(glue("✓ {basename(file1)}\n"))

  # Export priority version
  file2 <- export_asset_table(sex_priority, "sex_priority", format = "csv")
  cat(glue("✓ {basename(file2)}\n"))

  # Export wide format
  write.csv(sex_wide, "/mnt/user-data/outputs/sex_wide_format.csv", row.names = FALSE)
  cat("✓ sex_wide_format.csv\n")

  # Export conflicts if any
  if (!is.null(conflicts)) {
    write.csv(conflicts, "/mnt/user-data/outputs/sex_conflicts.csv", row.names = FALSE)
    cat("✓ sex_conflicts.csv\n")
  }

  # Cleanup
  DBI::dbDisconnect(conn)

  cat("\n")
  cat("==========================================================\n")
  cat("QUICK START COMPLETE!\n")
  cat("==========================================================\n\n")

  cat("Files created in /mnt/user-data/outputs/:\n")
  cat("  - sex_long_format.csv: All sources, all rows\n")
  cat("  - sex_priority.csv: One row per patient (highest priority)\n")
  cat("  - sex_wide_format.csv: Side-by-side source comparison\n")
  if (!is.null(conflicts)) {
    cat("  - sex_conflicts.csv: Patients with conflicting values\n")
  }

  return(list(
    long_format = sex_long,
    priority = sex_priority,
    wide = sex_wide,
    conflicts = conflicts,
    summary = summary_results
  ))
}

# ============================================================================
# COMMON USE CASES
# ============================================================================

# USE CASE 1: Get sex data for specific patients
get_sex_for_patients <- function(patient_ids) {
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn, config, "sex", patient_ids = patient_ids
  )

  sex_final <- get_highest_priority_per_patient(sex_long)

  DBI::dbDisconnect(conn)

  return(sex_final)
}

# USE CASE 2: Compare sources side-by-side for a patient
compare_sources_for_patient <- function(patient_id) {
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(
    conn, config, "sex", patient_ids = patient_id
  )

  DBI::dbDisconnect(conn)

  cat(glue("\n=== Sex Data for Patient {patient_id} ===\n\n"))
  print(sex_long)

  return(sex_long)
}

# USE CASE 3: Find all patients with conflicting sex data
find_sex_conflicts <- function() {
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  sex_long <- create_long_format_asset(conn, config, "sex")
  conflicts <- check_conflicts(sex_long, "sex", "sex_code")

  DBI::dbDisconnect(conn)

  if (is.null(conflicts)) {
    cat("No conflicts found!\n")
    return(NULL)
  } else {
    cat(glue("Found {nrow(conflicts)} patients with conflicts\n"))
    return(conflicts)
  }
}

# USE CASE 4: Create master demographics table
create_master_demographics <- function(patient_ids = NULL) {
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all asset tables
  asset_tables <- create_all_asset_tables(
    conn, config, patient_ids,
    assets = c("sex", "date_of_birth", "ethnicity", "lsoa")
  )

  # Get highest priority for each
  sex <- get_highest_priority_per_patient(asset_tables$sex)
  dob <- get_highest_priority_per_patient(asset_tables$date_of_birth)
  ethnicity <- get_highest_priority_per_patient(asset_tables$ethnicity)
  lsoa <- get_highest_priority_per_patient(asset_tables$lsoa)

  # Join all together
  demographics <- sex %>%
    select(patient_id, sex_code, sex_description) %>%
    full_join(
      dob %>% select(patient_id, date_of_birth),
      by = "patient_id"
    ) %>%
    full_join(
      ethnicity %>% select(patient_id, ethnicity_code, ethnicity_category),
      by = "patient_id"
    ) %>%
    full_join(
      lsoa %>% select(patient_id, lsoa_code),
      by = "patient_id"
    )

  DBI::dbDisconnect(conn)

  return(demographics)
}

# USE CASE 5: Export all assets for a cohort
export_cohort_data <- function(patient_ids, output_dir = "/mnt/user-data/outputs") {
  results <- create_asset_pipeline(
    config_path = "/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml",
    patient_ids = patient_ids,
    assets = c("sex", "date_of_birth", "ethnicity", "lsoa"),
    output_dir = output_dir
  )

  cat("\n✓ Cohort data exported to:\n")
  for (asset in names(results$exported_files)) {
    cat(glue("  - {basename(results$exported_files[[asset]])}\n"))
  }

  return(results)
}

# ============================================================================
# TROUBLESHOOTING TIPS
# ============================================================================

troubleshooting_guide <- function() {
  cat("\n")
  cat("==========================================================\n")
  cat("TROUBLESHOOTING GUIDE\n")
  cat("==========================================================\n\n")

  cat("PROBLEM: Database connection fails\n")
  cat("SOLUTION:\n")
  cat("  1. Check config file path is correct\n")
  cat("  2. Verify database credentials in config\n")
  cat("  3. Ensure DB2 is accessible from your environment\n")
  cat("  4. Try: source('utility_code/db2_connection.R')\n\n")

  cat("PROBLEM: 'Asset not found in configuration'\n")
  cat("SOLUTION:\n")
  cat("  1. Check asset name spelling (e.g., 'sex' not 'Sex')\n")
  cat("  2. Verify asset exists in your YAML config\n")
  cat("  3. Check config file is loaded correctly\n\n")

  cat("PROBLEM: No data retrieved from sources\n")
  cat("SOLUTION:\n")
  cat("  1. Check patient_ids exist in database\n")
  cat("  2. Verify table names in config are correct\n")
  cat("  3. Check schema name in config\n")
  cat("  4. Set options(debug_queries = TRUE) to see SQL\n\n")

  cat("PROBLEM: Column name mismatches\n")
  cat("SOLUTION:\n")
  cat("  1. Check column mappings in YAML config\n")
  cat("  2. Verify db_column names match actual database\n")
  cat("  3. DB2 may return uppercase - code handles this\n\n")

  cat("PROBLEM: Export fails\n")
  cat("SOLUTION:\n")
  cat("  1. Check output directory exists\n")
  cat("  2. Verify write permissions\n")
  cat("  3. Check disk space\n")
  cat("  4. Try: dir.create('/mnt/user-data/outputs', recursive = TRUE)\n\n")

  cat("PROBLEM: Too many conflicts\n")
  cat("SOLUTION:\n")
  cat("  1. This is expected - that's why we have priorities\n")
  cat("  2. Use get_highest_priority_per_patient() to resolve\n")
  cat("  3. Review source priorities in config\n")
  cat("  4. Export wide format to manually review\n\n")
}

# ============================================================================
# BEST PRACTICES
# ============================================================================

best_practices_guide <- function() {
  cat("\n")
  cat("==========================================================\n")
  cat("BEST PRACTICES\n")
  cat("==========================================================\n\n")

  cat("1. ALWAYS start with a small patient_ids subset for testing\n")
  cat("   Example: patient_ids = c(1001:1100)\n\n")

  cat("2. Check summaries BEFORE exporting\n")
  cat("   Use summarize_long_format_table() to understand your data\n\n")

  cat("3. Check for conflicts in key fields\n")
  cat("   Use check_conflicts() to identify data quality issues\n\n")

  cat("4. Keep long format for full audit trail\n")
  cat("   Export both long format and priority-selected versions\n\n")

  cat("5. Use wide format for manual review\n")
  cat("   pivot_to_wide_by_source() makes conflicts easy to spot\n\n")

  cat("6. Document your source priorities\n")
  cat("   Review and validate priority rankings in your config\n\n")

  cat("7. Export with timestamps\n")
  cat("   Keep different analysis runs separate\n\n")

  cat("8. Use the complete pipeline for production\n")
  cat("   create_asset_pipeline() ensures consistent workflow\n\n")

  cat("9. Always disconnect from database\n")
  cat("   Use DBI::dbDisconnect(conn) when done\n\n")

  cat("10. Version control your config files\n")
  cat("    Track changes to source priorities and mappings\n\n")
}

# ============================================================================
# EXAMPLE DATA DICTIONARY
# ============================================================================

data_dictionary <- function() {
  cat("\n")
  cat("==========================================================\n")
  cat("DATA DICTIONARY: Long Format Tables\n")
  cat("==========================================================\n\n")

  cat("STANDARD COLUMNS (present in all long format tables):\n\n")

  cat("patient_id\n")
  cat("  Type: Character/Numeric\n")
  cat("  Description: Unique patient identifier\n")
  cat("  Always first column\n\n")

  cat("source_table\n")
  cat("  Type: Character\n")
  cat("  Description: Name of the source (e.g., 'gp_registry')\n")
  cat("  Used for grouping and filtering\n\n")

  cat("source_db_table\n")
  cat("  Type: Character\n")
  cat("  Description: Actual database table name\n")
  cat("  For reference and debugging\n\n")

  cat("source_priority\n")
  cat("  Type: Numeric\n")
  cat("  Description: Priority ranking (lower = higher priority)\n")
  cat("  Used by get_highest_priority_per_patient()\n\n")

  cat("OPTIONAL METADATA COLUMNS (if defined in config):\n\n")

  cat("source_quality\n")
  cat("  Type: Character\n")
  cat("  Description: Data quality rating for this source\n\n")

  cat("source_coverage\n")
  cat("  Type: Character/Numeric\n")
  cat("  Description: Coverage percentage for this source\n\n")

  cat("source_last_updated\n")
  cat("  Type: Date/Character\n")
  cat("  Description: When this source was last updated\n\n")

  cat("ASSET-SPECIFIC COLUMNS:\n\n")

  cat("For 'sex' asset:\n")
  cat("  - sex_code: The sex code (e.g., '1', '2', 'M', 'F')\n")
  cat("  - sex_description: Human-readable description\n\n")

  cat("For 'date_of_birth' asset:\n")
  cat("  - date_of_birth: The birth date\n\n")

  cat("For 'ethnicity' asset:\n")
  cat("  - ethnicity_code: The ethnicity code\n")
  cat("  - ethnicity_category: Broader category\n\n")

  cat("For 'lsoa' asset:\n")
  cat("  - lsoa_code: Lower Super Output Area code\n\n")
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# To get started, uncomment and run:

# 1. Quick start workflow
# results <- quick_start_workflow()

# 2. Common use cases
# sex_data <- get_sex_for_patients(c(1001, 1002, 1003))
# patient_comparison <- compare_sources_for_patient(1001)
# conflicts <- find_sex_conflicts()
# demographics <- create_master_demographics(c(1001:2000))
# cohort <- export_cohort_data(c(1001:1500))

# 3. Help and documentation
# troubleshooting_guide()
# best_practices_guide()
# data_dictionary()

# ============================================================================
# NEXT STEPS
# ============================================================================

cat("\n")
cat("==========================================================\n")
cat("NEXT STEPS\n")
cat("==========================================================\n\n")

cat("Now that you understand the basics, explore the detailed examples:\n\n")

cat("For specific functions, see:\n")
cat("  - example_01_create_long_format_asset.R\n")
cat("  - example_02_summarize_long_format_table.R\n")
cat("  - example_03_check_conflicts.R\n")
cat("  - example_04_get_highest_priority.R\n")
cat("  - example_05_pivot_to_wide.R\n")
cat("  - example_06_export_functions.R\n")
cat("  - example_07_create_all_asset_tables.R\n")
cat("  - example_08_complete_pipeline.R\n\n")

cat("Each file contains multiple worked examples showing different\n")
cat("use cases and techniques.\n\n")

cat("Happy analyzing!\n")
