# Example 07: Creating All Asset Tables at Once
# This example demonstrates: create_all_asset_tables()
# Using all demographic assets (sex, date_of_birth, ethnicity, lsoa)

# Load required libraries
library(yaml)
library(DBI)
library(odbc)
library(dplyr)
library(glue)

# Source required files
source("/workspaces/sail_bdcd/r-scripts/utility_code/db2_connection.R")
source("/workspaces/sail_bdcd/r-scripts/pipeline_code/create_long_format_assets.R")

# ============================================================================
# EXAMPLE 1: Basic Usage - Create All Assets
# ============================================================================

example_create_all_basic <- function() {
  cat("\n=== EXAMPLE 1: Create All Asset Tables ===\n\n")

  # 1. Load configuration
  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # 2. Create all asset tables
  cat("Creating all asset tables...\n\n")

  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:1500)  # Subset for testing
  )

  # 3. Explore the results
  cat("\n--- Results ---\n\n")

  cat("asset_tables is a list containing:\n")
  for (asset_name in names(asset_tables)) {
    table <- asset_tables[[asset_name]]
    cat(glue("  {asset_name}: {nrow(table)} rows, {ncol(table)} columns\n"))
  }

  # 4. Access individual tables
  cat("\n--- Accessing Individual Tables ---\n\n")

  cat("You can access each table like this:\n")
  cat("  sex_long <- asset_tables$sex\n")
  cat("  dob_long <- asset_tables$date_of_birth\n")
  cat("  ethnicity_long <- asset_tables$ethnicity\n")
  cat("  lsoa_long <- asset_tables$lsoa\n\n")

  # Show example
  cat("Example - Sex table preview:\n")
  print(head(asset_tables$sex, 5))

  DBI::dbDisconnect(conn)

  return(asset_tables)
}

# ============================================================================
# EXAMPLE 2: Create Specific Assets Only
# ============================================================================

example_create_specific_assets <- function() {
  cat("\n=== EXAMPLE 2: Create Specific Assets Only ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create only sex and date_of_birth
  cat("Creating only sex and date_of_birth tables...\n\n")

  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:1200),
    assets = c("sex", "date_of_birth")  # Specify which assets
  )

  cat("\n--- Results ---\n\n")

  cat(glue("Created {length(asset_tables)} asset tables:\n"))
  for (asset_name in names(asset_tables)) {
    cat(glue("  - {asset_name}\n"))
  }

  DBI::dbDisconnect(conn)

  return(asset_tables)
}

# ============================================================================
# EXAMPLE 3: Process All Tables After Creation
# ============================================================================

example_process_all_tables <- function() {
  cat("\n=== EXAMPLE 3: Process All Tables After Creation ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all tables
  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:2000)
  )

  cat("\n--- Processing Each Table ---\n\n")

  # Generate summaries for all tables
  summaries <- list()

  for (asset_name in names(asset_tables)) {
    cat(glue("\n{'='*60}\n"))
    cat(glue("Processing: {asset_name}\n"))
    cat(glue("{'='*60}\n"))

    table <- asset_tables[[asset_name]]

    # Generate summary
    summary <- summarize_long_format_table(table, asset_name)
    summaries[[asset_name]] <- summary
  }

  cat("\n--- Summary of All Assets ---\n\n")

  for (asset_name in names(asset_tables)) {
    table <- asset_tables[[asset_name]]
    summary <- summaries[[asset_name]]

    cat(glue("{asset_name}:\n"))
    cat(glue("  Total rows: {nrow(table)}\n"))
    cat(glue("  Unique patients: {n_distinct(table$patient_id)}\n"))
    cat(glue("  Number of sources: {n_distinct(table$source_table)}\n\n"))
  }

  DBI::dbDisconnect(conn)

  return(list(
    tables = asset_tables,
    summaries = summaries
  ))
}

# ============================================================================
# EXAMPLE 4: Check Conflicts Across All Assets
# ============================================================================

example_check_all_conflicts <- function() {
  cat("\n=== EXAMPLE 4: Check Conflicts Across All Assets ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all tables
  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:2000)
  )

  cat("\n--- Checking for Conflicts in Each Asset ---\n\n")

  # Define key columns for each asset
  key_columns <- list(
    sex = "sex_code",
    date_of_birth = "date_of_birth",
    ethnicity = "ethnicity_code",
    lsoa = "lsoa_code"
  )

  # Check conflicts in each
  all_conflicts <- list()

  for (asset_name in names(asset_tables)) {
    if (asset_name %in% names(key_columns)) {
      cat(glue("\n{'='*60}\n"))
      cat(glue("Checking: {asset_name}\n"))
      cat(glue("{'='*60}\n"))

      conflicts <- check_conflicts(
        asset_tables[[asset_name]],
        asset_name,
        key_columns[[asset_name]]
      )

      all_conflicts[[asset_name]] <- conflicts
    }
  }

  # Summary of conflicts across all assets
  cat("\n\n=== Conflict Summary Across All Assets ===\n\n")

  for (asset_name in names(all_conflicts)) {
    conflicts <- all_conflicts[[asset_name]]

    if (is.null(conflicts)) {
      cat(glue("{asset_name}: ✓ No conflicts\n"))
    } else {
      cat(glue("{asset_name}: ⚠ {nrow(conflicts)} patients with conflicts\n"))
    }
  }

  DBI::dbDisconnect(conn)

  return(all_conflicts)
}

# ============================================================================
# EXAMPLE 5: Create Master Demographics Table
# ============================================================================

example_create_master_demographics <- function() {
  cat("\n=== EXAMPLE 5: Create Master Demographics Table ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  patient_ids <- c(1001:2000)

  # 1. Create all asset tables
  cat("Step 1: Creating all asset tables...\n\n")

  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = patient_ids
  )

  # 2. Get highest priority for each asset
  cat("\nStep 2: Selecting highest priority values for each asset...\n\n")

  master_tables <- list()

  for (asset_name in names(asset_tables)) {
    cat(glue("  Processing {asset_name}...\n"))
    master_tables[[asset_name]] <- get_highest_priority_per_patient(
      asset_tables[[asset_name]]
    )
  }

  # 3. Join all together into master demographics table
  cat("\nStep 3: Creating master demographics table...\n\n")

  # Start with sex
  demographics <- master_tables$sex %>%
    select(
      patient_id,
      sex_code,
      sex_description,
      sex_source = source_table
    )

  # Add date of birth
  if ("date_of_birth" %in% names(master_tables)) {
    demographics <- demographics %>%
      full_join(
        master_tables$date_of_birth %>%
          select(patient_id, date_of_birth, dob_source = source_table),
        by = "patient_id"
      )
  }

  # Add ethnicity
  if ("ethnicity" %in% names(master_tables)) {
    demographics <- demographics %>%
      full_join(
        master_tables$ethnicity %>%
          select(
            patient_id,
            ethnicity_code,
            ethnicity_category,
            ethnicity_source = source_table
          ),
        by = "patient_id"
      )
  }

  # Add LSOA
  if ("lsoa" %in% names(master_tables)) {
    demographics <- demographics %>%
      full_join(
        master_tables$lsoa %>%
          select(patient_id, lsoa_code, lsoa_source = source_table),
        by = "patient_id"
      )
  }

  cat("\n--- Master Demographics Table ---\n\n")
  cat(glue("Rows: {nrow(demographics)}\n"))
  cat(glue("Columns: {ncol(demographics)}\n\n"))

  cat("Columns:\n")
  for (col in names(demographics)) {
    cat(glue("  - {col}\n"))
  }

  cat("\n--- Sample Data ---\n")
  print(head(demographics, 10))

  # Export
  output_file <- "/mnt/user-data/outputs/master_demographics.csv"
  write.csv(demographics, output_file, row.names = FALSE)
  cat(glue("\n✓ Master demographics exported to: {output_file}\n"))

  DBI::dbDisconnect(conn)

  return(demographics)
}

# ============================================================================
# EXAMPLE 6: Export All Asset Tables
# ============================================================================

example_export_all <- function() {
  cat("\n=== EXAMPLE 6: Create and Export All Asset Tables ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all tables
  cat("Creating all asset tables...\n\n")

  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:2000)
  )

  # Export all
  cat("\n--- Exporting All Tables ---\n\n")

  exported_files <- export_all_asset_tables(
    asset_tables = asset_tables,
    output_dir = "/mnt/user-data/outputs",
    format = "csv"
  )

  cat("\n--- Export Summary ---\n\n")

  for (asset_name in names(exported_files)) {
    file <- exported_files[[asset_name]]
    size <- file.info(file)$size / 1024  # KB

    cat(glue("{asset_name}:\n"))
    cat(glue("  File: {basename(file)}\n"))
    cat(glue("  Size: {round(size, 1)} KB\n\n"))
  }

  DBI::dbDisconnect(conn)

  return(exported_files)
}

# ============================================================================
# EXAMPLE 7: Analyze Coverage Across All Assets
# ============================================================================

example_analyze_coverage_all_assets <- function() {
  cat("\n=== EXAMPLE 7: Analyze Coverage Across All Assets ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all tables
  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config,
    patient_ids = c(1001:3000)
  )

  cat("\n--- Coverage Analysis ---\n\n")

  coverage_summary <- data.frame(
    asset = character(),
    total_patients = integer(),
    patients_multiple_sources = integer(),
    pct_multiple_sources = numeric(),
    stringsAsFactors = FALSE
  )

  for (asset_name in names(asset_tables)) {
    table <- asset_tables[[asset_name]]

    total_patients <- n_distinct(table$patient_id)

    multi_source <- table %>%
      group_by(patient_id) %>%
      filter(n() > 1) %>%
      pull(patient_id) %>%
      n_distinct()

    pct_multi <- round(100 * multi_source / total_patients, 1)

    coverage_summary <- rbind(
      coverage_summary,
      data.frame(
        asset = asset_name,
        total_patients = total_patients,
        patients_multiple_sources = multi_source,
        pct_multiple_sources = pct_multi
      )
    )
  }

  print(coverage_summary)

  cat("\n--- Interpretation ---\n")
  cat("Higher percentage of patients with multiple sources indicates:\n")
  cat("  ✓ Better data coverage\n")
  cat("  ✓ More opportunities for validation\n")
  cat("  ⚠ More potential conflicts to resolve\n")

  DBI::dbDisconnect(conn)

  return(coverage_summary)
}

# ============================================================================
# EXAMPLE 8: Quality Report for All Assets
# ============================================================================

example_quality_report_all_assets <- function() {
  cat("\n=== EXAMPLE 8: Comprehensive Quality Report ===\n\n")

  config <- read_db_config("/workspaces/sail_bdcd/r-scripts/db2_config_multi_source.yaml")
  conn <- create_db2_connection(config)

  # Create all tables
  asset_tables <- create_all_asset_tables(
    conn = conn,
    config = config
  )

  cat("\n==========================================================\n")
  cat("QUALITY REPORT: ALL ASSETS\n")
  cat("==========================================================\n\n")

  for (asset_name in names(asset_tables)) {
    cat(glue("\n{'='*60}\n"))
    cat(glue("{toupper(asset_name)}\n"))
    cat(glue("{'='*60}\n"))

    table <- asset_tables[[asset_name]]

    # Basic stats
    cat("\n1. Basic Statistics:\n")
    cat(glue("   Total rows: {nrow(table)}\n"))
    cat(glue("   Unique patients: {n_distinct(table$patient_id)}\n"))
    cat(glue("   Number of sources: {n_distinct(table$source_table)}\n"))

    # Source distribution
    cat("\n2. Source Distribution:\n")
    source_dist <- table %>%
      count(source_table, sort = TRUE)
    print(source_dist)

    # Data completeness (check for NAs in key columns)
    cat("\n3. Data Completeness:\n")
    data_cols <- setdiff(
      names(table),
      c("patient_id", "source_table", "source_db_table",
        "source_priority", "source_quality", "source_coverage",
        "source_last_updated")
    )

    for (col in data_cols) {
      n_missing <- sum(is.na(table[[col]]))
      pct_complete <- round(100 * (1 - n_missing / nrow(table)), 1)
      cat(glue("   {col}: {pct_complete}% complete\n"))
    }

    cat("\n")
  }

  cat("\n==========================================================\n")
  cat("END OF QUALITY REPORT\n")
  cat("==========================================================\n")

  DBI::dbDisconnect(conn)

  return(asset_tables)
}

# ============================================================================
# RUN EXAMPLES
# ============================================================================

# Uncomment to run individual examples:

# Basic usage
# all_assets <- example_create_all_basic()

# Specific assets only
# some_assets <- example_create_specific_assets()

# Process all tables
# processed <- example_process_all_tables()

# Check all conflicts
# conflicts <- example_check_all_conflicts()

# Create master demographics
# demographics <- example_create_master_demographics()

# Export all
# exported <- example_export_all()

# Analyze coverage
# coverage <- example_analyze_coverage_all_assets()

# Quality report
# quality <- example_quality_report_all_assets()
